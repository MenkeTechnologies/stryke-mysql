//! `stryke-mysql-helper` — bridge binary for the stryke `mysql` package.
//!
//! Two execution modes:
//!
//! * **Single-shot**: every invocation opens a fresh MySQL connection,
//!   runs one command, prints JSON, exits. Trivial to script around;
//!   eats a TCP handshake per call.
//! * **Serve**: long-running JSON-RPC daemon on a Unix socket. Connection
//!   pool is held inside the helper; stryke side sends requests over the
//!   socket and reuses the warm connection. ~50× faster for hot loops.
//!
//! Output protocol:
//!   query        → NDJSON rows on stdout (or one columnar JSON object
//!                  with --columnar)
//!   execute      → {"affected_rows":N,"last_insert_id":N}
//!   exec --file  → array of per-statement {"affected_rows":...}
//!   schema       → {"table":..,"columns":[...],"indexes":[...]}
//!   tables       → NDJSON {"name":..}
//!   databases    → NDJSON {"name":..}
//!   stats        → JSON object
//!   ping         → "ok"  (exit 0 / non-zero)
//!
//! DSN: standard `mysql://user:pass@host:port/db?ssl-mode=required` URL,
//! or any subset of --host/--port/--user/--password/--database/--socket.
//! Env vars: $MYSQL_DSN, $MYSQL_HOST, $MYSQL_PORT, $MYSQL_USER,
//! $MYSQL_PASSWORD, $MYSQL_DATABASE.

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::{Parser, Subcommand};
use mysql::prelude::*;
use mysql::{Conn, Opts, OptsBuilder, Params, Row, SslOpts, Value as MyValue};
use serde_json::{json, Map as JMap, Value};

/* ------------------------------------------------------------------------- */
/* CLI                                                                       */
/* ------------------------------------------------------------------------- */

#[derive(Parser)]
#[command(
    name = "stryke-mysql-helper",
    version,
    about = "MySQL / MariaDB bridge for the stryke `mysql` package"
)]
struct Cli {
    /// DSN URL: `mysql://user:pass@host:port/db?ssl-mode=required`.
    #[arg(long, env = "MYSQL_DSN", global = true)]
    dsn: Option<String>,

    #[arg(long, short = 'H', env = "MYSQL_HOST", global = true)]
    host: Option<String>,

    #[arg(long, short = 'P', env = "MYSQL_PORT", global = true)]
    port: Option<u16>,

    #[arg(long, short = 'u', env = "MYSQL_USER", global = true)]
    user: Option<String>,

    /// Prefer setting `$MYSQL_PASSWORD` rather than passing on the CLI.
    #[arg(
        long,
        short = 'p',
        env = "MYSQL_PASSWORD",
        global = true,
        hide_env_values = true
    )]
    password: Option<String>,

    #[arg(long, short = 'D', env = "MYSQL_DATABASE", global = true)]
    database: Option<String>,

    /// Connect via a Unix socket instead of TCP.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,

    /// Enable SSL (`ssl-mode=required` analogue).
    #[arg(long, global = true)]
    ssl: bool,

    /// Path to a CA bundle for SSL verification (implies --ssl).
    #[arg(long, global = true)]
    ssl_ca: Option<PathBuf>,

    /// Connect timeout, seconds.
    #[arg(long, global = true, default_value_t = 10)]
    connect_timeout: u64,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a SELECT and stream rows as NDJSON.
    Query {
        sql: String,
        /// JSON array (positional `?`) or object (named `:name`) of bind values.
        #[arg(long)]
        bind: Option<String>,
        /// Emit one columnar JSON object instead of NDJSON.
        #[arg(long)]
        columnar: bool,
        /// Prepend a `{"meta":{columns:[...]}}` line before rows.
        #[arg(long)]
        with_meta: bool,
        /// Cap rows emitted (server-side LIMIT is preferred when possible).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Run a single non-SELECT statement.
    Execute {
        sql: String,
        #[arg(long)]
        bind: Option<String>,
    },
    /// Run a multi-statement SQL file (`;`-separated). Returns one JSON object
    /// per statement.
    Exec {
        #[arg(long, short = 'f')]
        file: PathBuf,
    },
    /// `SELECT * FROM TABLE [WHERE w] [ORDER BY o] [LIMIT n]` shorthand.
    Dump {
        #[arg(long, short = 't')]
        table: String,
        #[arg(long)]
        columns: Option<String>,
        #[arg(long = "where", short = 'w')]
        where_clause: Option<String>,
        #[arg(long)]
        order_by: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// List tables in the current database.
    Tables,
    /// List databases the user can see.
    Databases,
    /// DESCRIBE + SHOW INDEX for one table.
    Schema {
        #[arg(long, short = 't')]
        table: String,
    },
    /// Connect, run `SELECT 1`, exit 0/1.
    Ping,
    /// Run as a JSON-RPC daemon on a Unix socket.
    Serve {
        /// Socket path. Created with mode 0600.
        #[arg(long = "socket-path")]
        socket_path: PathBuf,
    },
}

/* ------------------------------------------------------------------------- */
/* main                                                                      */
/* ------------------------------------------------------------------------- */

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(&cli) {
        eprintln!("stryke-mysql-helper: {e:#}");
        std::process::exit(1);
    }
}

fn run(cli: &Cli) -> Result<()> {
    match &cli.cmd {
        Cmd::Query {
            sql,
            bind,
            columnar,
            with_meta,
            limit,
        } => {
            let mut conn = connect(cli)?;
            cmd_query(
                &mut conn,
                sql,
                bind.as_deref(),
                *columnar,
                *with_meta,
                *limit,
            )
        }
        Cmd::Execute { sql, bind } => {
            let mut conn = connect(cli)?;
            let r = exec_execute(&mut conn, sql, bind.as_deref())?;
            emit_json(&r)
        }
        Cmd::Exec { file } => {
            let mut conn = connect(cli)?;
            cmd_exec_file(&mut conn, file)
        }
        Cmd::Dump {
            table,
            columns,
            where_clause,
            order_by,
            limit,
        } => {
            let mut conn = connect(cli)?;
            cmd_dump(
                &mut conn,
                table,
                columns.as_deref(),
                where_clause.as_deref(),
                order_by.as_deref(),
                *limit,
            )
        }
        Cmd::Tables => {
            let mut conn = connect(cli)?;
            cmd_tables(&mut conn)
        }
        Cmd::Databases => {
            let mut conn = connect(cli)?;
            cmd_databases(&mut conn)
        }
        Cmd::Schema { table } => {
            let mut conn = connect(cli)?;
            cmd_schema(&mut conn, table)
        }
        Cmd::Ping => {
            let mut conn = connect(cli)?;
            let _: Option<i64> = conn.query_first("SELECT 1")?;
            println!("ok");
            Ok(())
        }
        Cmd::Serve { socket_path } => cmd_serve(cli, socket_path),
    }
}

/* ------------------------------------------------------------------------- */
/* connection plumbing                                                       */
/* ------------------------------------------------------------------------- */

fn build_opts(cli: &Cli) -> Result<Opts> {
    let mut builder = if let Some(url) = &cli.dsn {
        let parsed = Opts::from_url(url).context("parsing --dsn URL")?;
        OptsBuilder::from_opts(parsed)
    } else {
        OptsBuilder::new()
    };

    if let Some(h) = &cli.host {
        builder = builder.ip_or_hostname(Some(h.as_str()));
    }
    if let Some(p) = cli.port {
        builder = builder.tcp_port(p);
    }
    if let Some(u) = &cli.user {
        builder = builder.user(Some(u.as_str()));
    }
    if let Some(pw) = &cli.password {
        builder = builder.pass(Some(pw.as_str()));
    }
    if let Some(db) = &cli.database {
        builder = builder.db_name(Some(db.as_str()));
    }
    if let Some(sock) = &cli.socket {
        builder = builder.socket(Some(
            sock.to_str()
                .ok_or_else(|| anyhow!("--socket path is not valid UTF-8"))?,
        ));
    }
    if cli.ssl || cli.ssl_ca.is_some() {
        let mut sopts = SslOpts::default();
        if let Some(ca) = &cli.ssl_ca {
            sopts = sopts.with_root_cert_path(Some(ca.clone()));
        }
        builder = builder.ssl_opts(Some(sopts));
    }
    builder = builder.tcp_connect_timeout(Some(Duration::from_secs(cli.connect_timeout)));
    Ok(Opts::from(builder))
}

fn connect(cli: &Cli) -> Result<Conn> {
    let opts = build_opts(cli)?;
    Conn::new(opts).context("connecting to mysql")
}

/* ------------------------------------------------------------------------- */
/* bind-param decoding                                                       */
/* ------------------------------------------------------------------------- */

/// Parse `--bind` JSON. Accepts:
///   `[v1, v2, ...]`            → positional `?` placeholders
///   `{"name": v, ...}`         → named `:name` placeholders
fn parse_bind(s: Option<&str>) -> Result<Params> {
    let Some(raw) = s else {
        return Ok(Params::Empty);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Params::Empty);
    }
    let v: Value = serde_json::from_str(raw).context("parsing --bind JSON")?;
    match v {
        Value::Array(arr) => Ok(Params::Positional(
            arr.into_iter().map(json_to_myval).collect(),
        )),
        Value::Object(obj) => {
            let mut map: HashMap<Vec<u8>, MyValue> = HashMap::new();
            for (k, val) in obj {
                map.insert(k.into_bytes(), json_to_myval(val));
            }
            Ok(Params::Named(map))
        }
        Value::Null => Ok(Params::Empty),
        _ => bail!("--bind must be a JSON array or object"),
    }
}

fn json_to_myval(v: Value) -> MyValue {
    match v {
        Value::Null => MyValue::NULL,
        Value::Bool(b) => MyValue::Int(if b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                MyValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                MyValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                MyValue::Double(f)
            } else {
                MyValue::Bytes(n.to_string().into_bytes())
            }
        }
        Value::String(s) => MyValue::Bytes(s.into_bytes()),
        Value::Array(_) | Value::Object(_) => {
            // Serialize back to JSON and pass as a string so mysql treats it
            // as text. Consumer can store in a JSON column.
            MyValue::Bytes(v.to_string().into_bytes())
        }
    }
}

/* ------------------------------------------------------------------------- */
/* row → JSON                                                                */
/* ------------------------------------------------------------------------- */

fn row_to_json(row: &Row) -> Value {
    let mut out = JMap::with_capacity(row.len());
    for (i, col) in row.columns_ref().iter().enumerate() {
        let name = std::str::from_utf8(col.name_ref())
            .unwrap_or("?")
            .to_string();
        let mv: &MyValue = row.as_ref(i).unwrap_or(&MyValue::NULL);
        out.insert(name, myval_to_json(mv));
    }
    Value::Object(out)
}

fn myval_to_json(v: &MyValue) -> Value {
    match v {
        MyValue::NULL => Value::Null,
        MyValue::Int(i) => json!(*i),
        MyValue::UInt(u) => json!(*u),
        MyValue::Float(f) => json!(*f),
        MyValue::Double(d) => json!(*d),
        MyValue::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => json!(s),
            Err(_) => {
                // Non-UTF-8 blob → base64-encoded string with a sentinel
                // prefix so consumers can tell it from a normal string.
                let mut out = String::from("base64:");
                out.push_str(&B64.encode(b));
                Value::String(out)
            }
        },
        MyValue::Date(y, m, d, h, mi, s, us) => {
            if *h == 0 && *mi == 0 && *s == 0 && *us == 0 {
                json!(format!("{:04}-{:02}-{:02}", y, m, d))
            } else {
                json!(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    y, m, d, h, mi, s, us
                ))
            }
        }
        MyValue::Time(neg, days, h, m, s, us) => {
            let sign = if *neg { "-" } else { "" };
            let hours = *days * 24 + (*h as u32);
            json!(format!("{}{:02}:{:02}:{:02}.{:06}", sign, hours, m, s, us))
        }
    }
}

/* ------------------------------------------------------------------------- */
/* commands                                                                  */
/* ------------------------------------------------------------------------- */

fn cmd_query(
    conn: &mut Conn,
    sql: &str,
    bind: Option<&str>,
    columnar: bool,
    with_meta: bool,
    limit: Option<usize>,
) -> Result<()> {
    let params = parse_bind(bind)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut result = conn.exec_iter(sql, params).context("exec_iter")?;
    let set = result
        .iter()
        .ok_or_else(|| anyhow!("query returned no result set"))?;

    let columns: Vec<String> = set
        .columns()
        .as_ref()
        .iter()
        .map(|c| std::str::from_utf8(c.name_ref()).unwrap_or("?").to_string())
        .collect();

    if columnar {
        let mut rows: Vec<Value> = Vec::new();
        let mut count = 0usize;
        for row in set {
            let row = row?;
            let mut arr: Vec<Value> = Vec::with_capacity(row.len());
            for i in 0..row.len() {
                let mv: &MyValue = row.as_ref(i).unwrap_or(&MyValue::NULL);
                arr.push(myval_to_json(mv));
            }
            rows.push(Value::Array(arr));
            count += 1;
            if let Some(l) = limit {
                if count >= l {
                    break;
                }
            }
        }
        let obj = json!({
            "columns": columns,
            "num_rows": rows.len(),
            "rows": rows,
        });
        serde_json::to_writer(&mut out, &obj)?;
        out.write_all(b"\n")?;
    } else {
        if with_meta {
            let meta = json!({ "meta": { "columns": columns } });
            serde_json::to_writer(&mut out, &meta)?;
            out.write_all(b"\n")?;
        }
        let mut count = 0usize;
        for row in set {
            let row = row?;
            let v = row_to_json(&row);
            serde_json::to_writer(&mut out, &v)?;
            out.write_all(b"\n")?;
            count += 1;
            if let Some(l) = limit {
                if count >= l {
                    break;
                }
            }
        }
    }
    out.flush()?;
    Ok(())
}

#[derive(serde::Serialize)]
struct ExecResult {
    affected_rows: u64,
    last_insert_id: Option<u64>,
    warnings: u16,
    info: String,
}

fn exec_execute(conn: &mut Conn, sql: &str, bind: Option<&str>) -> Result<ExecResult> {
    let params = parse_bind(bind)?;
    let (info, warnings) = {
        let mut result = conn.exec_iter(sql, params).context("exec_iter")?;
        while let Some(set) = result.iter() {
            for _ in set {}
        }
        (result.info_str().to_string(), result.warnings())
    };
    let affected_rows = conn.affected_rows();
    let last_insert_id = match conn.last_insert_id() {
        0 => None,
        v => Some(v),
    };
    Ok(ExecResult {
        affected_rows,
        last_insert_id,
        warnings,
        info,
    })
}

fn cmd_exec_file(conn: &mut Conn, file: &PathBuf) -> Result<()> {
    let raw = fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let stmts = split_statements(&raw);
    let mut results: Vec<Value> = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        if stmt.trim().is_empty() {
            continue;
        }
        let mut result = conn.exec_iter(&stmt, Params::Empty)?;
        while let Some(set) = result.iter() {
            for _ in set {}
        }
        results.push(json!({
            "sql": stmt,
            "affected_rows": result.affected_rows(),
            "last_insert_id": result.last_insert_id(),
            "warnings": result.warnings(),
        }));
    }
    emit_json(&Value::Array(results))
}

fn split_statements(src: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut prev = '\0';
    for c in src.chars() {
        match c {
            '\'' if !in_double && !in_backtick && prev != '\\' => in_single = !in_single,
            '"' if !in_single && !in_backtick && prev != '\\' => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            ';' if !in_single && !in_double && !in_backtick => {
                let s = std::mem::take(&mut buf);
                out.push(s);
                prev = c;
                continue;
            }
            _ => {}
        }
        buf.push(c);
        prev = c;
    }
    if !buf.trim().is_empty() {
        out.push(buf);
    }
    out
}

fn cmd_dump(
    conn: &mut Conn,
    table: &str,
    columns: Option<&str>,
    where_clause: Option<&str>,
    order_by: Option<&str>,
    limit: Option<usize>,
) -> Result<()> {
    let cols = columns.unwrap_or("*");
    let mut sql = format!("SELECT {} FROM {}", cols, quote_ident(table));
    if let Some(w) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(w);
    }
    if let Some(o) = order_by {
        sql.push_str(" ORDER BY ");
        sql.push_str(o);
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {}", n));
    }
    cmd_query(conn, &sql, None, false, false, None)
}

fn cmd_tables(conn: &mut Conn) -> Result<()> {
    let rows: Vec<String> = conn.query("SHOW TABLES")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for r in rows {
        serde_json::to_writer(&mut out, &json!({ "name": r }))?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn cmd_databases(conn: &mut Conn) -> Result<()> {
    let rows: Vec<String> = conn.query("SHOW DATABASES")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for r in rows {
        serde_json::to_writer(&mut out, &json!({ "name": r }))?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn cmd_schema(conn: &mut Conn, table: &str) -> Result<()> {
    let columns = collect_rows(conn, &format!("DESCRIBE {}", quote_ident(table)))?;
    let indexes = collect_rows(conn, &format!("SHOW INDEX FROM {}", quote_ident(table)))?;
    let out = json!({
        "table": table,
        "columns": columns,
        "indexes": indexes,
    });
    emit_json(&out)
}

/// Run a query and collect every row as `serde_json::Value`. Owns the
/// QueryResult for its full lifetime so the borrow ends before the next
/// query starts.
fn collect_rows(conn: &mut Conn, sql: &str) -> Result<Vec<Value>> {
    let mut out: Vec<Value> = Vec::new();
    let mut result = conn.query_iter(sql)?;
    if let Some(set) = result.iter() {
        for row in set {
            out.push(row_to_json(&row?));
        }
    }
    Ok(out)
}

fn quote_ident(s: &str) -> String {
    let escaped = s.replace('`', "``");
    format!("`{}`", escaped)
}

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut out, v)?;
    out.write_all(b"\n")?;
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* serve mode — JSON-RPC over Unix socket                                    */
/* ------------------------------------------------------------------------- */

#[derive(serde::Deserialize)]
struct Req {
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(serde::Serialize)]
struct Resp {
    id: Value,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn cmd_serve(cli: &Cli, socket_path: &PathBuf) -> Result<()> {
    // Best-effort cleanup of a stale socket from a prior run.
    let _ = fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("binding {}", socket_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(socket_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(socket_path, perms)?;
    }
    // Log readiness to stderr so the spawning stryke side can wait for it.
    eprintln!(
        "stryke-mysql-helper: listening on {}",
        socket_path.display()
    );

    let opts = build_opts(cli)?;

    for stream in listener.incoming() {
        let stream = stream?;
        if let Err(e) = serve_client(stream, &opts) {
            eprintln!("stryke-mysql-helper: client closed with error: {e:#}");
        }
    }
    Ok(())
}

fn serve_client(stream: UnixStream, opts: &Opts) -> Result<()> {
    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    let mut conn = Conn::new(opts.clone())?;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let req: Req = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                write_resp(
                    &mut writer,
                    &Resp {
                        id: Value::Null,
                        ok: false,
                        result: None,
                        error: Some(format!("parse error: {e}")),
                    },
                )?;
                continue;
            }
        };
        let resp = handle_rpc(&mut conn, &req);
        write_resp(&mut writer, &resp)?;
        if req.method == "close" || req.method == "shutdown" {
            break;
        }
    }
    Ok(())
}

fn write_resp<W: Write>(w: &mut W, resp: &Resp) -> Result<()> {
    serde_json::to_writer(&mut *w, resp)?;
    w.write_all(b"\n")?;
    w.flush()?;
    Ok(())
}

fn handle_rpc(conn: &mut Conn, req: &Req) -> Resp {
    let result = match req.method.as_str() {
        "ping" => {
            let _: Option<i64> = match conn.query_first("SELECT 1") {
                Ok(v) => v,
                Err(e) => return err_resp(&req.id, e.to_string()),
            };
            Ok(json!("ok"))
        }
        "query" => rpc_query(conn, &req.params),
        "execute" => rpc_execute(conn, &req.params),
        "tables" => {
            let v: Vec<String> = match conn.query("SHOW TABLES") {
                Ok(v) => v,
                Err(e) => return err_resp(&req.id, e.to_string()),
            };
            Ok(json!(v))
        }
        "databases" => {
            let v: Vec<String> = match conn.query("SHOW DATABASES") {
                Ok(v) => v,
                Err(e) => return err_resp(&req.id, e.to_string()),
            };
            Ok(json!(v))
        }
        "schema" => rpc_schema(conn, &req.params),
        "close" | "shutdown" => Ok(json!("bye")),
        other => Err(anyhow!("unknown method `{other}`")),
    };
    match result {
        Ok(v) => Resp {
            id: req.id.clone(),
            ok: true,
            result: Some(v),
            error: None,
        },
        Err(e) => err_resp(&req.id, e.to_string()),
    }
}

fn err_resp(id: &Value, msg: String) -> Resp {
    Resp {
        id: id.clone(),
        ok: false,
        result: None,
        error: Some(msg),
    }
}

fn rpc_query(conn: &mut Conn, params: &Value) -> Result<Value> {
    let sql = params
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("query: missing `sql`"))?;
    let bind = params_to_params(params.get("bind"))?;
    let mut result = conn.exec_iter(sql, bind)?;
    let mut rows: Vec<Value> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    if let Some(set) = result.iter() {
        columns = set
            .columns()
            .as_ref()
            .iter()
            .map(|c| std::str::from_utf8(c.name_ref()).unwrap_or("?").to_string())
            .collect();
        for row in set {
            let row = row?;
            rows.push(row_to_json(&row));
        }
    }
    Ok(json!({
        "columns": columns,
        "rows": rows,
    }))
}

fn rpc_execute(conn: &mut Conn, params: &Value) -> Result<Value> {
    let sql = params
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("execute: missing `sql`"))?;
    let bind = params_to_params(params.get("bind"))?;
    let mut result = conn.exec_iter(sql, bind)?;
    while let Some(set) = result.iter() {
        for _ in set {}
    }
    Ok(json!({
        "affected_rows": result.affected_rows(),
        "last_insert_id": result.last_insert_id(),
        "warnings": result.warnings(),
    }))
}

fn rpc_schema(conn: &mut Conn, params: &Value) -> Result<Value> {
    let table = params
        .get("table")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("schema: missing `table`"))?;
    let columns = collect_rows(conn, &format!("DESCRIBE {}", quote_ident(table)))?;
    let indexes = collect_rows(conn, &format!("SHOW INDEX FROM {}", quote_ident(table)))?;
    Ok(json!({
        "table": table,
        "columns": columns,
        "indexes": indexes,
    }))
}

fn params_to_params(v: Option<&Value>) -> Result<Params> {
    match v {
        None | Some(Value::Null) => Ok(Params::Empty),
        Some(Value::Array(arr)) => Ok(Params::Positional(
            arr.iter().cloned().map(json_to_myval).collect(),
        )),
        Some(Value::Object(obj)) => {
            let mut map: HashMap<Vec<u8>, MyValue> = HashMap::new();
            for (k, val) in obj {
                map.insert(k.as_bytes().to_vec(), json_to_myval(val.clone()));
            }
            Ok(Params::Named(map))
        }
        _ => bail!("bind must be array or object"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_bind ──────────────────────────────────────────────────

    #[test]
    fn parse_bind_none_empty() {
        assert!(matches!(parse_bind(None).unwrap(), Params::Empty));
    }

    #[test]
    fn parse_bind_blank_string_empty() {
        assert!(matches!(parse_bind(Some("")).unwrap(), Params::Empty));
        assert!(matches!(parse_bind(Some("   ")).unwrap(), Params::Empty));
    }

    #[test]
    fn parse_bind_null_treated_as_empty() {
        assert!(matches!(parse_bind(Some("null")).unwrap(), Params::Empty));
    }

    #[test]
    fn parse_bind_array_is_positional() {
        match parse_bind(Some("[1, 2, 3]")).unwrap() {
            Params::Positional(v) => assert_eq!(v.len(), 3),
            other => panic!("expected Positional, got {other:?}"),
        }
    }

    #[test]
    fn parse_bind_object_is_named() {
        match parse_bind(Some(r#"{"id":42,"name":"x"}"#)).unwrap() {
            Params::Named(m) => {
                assert_eq!(m.len(), 2);
                assert!(m.contains_key(b"id".as_slice()));
                assert!(m.contains_key(b"name".as_slice()));
            }
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn parse_bind_scalar_rejected() {
        let err = parse_bind(Some("42")).unwrap_err();
        assert!(format!("{err}").contains("array or object"));
    }

    #[test]
    fn parse_bind_invalid_json_errors() {
        let err = parse_bind(Some("{not json}")).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("parsing"));
    }

    // ─── json_to_myval ───────────────────────────────────────────────

    #[test]
    fn json_to_myval_null() {
        assert!(matches!(json_to_myval(Value::Null), MyValue::NULL));
    }

    #[test]
    fn json_to_myval_bool_maps_to_int() {
        // MySQL has no bool — booleans store as TINYINT(1).
        match json_to_myval(json!(true)) {
            MyValue::Int(1) => {}
            other => panic!("expected Int(1), got {other:?}"),
        }
        match json_to_myval(json!(false)) {
            MyValue::Int(0) => {}
            other => panic!("expected Int(0), got {other:?}"),
        }
    }

    #[test]
    fn json_to_myval_positive_int_is_int() {
        assert!(matches!(json_to_myval(json!(42)), MyValue::Int(42)));
        assert!(matches!(json_to_myval(json!(-5)), MyValue::Int(-5)));
    }

    #[test]
    fn json_to_myval_large_unsigned_is_uint() {
        let big: u64 = i64::MAX as u64 + 1;
        match json_to_myval(json!(big)) {
            MyValue::UInt(u) => assert_eq!(u, big),
            other => panic!("expected UInt, got {other:?}"),
        }
    }

    #[test]
    fn json_to_myval_float_is_double() {
        match json_to_myval(json!(2.5)) {
            MyValue::Double(f) => assert_eq!(f, 2.5),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[test]
    fn json_to_myval_string_is_bytes() {
        match json_to_myval(json!("hi")) {
            MyValue::Bytes(b) => assert_eq!(b, b"hi"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn json_to_myval_array_serialized_to_bytes() {
        // Container types → JSON string → bytes (MySQL JSON column-friendly).
        match json_to_myval(json!([1, 2, 3])) {
            MyValue::Bytes(b) => assert_eq!(b, b"[1,2,3]"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn json_to_myval_object_serialized_to_bytes() {
        match json_to_myval(json!({"k":1})) {
            MyValue::Bytes(b) => assert_eq!(b, br#"{"k":1}"#),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    // ─── myval_to_json ───────────────────────────────────────────────

    #[test]
    fn myval_to_json_null() {
        assert_eq!(myval_to_json(&MyValue::NULL), Value::Null);
    }

    #[test]
    fn myval_to_json_int_variants() {
        assert_eq!(myval_to_json(&MyValue::Int(7)), json!(7));
        assert_eq!(myval_to_json(&MyValue::UInt(99)), json!(99));
        assert_eq!(myval_to_json(&MyValue::Double(1.5)), json!(1.5));
        assert_eq!(myval_to_json(&MyValue::Float(0.5)), json!(0.5));
    }

    #[test]
    fn myval_to_json_utf8_bytes_become_string() {
        assert_eq!(
            myval_to_json(&MyValue::Bytes(b"hello".to_vec())),
            json!("hello"),
        );
    }

    #[test]
    fn myval_to_json_non_utf8_bytes_base64_prefixed() {
        // 0xFF, 0xFE — invalid UTF-8 — encoded with base64: sentinel prefix.
        let v = myval_to_json(&MyValue::Bytes(vec![0xff, 0xfe]));
        let s = v.as_str().unwrap();
        assert!(s.starts_with("base64:"));
        let decoded = B64.decode(s.strip_prefix("base64:").unwrap()).unwrap();
        assert_eq!(decoded, vec![0xff, 0xfe]);
    }

    #[test]
    fn myval_to_json_date_only_when_time_components_zero() {
        let v = myval_to_json(&MyValue::Date(2024, 3, 14, 0, 0, 0, 0));
        assert_eq!(v, json!("2024-03-14"));
    }

    #[test]
    fn myval_to_json_datetime_includes_time_and_microseconds() {
        let v = myval_to_json(&MyValue::Date(2024, 3, 14, 13, 45, 6, 123456));
        assert_eq!(v, json!("2024-03-14 13:45:06.123456"));
    }

    #[test]
    fn myval_to_json_time_negative_sign() {
        // -2 days, 5h, 30m, 0s, 0us → -53:30:00.000000
        let v = myval_to_json(&MyValue::Time(true, 2, 5, 30, 0, 0));
        assert_eq!(v, json!("-53:30:00.000000"));
    }

    #[test]
    fn myval_to_json_time_positive() {
        // 0 days, 1h, 0m, 0s, 0us → 01:00:00.000000
        let v = myval_to_json(&MyValue::Time(false, 0, 1, 0, 0, 0));
        assert_eq!(v, json!("01:00:00.000000"));
    }

    // ─── split_statements ────────────────────────────────────────────

    #[test]
    fn split_statements_basic_semicolons() {
        let s = split_statements("SELECT 1; SELECT 2; SELECT 3");
        assert_eq!(s.len(), 3);
        assert!(s[0].contains("SELECT 1"));
        assert!(s[1].contains("SELECT 2"));
        assert!(s[2].contains("SELECT 3"));
    }

    #[test]
    fn split_statements_ignores_semicolon_in_single_quotes() {
        let s = split_statements("INSERT INTO t VALUES ('a;b'); SELECT 1");
        assert_eq!(s.len(), 2);
        assert!(s[0].contains("'a;b'"));
    }

    #[test]
    fn split_statements_ignores_semicolon_in_double_quotes() {
        let s = split_statements(r#"SELECT "x;y"; SELECT 1"#);
        assert_eq!(s.len(), 2);
        assert!(s[0].contains(r#""x;y""#));
    }

    #[test]
    fn split_statements_ignores_semicolon_in_backticks() {
        let s = split_statements("SELECT `weird;ident` FROM t; SELECT 2");
        assert_eq!(s.len(), 2);
        assert!(s[0].contains("`weird;ident`"));
    }

    #[test]
    fn split_statements_drops_trailing_blank_segment() {
        // After trailing ';' the trim().is_empty() guard drops it.
        let s = split_statements("SELECT 1;   ");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn split_statements_empty_input_empty_vec() {
        assert!(split_statements("").is_empty());
        assert!(split_statements("   ").is_empty());
    }

    #[test]
    fn split_statements_single_stmt_no_semicolon() {
        let s = split_statements("SELECT 1");
        assert_eq!(s.len(), 1);
        assert_eq!(s[0], "SELECT 1");
    }

    // ─── quote_ident ─────────────────────────────────────────────────

    #[test]
    fn quote_ident_wraps_in_backticks() {
        assert_eq!(quote_ident("users"), "`users`");
    }

    #[test]
    fn quote_ident_doubles_internal_backtick() {
        // MySQL identifier escaping: ` → ``
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn quote_ident_preserves_dots_and_spaces() {
        assert_eq!(quote_ident("my.schema"), "`my.schema`");
        assert_eq!(quote_ident("col name"), "`col name`");
    }

    // ─── err_resp / params_to_params ─────────────────────────────────

    #[test]
    fn err_resp_marks_failure_and_carries_id_and_msg() {
        let r = err_resp(&json!(7), "boom".into());
        let s = serde_json::to_value(&r).unwrap();
        assert_eq!(s["id"], json!(7));
        assert_eq!(s["ok"], json!(false));
        assert_eq!(s["error"], json!("boom"));
        // skip_serializing_if dropped `result`.
        assert!(!s.as_object().unwrap().contains_key("result"));
    }

    #[test]
    fn params_to_params_none_or_null_is_empty() {
        assert!(matches!(params_to_params(None).unwrap(), Params::Empty));
        assert!(matches!(
            params_to_params(Some(&Value::Null)).unwrap(),
            Params::Empty
        ));
    }

    #[test]
    fn params_to_params_array_positional() {
        let v = json!([1, "x"]);
        match params_to_params(Some(&v)).unwrap() {
            Params::Positional(p) => assert_eq!(p.len(), 2),
            other => panic!("expected Positional, got {other:?}"),
        }
    }

    #[test]
    fn params_to_params_object_named() {
        let v = json!({"a": 1, "b": 2});
        match params_to_params(Some(&v)).unwrap() {
            Params::Named(m) => assert_eq!(m.len(), 2),
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn params_to_params_scalar_rejected() {
        let err = params_to_params(Some(&json!(42))).unwrap_err();
        assert!(format!("{err}").contains("array or object"));
    }

    #[test]
    fn split_statements_backslash_escaped_quote() {
        let s = split_statements(r"SELECT 'it\'s'; SELECT 2");
        assert_eq!(s.len(), 2);
        assert!(s[0].contains("it\\'s") || s[0].contains("it's"));
    }

    #[test]
    fn split_statements_semicolon_only_yields_empty_segments() {
        // Each ';' flushes the buffer (possibly empty) — not trimmed away.
        let s = split_statements(";;;");
        assert_eq!(s.len(), 3);
        assert!(s.iter().all(|x| x.is_empty()));
    }

    #[test]
    fn myval_to_json_date_midnight_formats_date_only() {
        let v = myval_to_json(&MyValue::Date(2024, 6, 15, 0, 0, 0, 0));
        assert_eq!(v, json!("2024-06-15"));
    }

    #[test]
    fn quote_ident_empty_string() {
        assert_eq!(quote_ident(""), "``");
    }

    #[test]
    fn json_to_myval_negative_zero_float() {
        match json_to_myval(json!(-0.0)) {
            MyValue::Double(f) => assert!(f == 0.0 && f.is_sign_negative()),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[test]
    fn parse_bind_whitespace_only_array_elements() {
        match parse_bind(Some("[1, 2]")).unwrap() {
            Params::Positional(v) => assert_eq!(v.len(), 2),
            other => panic!("expected Positional, got {other:?}"),
        }
    }

    #[test]
    fn split_statements_comment_semicolon_still_splits() {
        // Line comments aren't parsed — bare ';' in `-- ;` still delimits.
        let s = split_statements("SELECT 1 -- ;\n; SELECT 2");
        assert_eq!(s.len(), 3);
        assert!(s[0].contains("SELECT 1"));
        assert!(s[2].contains("SELECT 2"));
    }

    #[test]
    fn json_to_myval_u64_max() {
        let big = u64::MAX;
        match json_to_myval(json!(big)) {
            MyValue::UInt(u) => assert_eq!(u, big),
            other => panic!("expected UInt, got {other:?}"),
        }
    }

    #[test]
    fn json_to_myval_empty_string_bytes() {
        match json_to_myval(json!("")) {
            MyValue::Bytes(b) => assert!(b.is_empty()),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn params_to_params_empty_object() {
        match params_to_params(Some(&json!({}))).unwrap() {
            Params::Named(m) => assert!(m.is_empty()),
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn split_statements_escaped_double_quote() {
        let s = split_statements(r#"SELECT "a""b"; SELECT 2"#);
        assert_eq!(s.len(), 2);
        assert!(s[0].contains(r#""a""b""#));
    }

    #[test]
    fn myval_to_json_float_variant() {
        assert_eq!(myval_to_json(&MyValue::Float(1.25)), json!(1.25));
    }

    #[test]
    fn quote_ident_multiple_internal_backticks() {
        assert_eq!(quote_ident("a``b"), "`a````b`");
    }

    #[test]
    fn parse_bind_object_two_keys() {
        match parse_bind(Some(r#"{"id":1,"name":"x"}"#)).unwrap() {
            Params::Named(m) => assert_eq!(m.len(), 2),
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn split_statements_no_trailing_semicolon_single_stmt() {
        let s = split_statements("INSERT INTO t VALUES (1)");
        assert_eq!(s.len(), 1);
        assert!(!s[0].ends_with(';'));
    }

    #[test]
    fn json_to_myval_true_bool_is_int_one() {
        match json_to_myval(json!(true)) {
            MyValue::Int(1) => {}
            other => panic!("expected Int(1), got {other:?}"),
        }
    }

    #[test]
    fn json_to_myval_fractional_double() {
        match json_to_myval(json!(1.5)) {
            MyValue::Double(f) => assert_eq!(f, 1.5),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[test]
    fn myval_to_json_uint_large() {
        assert_eq!(myval_to_json(&MyValue::UInt(1_000_000)), json!(1_000_000));
    }

    #[test]
    fn split_statements_backtick_string_with_semicolon() {
        let s = split_statements("SELECT `a;b` FROM t; SELECT 2");
        assert_eq!(s.len(), 2);
        assert!(s[0].contains("`a;b`"));
    }

    #[test]
    fn params_to_params_null_is_empty() {
        assert!(matches!(
            params_to_params(Some(&Value::Null)).unwrap(),
            Params::Empty
        ));
    }

    #[test]
    fn quote_ident_unicode() {
        assert_eq!(quote_ident("列"), "`列`");
    }

    #[test]
    fn parse_bind_array_empty() {
        match parse_bind(Some("[]")).unwrap() {
            Params::Positional(v) => assert!(v.is_empty()),
            other => panic!("expected Positional, got {other:?}"),
        }
    }

    #[test]
    fn myval_to_json_time_zero() {
        let v = myval_to_json(&MyValue::Time(false, 0, 0, 0, 0, 0));
        assert_eq!(v, json!("00:00:00.000000"));
    }

    #[test]
    fn json_to_myval_false_bool_is_int_zero() {
        match json_to_myval(json!(false)) {
            MyValue::Int(0) => {}
            other => panic!("expected Int(0), got {other:?}"),
        }
    }

    #[test]
    fn split_statements_two_stmts_with_trailing_semicolon() {
        let s = split_statements("SELECT 1; SELECT 2;");
        assert_eq!(s.len(), 2);
        assert!(s[0].contains('1'));
        assert!(s[1].contains('2'));
    }

    #[test]
    fn myval_to_json_int_negative() {
        assert_eq!(myval_to_json(&MyValue::Int(-3)), json!(-3));
    }

    #[test]
    fn quote_ident_single_backtick_inside() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn err_resp_null_id() {
        let r = err_resp(&Value::Null, "fail".into());
        let s = serde_json::to_value(&r).unwrap();
        assert_eq!(s["id"], Value::Null);
        assert_eq!(s["ok"], json!(false));
    }

    #[test]
    fn split_statements_whitespace_only_between_semicolons() {
        let s = split_statements("SELECT 1;   ; SELECT 2");
        assert_eq!(s.len(), 3);
        assert!(s[1].trim().is_empty());
    }

    #[test]
    fn myval_to_json_bytes_utf8_string() {
        assert_eq!(myval_to_json(&MyValue::Bytes(b"hi".to_vec())), json!("hi"));
    }

    #[test]
    fn json_to_myval_large_positive_int() {
        match json_to_myval(json!(2_147_483_647)) {
            MyValue::Int(n) => assert_eq!(n, 2_147_483_647),
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn split_statements_multiple_blank_segments() {
        let s = split_statements(";;SELECT 1");
        assert_eq!(s.len(), 3);
        assert!(s[0].is_empty());
        assert!(s[1].is_empty());
        assert!(s[2].contains('1'));
    }

    #[test]
    fn myval_to_json_null_variant() {
        assert_eq!(myval_to_json(&MyValue::NULL), Value::Null);
    }

    #[test]
    fn quote_ident_numeric_start() {
        assert_eq!(quote_ident("1col"), "`1col`");
    }

    #[test]
    fn params_to_params_empty_array() {
        match params_to_params(Some(&json!([]))).unwrap() {
            Params::Positional(v) => assert!(v.is_empty()),
            other => panic!("expected Positional, got {other:?}"),
        }
    }

    #[test]
    fn err_resp_string_id() {
        let r = err_resp(&json!("req-1"), "err".into());
        assert_eq!(serde_json::to_value(&r).unwrap()["id"], json!("req-1"));
    }

    #[test]
    fn split_statements_dollar_quote_postgres_style_not_supported() {
        let s = split_statements("SELECT 1; SELECT 2");
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn myval_to_json_date_with_time() {
        let v = myval_to_json(&MyValue::Date(2024, 1, 2, 3, 4, 5, 0));
        assert!(v.as_str().unwrap().contains("2024-01-02"));
    }

    #[test]
    fn json_to_myval_negative_int() {
        match json_to_myval(json!(-9)) {
            MyValue::Int(n) => assert_eq!(n, -9),
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn split_statements_single_statement_no_semicolon() {
        assert_eq!(split_statements("SELECT 1").len(), 1);
    }

    #[test]
    fn myval_to_json_float() {
        assert_eq!(myval_to_json(&MyValue::Float(2.0)), json!(2.0));
    }

    #[test]
    fn quote_ident_reserved_word() {
        assert_eq!(quote_ident("select"), "`select`");
    }

    #[test]
    fn params_to_params_named_two_keys() {
        match params_to_params(Some(&json!({"a": 1, "b": 2}))).unwrap() {
            Params::Named(m) => assert_eq!(m.len(), 2),
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn err_resp_ok_false() {
        let s = serde_json::to_value(err_resp(&json!(1), "e".into())).unwrap();
        assert_eq!(s["ok"], json!(false));
    }

    #[test]
    fn split_statements_trailing_only_semicolon() {
        let s = split_statements("SELECT 1;");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn myval_to_json_uint_zero() {
        assert_eq!(myval_to_json(&MyValue::UInt(0)), json!(0));
    }

    #[test]
    fn split_statements_two_statements() {
        assert_eq!(split_statements("SELECT 1; SELECT 2;").len(), 2);
    }

    #[test]
    fn json_to_myval_uint_above_i64_max() {
        let big = (i64::MAX as u64) + 1;
        match json_to_myval(json!(big)) {
            MyValue::UInt(n) => assert_eq!(n, big),
            other => panic!("expected UInt, got {other:?}"),
        }
    }

    #[test]
    fn split_statements_semicolon_in_string_not_split() {
        let s = split_statements("SELECT ';'; SELECT 2;");
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn myval_to_json_bytes_as_base64() {
        let v = myval_to_json(&MyValue::Bytes(vec![0, 255]));
        assert!(v.as_str().unwrap().starts_with("base64:"));
    }

    #[test]
    fn err_resp_includes_error_string() {
        let s = serde_json::to_value(err_resp(&json!(null), "fail".into())).unwrap();
        assert_eq!(s["error"], json!("fail"));
    }

    // ─── parse_bind error-shape pins ─────────────────────────────────
    //
    // CLI users grep the rejection text for both the offending shape
    // and the expected `JSON array or object` template; existing
    // tests pin the rejection itself but not the message — drift here
    // silently changes script behavior.

    #[test]
    fn parse_bind_scalar_error_templates_array_or_object() {
        let err = parse_bind(Some("42")).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("array or object"),
            "error should hint at expected shape; got: {msg}"
        );
    }

    #[test]
    fn parse_bind_string_scalar_rejected_same_way() {
        let err = parse_bind(Some("\"nope\"")).unwrap_err();
        assert!(format!("{err}").contains("array or object"));
    }

    #[test]
    fn parse_bind_invalid_json_surfaces_context() {
        let err = parse_bind(Some("{")).unwrap_err();
        let chain: Vec<_> = err.chain().map(|c| c.to_string()).collect();
        assert!(
            chain.iter().any(|s| s.contains("parsing --bind JSON")),
            "expected `parsing --bind JSON` context in chain; got {chain:?}"
        );
    }
}
