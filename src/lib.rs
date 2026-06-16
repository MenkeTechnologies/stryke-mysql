//! stryke-mysql — MySQL / MariaDB cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn mysql__*` is a JSON-string-in /
//! JSON-string-out wrapper around the sync `mysql` crate. stryke's FFI
//! bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use MySQL`, registers each one as a stryke-callable function, and on
//! each call passes a JSON-encoded args dict and copies the returned JSON
//! into a stryke string.
//!
//! Persistent state: `POOLS` caches one `mysql::Pool` per connection URL
//! for the life of the stryke process. The v1 helper opened a fresh TCP
//! connection per fork; the pool reuses the same connection objects
//! across calls.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

use anyhow::{anyhow, bail, Result};
use mysql::prelude::*;
use mysql::{Params, Pool, QueryResult, Row, TxOpts, Value as MyValue};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Map, Value};

// ── pool cache ──────────────────────────────────────────────────────────────

static POOLS: OnceCell<Mutex<HashMap<String, Pool>>> = OnceCell::new();

fn pools() -> &'static Mutex<HashMap<String, Pool>> {
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Build a mysql connection URL from either the explicit `url` field or
/// `host`/`port`/`user`/`password`/`database` parts.
fn url_from_opts(opts: &Value) -> String {
    if let Some(u) = opts.get("url").and_then(|v| v.as_str()) {
        return u.to_string();
    }
    if let Ok(u) = std::env::var("MYSQL_URL") {
        return u;
    }
    let host = opts
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("127.0.0.1");
    let port = opts.get("port").and_then(|v| v.as_i64()).unwrap_or(3306);
    let user = opts.get("user").and_then(|v| v.as_str()).unwrap_or("root");
    let password = opts.get("password").and_then(|v| v.as_str()).unwrap_or("");
    let db = opts.get("database").and_then(|v| v.as_str()).unwrap_or("");
    let auth = if password.is_empty() {
        user.to_string()
    } else {
        format!("{}:{}", user, password)
    };
    format!("mysql://{}@{}:{}/{}", auth, host, port, db)
}

fn get_pool(opts: &Value) -> Result<Pool> {
    let url = url_from_opts(opts);
    {
        let map = pools().lock();
        if let Some(p) = map.get(&url) {
            return Ok(p.clone());
        }
    }
    let pool = Pool::new(url.as_str())?;
    pools().lock().insert(url, pool.clone());
    Ok(pool)
}

// ── JSON ↔ mysql conversion ─────────────────────────────────────────────────

fn json_to_my_value(v: &Value) -> MyValue {
    match v {
        Value::Null => MyValue::NULL,
        Value::Bool(b) => MyValue::Int(if *b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                MyValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                MyValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                // Bind as f64 (mysql DOUBLE) — the old `f as f32` cast threw
                // away ~29 mantissa bits from every DECIMAL/monetary/coordinate
                // value.
                MyValue::Double(f)
            } else {
                MyValue::Bytes(n.to_string().into_bytes())
            }
        }
        Value::String(s) => MyValue::Bytes(s.as_bytes().to_vec()),
        _ => MyValue::Bytes(v.to_string().into_bytes()),
    }
}

fn my_value_to_json(v: &MyValue) -> Value {
    match v {
        MyValue::NULL => Value::Null,
        MyValue::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => Value::String(format!("<binary {} bytes>", b.len())),
        },
        MyValue::Int(n) => json!(n),
        MyValue::UInt(n) => json!(n),
        MyValue::Float(n) => json!(n),
        MyValue::Double(n) => json!(n),
        MyValue::Date(y, m, d, h, mi, s, _) => Value::String(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            y, m, d, h, mi, s
        )),
        MyValue::Time(neg, days, hours, minutes, seconds, _) => Value::String(format!(
            "{}{}d {:02}:{:02}:{:02}",
            if *neg { "-" } else { "" },
            days,
            hours,
            minutes,
            seconds
        )),
    }
}

fn row_to_json(row: Row, names: &[String]) -> Value {
    let mut obj = Map::new();
    for (i, name) in names.iter().enumerate() {
        let v = row.as_ref(i).cloned().unwrap_or(MyValue::NULL);
        obj.insert(name.clone(), my_value_to_json(&v));
    }
    Value::Object(obj)
}

fn params_from_value(v: &Value) -> Params {
    match v.as_array() {
        Some(arr) => Params::Positional(arr.iter().map(json_to_my_value).collect()),
        None => Params::Empty,
    }
}

// ── ops ─────────────────────────────────────────────────────────────────────

fn op_ping(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let _: Option<i64> = conn.query_first("SELECT 1")?;
    Ok(json!({"ok": true}))
}

fn op_version(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let v: String = conn.query_first("SELECT VERSION()")?.unwrap_or_default();
    Ok(json!({"version": v}))
}

fn op_databases(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let dbs: Vec<String> = conn.query("SHOW DATABASES")?;
    Ok(json!({"databases": dbs}))
}

fn op_tables(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let tables: Vec<String> = conn.query("SHOW TABLES")?;
    Ok(json!({"tables": tables}))
}

fn op_schema(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let table = validate_identifier(
        opts["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?,
        "table",
    )?;
    let stmt = conn.prep(format!("DESCRIBE {}", table))?;
    let names = vec![
        "Field".to_string(),
        "Type".to_string(),
        "Null".to_string(),
        "Key".to_string(),
        "Default".to_string(),
        "Extra".to_string(),
    ];
    let rows: Vec<Row> = conn.exec(&stmt, ())?;
    let out: Vec<Value> = rows.into_iter().map(|r| row_to_json(r, &names)).collect();
    Ok(json!({"table": table, "columns": out}))
}

fn op_query(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let sql = opts["sql"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sql"))?
        .to_string();
    let params = params_from_value(&opts["params"]);
    let stmt = conn.prep(&sql)?;
    let names: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let rows: Vec<Row> = match params {
        Params::Empty => conn.exec(&stmt, ())?,
        Params::Positional(_) | Params::Named(_) => conn.exec(&stmt, params)?,
    };
    let out: Vec<Value> = rows.into_iter().map(|r| row_to_json(r, &names)).collect();
    Ok(json!({"columns": names, "rows": out}))
}

fn op_execute(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let sql = opts["sql"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sql"))?
        .to_string();
    let params = params_from_value(&opts["params"]);
    match params {
        Params::Empty => conn.query_drop(&sql)?,
        Params::Positional(_) | Params::Named(_) => conn.exec_drop(&sql, params)?,
    }
    Ok(json!({
        "affected": conn.affected_rows() as i64,
        "last_insert_id": conn.last_insert_id() as i64,
    }))
}

/// Drain every result set from a `QueryResult` into `[{columns, rows}]`.
/// MySQL stored procedures and multi-statement queries return more than one
/// set, so both `call` and `query_multi` reuse this.
fn collect_result_sets<P: Protocol>(mut result: QueryResult<'_, '_, '_, P>) -> Result<Vec<Value>> {
    let mut sets = Vec::new();
    while let Some(mut set) = result.iter() {
        let names: Vec<String> = set
            .columns()
            .as_ref()
            .iter()
            .map(|c| c.name_str().to_string())
            .collect();
        let mut rows = Vec::new();
        for r in set.by_ref() {
            rows.push(row_to_json(r?, &names));
        }
        sets.push(json!({"columns": names, "rows": rows}));
    }
    Ok(sets)
}

/// Run an array of `{sql, params?}` statements atomically. On any error the
/// transaction is rolled back (MySQL `Transaction` rolls back on drop);
/// otherwise all are committed together. Returns per-statement
/// `{affected, last_insert_id}`.
fn op_transaction(opts: Value) -> Result<Value> {
    let stmts = opts["statements"]
        .as_array()
        .ok_or_else(|| anyhow!("missing statements (array of sql/params objects)"))?;
    if stmts.is_empty() {
        return Err(anyhow!("statements must be a non-empty array"));
    }
    // Validate every statement has a sql string before opening the transaction.
    for s in stmts {
        if s.get("sql").and_then(Value::as_str).is_none() {
            return Err(anyhow!("each statement needs a `sql` string"));
        }
    }
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let mut tx = conn.start_transaction(TxOpts::default())?;
    let mut results = Vec::new();
    for s in stmts {
        let sql = s["sql"].as_str().unwrap();
        let params = params_from_value(&s["params"]);
        match params {
            Params::Empty => tx.query_drop(sql)?,
            _ => tx.exec_drop(sql, params)?,
        }
        results.push(json!({
            "affected": tx.affected_rows() as i64,
            "last_insert_id": tx.last_insert_id().map(|v| v as i64),
        }));
    }
    tx.commit()?;
    Ok(json!({"ok": true, "statements": results}))
}

/// Call a stored procedure `proc(args...)`, collecting every result set it
/// emits. `args` is an optional positional array.
fn op_call(opts: Value) -> Result<Value> {
    let proc = validate_identifier(
        opts["proc"]
            .as_str()
            .ok_or_else(|| anyhow!("missing proc"))?,
        "proc",
    )?;
    let args = params_from_value(&opts["args"]);
    let placeholders = match &args {
        Params::Positional(v) => vec!["?"; v.len()].join(", "),
        _ => String::new(),
    };
    let sql = format!("CALL {}({})", proc, placeholders);
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    // query_iter (Text) and exec_iter (Binary) are distinct QueryResult types,
    // so the generic collector runs inside each arm.
    let sets = match args {
        Params::Empty => collect_result_sets(conn.query_iter(sql)?)?,
        _ => collect_result_sets(conn.exec_iter(sql, args)?)?,
    };
    Ok(json!({"result_sets": sets}))
}

/// Run a multi-statement SQL string, returning every result set. Requires the
/// connection to allow multiple statements (MySQL `CLIENT_MULTI_STATEMENTS`,
/// which the `mysql` pool enables by default).
fn op_query_multi(opts: Value) -> Result<Value> {
    let sql = opts["sql"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sql"))?
        .to_string();
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let result = conn.query_iter(sql)?;
    Ok(json!({"result_sets": collect_result_sets(result)?}))
}

fn op_exec(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let sql = opts["sql"].as_str().ok_or_else(|| anyhow!("missing sql"))?;
    // Pre-fix this used `sql.split(';')` which broke SQL with embedded
    // semicolons inside string literals or comments. A statement like
    // `INSERT INTO t (msg) VALUES ('hello; world')` was split into
    // `INSERT INTO t (msg) VALUES ('hello` (parse error) and
    // ` world')` (orphan parens). The splitter below respects single
    // quotes, double quotes, backticks, line comments, and block comments.
    for stmt in split_sql_statements(sql) {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        conn.query_drop(trimmed)?;
    }
    Ok(json!({"ok": true}))
}

/// SQL-aware statement splitter. Respects single-quoted strings (with `''`
/// escape), double-quoted strings (`""`), backtick identifiers, line
/// comments (`-- … \n` and `# … \n`), and block comments (`/* … */`).
fn split_sql_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut out: Vec<String> = Vec::new();
    // Accumulate raw bytes, not `b as char` — the latter reinterprets every
    // UTF-8 continuation byte as a Latin-1 codepoint, corrupting non-ASCII
    // string literals/identifiers/comments. Statement boundaries are all
    // ASCII (`;`, quotes, `/*`), so each emitted segment is whole UTF-8.
    let mut cur: Vec<u8> = Vec::new();
    let flush = |cur: &mut Vec<u8>, out: &mut Vec<String>| {
        out.push(String::from_utf8_lossy(cur).into_owned());
    };
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' | b'"' | b'`' => {
                let quote = b;
                cur.push(b);
                i += 1;
                while i < bytes.len() {
                    let c = bytes[i];
                    cur.push(c);
                    i += 1;
                    if c == quote {
                        // SQL standard: doubled quote = escaped, continue.
                        if i < bytes.len() && bytes[i] == quote {
                            cur.push(quote);
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    // Backslash-escape only for non-backtick quotes; mysql also
                    // recognizes `\\` and `\'` inside strings.
                    if quote != b'`' && c == b'\\' && i < bytes.len() {
                        cur.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                // Line comment to end of line.
                while i < bytes.len() && bytes[i] != b'\n' {
                    cur.push(bytes[i]);
                    i += 1;
                }
            }
            b'#' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    cur.push(bytes[i]);
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                cur.extend_from_slice(b"/*");
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    cur.push(bytes[i]);
                    i += 1;
                }
                if i + 1 < bytes.len() {
                    cur.extend_from_slice(b"*/");
                    i += 2;
                }
            }
            b';' => {
                flush(&mut cur, &mut out);
                cur.clear();
                i += 1;
            }
            _ => {
                cur.push(b);
                i += 1;
            }
        }
    }
    if !cur.is_empty() {
        flush(&mut cur, &mut out);
    }
    out
}

fn op_insert_many(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let table = validate_identifier(
        opts["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?,
        "table",
    )?;
    let rows = opts["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (array of objects)"))?;
    if rows.is_empty() {
        return Ok(json!({"inserted": 0}));
    }
    let first = rows[0]
        .as_object()
        .ok_or_else(|| anyhow!("first row must be an object"))?;
    let cols: Vec<String> = first
        .keys()
        .map(|s| validate_identifier(s, "column"))
        .collect::<Result<_>>()?;
    let col_list = cols.join(", ");
    let placeholders = vec!["?"; cols.len()].join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table, col_list, placeholders
    );
    let stmt = conn.prep(&sql)?;
    let mut total = 0i64;
    for row in rows {
        let obj = row
            .as_object()
            .ok_or_else(|| anyhow!("row must be an object"))?;
        // Pre-fix indexed via `obj[*c]` which returns &Value::Null for missing
        // keys — silently binding NULL when a row was missing a column. Use
        // explicit `get()` so a missing column hard-errors instead, surfacing
        // the row-shape mismatch to the caller.
        let vals: Vec<MyValue> = cols
            .iter()
            .map(|c| {
                obj.get(c).map(json_to_my_value).ok_or_else(|| {
                    anyhow!("row missing column `{c}` (must match first row's keys)")
                })
            })
            .collect::<Result<_>>()?;
        conn.exec_drop(&stmt, Params::Positional(vals))?;
        total += conn.affected_rows() as i64;
    }
    Ok(json!({"inserted": total}))
}

/// Validate a MySQL identifier (table or column name) for safe `format!`
/// interpolation into SQL. Pre-fix `op_schema` raw-interpolated `table` into
/// `DESCRIBE {}` enabling SQL injection. Whitelist: ASCII letters/digits/
/// underscore/dollar, with optional schema-qualified `schema.table` form.
fn validate_identifier(name: &str, what: &str) -> Result<String> {
    if name.is_empty() {
        bail!("`{what}` must not be empty");
    }
    let valid_start = |c: char| c.is_ascii_alphabetic() || c == '_';
    let valid_rest = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '$';
    for (i, part) in name.split('.').enumerate() {
        if part.is_empty() {
            bail!("`{what}` has empty segment (position {i}) in `{name}`");
        }
        let mut chars = part.chars();
        let first = chars.next().expect("non-empty");
        if !valid_start(first) {
            bail!("`{what}` segment `{part}` must start with letter or underscore");
        }
        for c in chars {
            if !valid_rest(c) {
                bail!("`{what}` segment `{part}` contains invalid character `{c}`");
            }
        }
    }
    Ok(name.to_string())
}

fn op_dump(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let table = validate_identifier(
        opts["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?,
        "table",
    )?;
    let limit = opts["limit"].as_i64();
    let sql = match limit {
        Some(n) => format!("SELECT * FROM {} LIMIT {}", table, n),
        None => format!("SELECT * FROM {}", table),
    };
    let stmt = conn.prep(&sql)?;
    let names: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let rows: Vec<Row> = conn.exec(&stmt, ())?;
    let out: Vec<Value> = rows.into_iter().map(|r| row_to_json(r, &names)).collect();
    Ok(json!({"columns": names, "rows": out}))
}

// ── introspection extras ──────────────────────────────────────────────────────

/// Prepare + run `sql` with no params, returning (column_names, rows-as-json).
/// Shared by the catalog/listing ops below.
fn rows_of(conn: &mut mysql::PooledConn, sql: &str) -> Result<(Vec<String>, Vec<Value>)> {
    use mysql::prelude::Queryable;
    let stmt = conn.prep(sql)?;
    let names: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let rows: Vec<Row> = conn.exec(&stmt, ())?;
    let out: Vec<Value> = rows.into_iter().map(|r| row_to_json(r, &names)).collect();
    Ok((names, out))
}

fn op_explain(opts: Value) -> Result<Value> {
    use mysql::prelude::Queryable;
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let sql = opts["sql"].as_str().ok_or_else(|| anyhow!("missing sql"))?;
    let params = params_from_value(&opts["params"]);
    let stmt = conn.prep(format!("EXPLAIN {}", sql))?;
    let names: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let rows: Vec<Row> = match params {
        Params::Empty => conn.exec(&stmt, ())?,
        _ => conn.exec(&stmt, params)?,
    };
    let out: Vec<Value> = rows.into_iter().map(|r| row_to_json(r, &names)).collect();
    Ok(json!({"plan": out}))
}

fn op_views(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let (_, rows) = rows_of(
        &mut conn,
        "SELECT table_name FROM information_schema.views WHERE table_schema = DATABASE() ORDER BY table_name",
    )?;
    let names: Vec<Value> = rows.into_iter().map(|r| r["table_name"].clone()).collect();
    Ok(json!({"views": names}))
}

fn op_procedures(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let (_, rows) = rows_of(
        &mut conn,
        "SELECT routine_name, routine_type FROM information_schema.routines \
         WHERE routine_schema = DATABASE() ORDER BY routine_name",
    )?;
    Ok(json!({"routines": rows}))
}

fn op_indexes(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let table = validate_identifier(
        opts["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?,
        "table",
    )?;
    let (_, rows) = rows_of(&mut conn, &format!("SHOW INDEX FROM {}", table))?;
    Ok(json!({"table": table, "indexes": rows}))
}

fn op_triggers(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let (_, rows) = rows_of(&mut conn, "SHOW TRIGGERS")?;
    Ok(json!({"triggers": rows}))
}

fn op_users(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let (_, rows) = rows_of(
        &mut conn,
        "SELECT user, host FROM mysql.user ORDER BY user, host",
    )?;
    Ok(json!({"users": rows}))
}

fn op_db_size(opts: Value) -> Result<Value> {
    use mysql::prelude::Queryable;
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let bytes: Option<i64> = conn.query_first(
        "SELECT COALESCE(SUM(data_length + index_length), 0) \
         FROM information_schema.tables WHERE table_schema = DATABASE()",
    )?;
    Ok(json!({"bytes": bytes.unwrap_or(0)}))
}

fn op_processlist(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let (_, rows) = rows_of(&mut conn, "SHOW FULL PROCESSLIST")?;
    Ok(json!({"processes": rows}))
}

fn op_status(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let global = opts["global"].as_bool().unwrap_or(true);
    let (_, rows) = rows_of(
        &mut conn,
        if global {
            "SHOW GLOBAL STATUS"
        } else {
            "SHOW SESSION STATUS"
        },
    )?;
    Ok(json!({"status": rows}))
}

fn op_variables(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let global = opts["global"].as_bool().unwrap_or(true);
    let (_, rows) = rows_of(
        &mut conn,
        if global {
            "SHOW GLOBAL VARIABLES"
        } else {
            "SHOW SESSION VARIABLES"
        },
    )?;
    Ok(json!({"variables": rows}))
}

fn op_engines(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let (_, rows) = rows_of(&mut conn, "SHOW ENGINES")?;
    Ok(json!({"engines": rows}))
}

fn op_table_size(opts: Value) -> Result<Value> {
    use mysql::prelude::Queryable;
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let table = validate_identifier(
        opts["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?,
        "table",
    )?;
    let row: Option<(i64, i64)> = conn.exec_first(
        "SELECT COALESCE(data_length, 0), COALESCE(index_length, 0) \
         FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
        (&table,),
    )?;
    let (data, index) = row.unwrap_or((0, 0));
    Ok(json!({"table": table, "data_bytes": data, "index_bytes": index, "bytes": data + index}))
}

fn op_kill(opts: Value) -> Result<Value> {
    use mysql::prelude::Queryable;
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let id = opts["id"]
        .as_i64()
        .ok_or_else(|| anyhow!("missing id (connection id)"))?;
    // KILL doesn't take a placeholder; the id is validated as an integer above.
    conn.query_drop(format!("KILL {}", id))?;
    Ok(json!({"id": id, "killed": true}))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-mysql handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── pure helpers (no connection) ─────────────────────────────────────────────

/// RFC 3986 percent-encode for the URI userinfo / path component — anything
/// outside the unreserved set is escaped, so `@`/`:`/`/` in a password survive.
fn percent_encode_userinfo(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Inverse of `percent_encode_userinfo`. Invalid escapes are left verbatim.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse a URI DSN `mysql|mariadb://[user[:pass]@]host[:port][/db][?k=v…]` into
/// its components (userinfo/db/params percent-decoded). Pure — no connection.
fn op_parse_dsn(opts: Value) -> Result<Value> {
    let dsn = opts
        .get("dsn")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing dsn"))?;
    let (scheme, rest) = dsn
        .split_once("://")
        .ok_or_else(|| anyhow!("not a URI DSN (missing `://`): {dsn}"))?;
    if !matches!(scheme, "mysql" | "mariadb") {
        bail!("unsupported scheme `{scheme}` (want mysql|mariadb)");
    }
    let (authority_path, query) = match rest.split_once('?') {
        Some((ap, q)) => (ap, Some(q)),
        None => (rest, None),
    };
    let (authority, path) = match authority_path.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (authority_path, None),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (user, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(percent_decode(u)), Some(percent_decode(p))),
            None => (Some(percent_decode(ui)), None),
        },
        None => (None, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() => (h.to_string(), p.parse::<u32>().ok()),
        _ => (hostport.to_string(), None),
    };
    let database = path.map(percent_decode);
    let mut params = serde_json::Map::new();
    if let Some(q) = query {
        for pair in q.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            params.insert(percent_decode(k), json!(percent_decode(v)));
        }
    }
    Ok(json!({
        "scheme": scheme,
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "database": database,
        "params": Value::Object(params),
    }))
}

/// Build a URI DSN from explicit parts, percent-encoding userinfo + database.
/// Deterministic — the inverse of `parse_dsn`. opts: user, password, host,
/// port, database.
fn op_build_dsn(opts: Value) -> Result<Value> {
    let user = opts.get("user").and_then(Value::as_str).unwrap_or("root");
    let host = opts
        .get("host")
        .and_then(Value::as_str)
        .unwrap_or("127.0.0.1");
    let port = opts.get("port").and_then(Value::as_u64).unwrap_or(3306);
    let database = opts.get("database").and_then(Value::as_str).unwrap_or("");
    let userinfo = match opts.get("password").and_then(Value::as_str) {
        Some(p) if !p.is_empty() => format!(
            "{}:{}",
            percent_encode_userinfo(user),
            percent_encode_userinfo(p)
        ),
        _ => percent_encode_userinfo(user),
    };
    let dsn = format!(
        "mysql://{}@{}:{}/{}",
        userinfo,
        host,
        port,
        percent_encode_userinfo(database)
    );
    Ok(json!({"dsn": dsn}))
}

/// Quote a MySQL identifier with backticks, doubling any embedded backtick.
/// Backtick-quote a single MySQL identifier, doubling embedded backticks.
fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn op_quote_ident(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    Ok(json!({"quoted": quote_ident(name)}))
}

/// Decode a backtick-quoted MySQL identifier back to its raw name — the inverse
/// of `quote_ident`. The input must be wrapped in matching backticks with every
/// embedded backtick doubled (`` `` `` → `` ` ``); an unpaired backtick is
/// rejected. opts: `quoted` (or `ident`). Returns `{name}`. Pure.
fn op_unquote_ident(opts: Value) -> Result<Value> {
    let input = opts
        .get("quoted")
        .or_else(|| opts.get("ident"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing quoted"))?;
    let inner = input
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .filter(|_| input.len() >= 2)
        .ok_or_else(|| anyhow!("not a backtick-quoted identifier: {input}"))?;
    // Every embedded backtick must be doubled — an odd count means a stray one.
    if inner.matches('`').count() % 2 != 0 {
        return Err(anyhow!(
            "malformed identifier: unpaired backtick in {input}"
        ));
    }
    Ok(json!({ "name": inner.replace("``", "`") }))
}

/// Quote a dotted, qualified identifier — each `.`-separated segment is quoted
/// independently and rejoined, so `mydb.my table` becomes `` `mydb`.`my table` ``.
/// An empty segment (leading, trailing, or doubled dot) is rejected. Pure.
fn op_quote_qualified_ident(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let parts: Vec<&str> = name.split('.').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(anyhow!(
            "qualified identifier has an empty segment: `{name}`"
        ));
    }
    let quoted = parts
        .iter()
        .map(|p| quote_ident(p))
        .collect::<Vec<_>>()
        .join(".");
    Ok(json!({"quoted": quoted, "parts": parts}))
}

/// Parse a dotted, possibly-backtick-quoted qualified identifier into its
/// segments — the inverse of `quote_qualified_ident`. A backtick-quoted segment
/// may contain `.` (kept literal) and a doubled backtick (un-doubled to one);
/// bare segments pass through. A `.` outside backticks separates segments; an
/// unquoted empty segment and an unterminated backtick are rejected. opts:
/// `name` (required). Returns `{parts}`. Pure.
fn op_parse_qualified_ident(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .or_else(|| opts.get("ident"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let chars: Vec<char> = name.chars().collect();
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut had_content = false;
    let mut in_quote = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if in_quote {
            if c == '`' {
                if i + 1 < chars.len() && chars[i + 1] == '`' {
                    cur.push('`');
                    i += 2;
                    continue;
                }
                in_quote = false;
                i += 1;
            } else {
                cur.push(c);
                i += 1;
            }
        } else if c == '`' {
            in_quote = true;
            had_content = true;
            i += 1;
        } else if c == '.' {
            if !had_content {
                return Err(anyhow!("empty segment in qualified identifier: `{name}`"));
            }
            parts.push(std::mem::take(&mut cur));
            had_content = false;
            i += 1;
        } else {
            cur.push(c);
            had_content = true;
            i += 1;
        }
    }
    if in_quote {
        return Err(anyhow!("unterminated quoted identifier: `{name}`"));
    }
    if !had_content {
        return Err(anyhow!("empty segment in qualified identifier: `{name}`"));
    }
    parts.push(cur);
    Ok(json!({ "parts": parts }))
}

/// Quote a single MySQL string literal. Default mode: backslash is an escape
/// char, so escape `\` first, then `'` — `O'Brien` → `'O\'Brien'`.
fn quote_literal_str(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

/// Quote a MySQL string literal. In MySQL's default mode the backslash is an
/// escape character, so escape `\` first, then `'` — `O'Brien` → `'O\'Brien'`.
fn op_quote_literal(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    Ok(json!({"quoted": quote_literal_str(value)}))
}

/// MySQL's `QUOTE()` built-in: quote a value for safe SQL inlining, escaping
/// each backslash, single quote, ASCII NUL (`\0`), and Control-Z (`\Z`) with a
/// backslash and wrapping in single quotes; a NULL (absent or null `value`)
/// returns the unquoted word `NULL`. Stricter than `quote_literal`, which omits
/// the NUL/Ctrl-Z escapes and the NULL handling. opts: `value` (string or
/// null). Returns `{quoted}`. Pure.
fn op_quote(opts: Value) -> Result<Value> {
    match opts.get("value") {
        None | Some(Value::Null) => Ok(json!({"quoted": "NULL"})),
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| anyhow!("value must be a string or null"))?;
            // Backslash first so the escapes added below aren't re-doubled.
            let escaped = s
                .replace('\\', "\\\\")
                .replace('\'', "\\'")
                .replace('\0', "\\0")
                .replace('\u{1a}', "\\Z");
            Ok(json!({"quoted": format!("'{escaped}'")}))
        }
    }
}

/// Escape the LIKE metacharacters in a value so it matches literally in a `LIKE`
/// clause: each `\`, `%`, and `_` is backslash-prefixed (the default LIKE escape
/// is `\`). This is the LIKE-pattern level only — wrap the result with
/// `quote_literal` to inline it as a string (which adds the separate SQL-literal
/// backslash doubling). opts: `value` (required). Returns `{escaped}`. Pure.
/// Escape the LIKE metacharacters in `value` (backslash first so the `%`/`_`
/// escapes it adds aren't themselves doubled). Shared by `escape_like` and
/// `like_pattern`.
fn escape_like_str(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn op_escape_like(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    Ok(json!({ "escaped": escape_like_str(value) }))
}

/// Inverse of `escape_like_str`: decode `\\` → `\`, `\%` → `%`, `\_` → `_` with a
/// single left-to-right scan (a naive sequence of `replace`s would mis-handle a
/// `\\` adjacent to a `%`/`_`). A backslash not introducing one of those escapes
/// is left literal.
fn unescape_like_str(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' && i + 1 < chars.len() && matches!(chars[i + 1], '\\' | '%' | '_') {
            out.push(chars[i + 1]);
            i += 2;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn op_unescape_like(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .or_else(|| opts.get("escaped"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    Ok(json!({ "value": unescape_like_str(value) }))
}

/// Build a MySQL `LIKE` pattern from a literal substring for the common
/// search-box shapes. The substring's LIKE metacharacters (`\`, `%`, `_`) are
/// escaped (as `escape_like` does), then wildcards are added per `mode`:
/// `contains` → `%value%`, `starts_with`/`prefix` → `value%`,
/// `ends_with`/`suffix` → `%value`, `equals`/`exact` → `value` (escaped, no
/// wildcards). Wrap the result with `quote_literal` to inline it. opts: `value`
/// (required), `mode` (default `contains`). Returns `{pattern, mode}`. Pure.
fn op_like_pattern(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let mode = opts
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("contains");
    let esc = escape_like_str(value);
    let pattern = match mode {
        "contains" => format!("%{esc}%"),
        "starts_with" | "prefix" => format!("{esc}%"),
        "ends_with" | "suffix" => format!("%{esc}"),
        "equals" | "exact" => esc,
        other => bail!("unknown mode `{other}` (contains|starts_with|ends_with|equals)"),
    };
    Ok(json!({ "pattern": pattern, "mode": mode }))
}

/// Build a parenthesized, quoted `IN (...)` value list from a list of string
/// `elements` — MySQL's idiom for value sets (it has no array type). Each
/// element is quoted with `quote_literal`'s escaping; an empty list yields
/// `(NULL)` so `col IN (NULL)` is valid SQL that matches nothing. Pure.
fn op_format_in_list(opts: Value) -> Result<Value> {
    let elements = opts
        .get("elements")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing elements (array of strings)"))?;
    if elements.is_empty() {
        return Ok(json!({"list": "(NULL)"}));
    }
    let quoted: Vec<String> = elements
        .iter()
        .map(|e| quote_literal_str(e.as_str().unwrap_or("")))
        .collect();
    Ok(json!({"list": format!("({})", quoted.join(","))}))
}

/// Decode a MySQL string literal back to its raw value — inverse of
/// `quote_literal`. The input must be wrapped in matching `'` or `"` quotes.
/// Backslash escapes follow MySQL's default mode: `\0 \b \n \r \t \Z` map to
/// their control characters, `\' \" \\` to the literal char, `\% \_` keep the
/// backslash (LIKE metacharacters), and any other `\X` collapses to `X`. A
/// doubled quote (`''` inside a `'`-quoted literal) decodes to one quote.
/// opts: value (required). Returns `{value}`. Pure.
/// Decode one MySQL string literal to its raw value (the body of
/// `unquote_literal`). Shared with `parse_in_list`. See `op_unquote_literal`
/// for the escape rules.
fn unquote_literal_str(input: &str) -> Result<String> {
    let mut chars = input.chars();
    let quote = chars
        .next()
        .filter(|c| *c == '\'' || *c == '"')
        .ok_or_else(|| anyhow!("not a quoted literal (must start with ' or \"): {input}"))?;
    let body: Vec<char> = chars.collect();
    if body.last() != Some(&quote) {
        return Err(anyhow!(
            "unterminated literal (no closing {quote}): {input}"
        ));
    }
    let inner = &body[..body.len() - 1];
    let mut out = String::new();
    let mut i = 0;
    while i < inner.len() {
        let c = inner[i];
        if c == '\\' && i + 1 < inner.len() {
            let n = inner[i + 1];
            match n {
                '0' => out.push('\0'),
                'b' => out.push('\u{0008}'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                'Z' => out.push('\u{001a}'),
                '%' => out.push_str("\\%"),
                '_' => out.push_str("\\_"),
                other => out.push(other),
            }
            i += 2;
        } else if c == quote && i + 1 < inner.len() && inner[i + 1] == quote {
            out.push(quote);
            i += 2;
        } else {
            out.push(c);
            i += 1;
        }
    }
    Ok(out)
}

fn op_unquote_literal(opts: Value) -> Result<Value> {
    let input = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    Ok(json!({ "value": unquote_literal_str(input)? }))
}

/// Split an `IN (...)` body at top-level commas — commas inside a `'`/`"` quoted
/// element (respecting `\` escapes and doubled quotes) do not split. Used by
/// `parse_in_list`.
fn split_in_list_elements(s: &str) -> Result<Vec<String>> {
    let chars: Vec<char> = s.chars().collect();
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match quote {
            Some(q) => {
                cur.push(c);
                if c == '\\' && i + 1 < chars.len() {
                    cur.push(chars[i + 1]);
                    i += 2;
                    continue;
                } else if c == q {
                    if i + 1 < chars.len() && chars[i + 1] == q {
                        cur.push(q);
                        i += 2;
                        continue;
                    }
                    quote = None;
                }
                i += 1;
            }
            None => {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                    cur.push(c);
                } else if c == ',' {
                    parts.push(std::mem::take(&mut cur));
                } else {
                    cur.push(c);
                }
                i += 1;
            }
        }
    }
    if quote.is_some() {
        bail!("unterminated quoted element in IN list");
    }
    parts.push(cur);
    Ok(parts)
}

/// Parse a MySQL `IN (...)` value list back into its elements — the inverse of
/// `format_in_list`. The list is split at top-level commas (commas inside a
/// quoted element are ignored); each element is decoded with `unquote_literal`'s
/// rules when quoted, an unquoted `NULL` (case-insensitive) becomes a JSON null,
/// and any other bare token is returned as-is. `format_in_list` renders an empty
/// list as `(NULL)`, so that round-trips to a single null rather than an empty
/// list (the sentinel is inherently lossy). opts: `list` (or `value`). Returns
/// `{values, count}`. Pure.
fn op_parse_in_list(opts: Value) -> Result<Value> {
    let raw = opts
        .get("list")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing list"))?;
    let inner = raw
        .trim()
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| anyhow!("not a parenthesized IN list: {raw}"))?
        .trim();
    if inner.is_empty() {
        return Ok(json!({ "values": [], "count": 0 }));
    }
    let mut values: Vec<Value> = Vec::new();
    for elem in split_in_list_elements(inner)? {
        let e = elem.trim();
        if e.eq_ignore_ascii_case("NULL") {
            values.push(Value::Null);
        } else if e.starts_with('\'') || e.starts_with('"') {
            values.push(Value::String(unquote_literal_str(e)?));
        } else {
            values.push(Value::String(e.to_string()));
        }
    }
    let count = values.len();
    Ok(json!({ "values": values, "count": count }))
}

/// Parse a MySQL `ENUM(...)` / `SET(...)` column type into its member values —
/// the `information_schema.COLUMNS.COLUMN_TYPE` form (e.g. `enum('small','large')`
/// or `set('a','b')`). Members are single-quoted with embedded quotes doubled
/// (`''`) and MySQL backslash escapes, both decoded here by the same literal
/// parsing `parse_in_list` uses. The `enum`/`set` keyword is case-insensitive.
/// opts: `type` (or `value`, required). Returns `{type, kind, values, count}`.
/// Pure.
fn op_parse_enum(opts: Value) -> Result<Value> {
    let raw = opts
        .get("type")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing type"))?;
    let s = raw.trim();
    let lower = s.to_ascii_lowercase();
    let (kind, plen) = if lower.starts_with("enum(") {
        ("enum", 5)
    } else if lower.starts_with("set(") {
        ("set", 4)
    } else {
        return Err(anyhow!(
            "not an enum/set type (want enum(...) or set(...)): {raw}"
        ));
    };
    let inner = s[plen..]
        .strip_suffix(')')
        .ok_or_else(|| anyhow!("unterminated {kind}(...): {raw}"))?
        .trim();
    let mut values: Vec<Value> = Vec::new();
    if !inner.is_empty() {
        for elem in split_in_list_elements(inner)? {
            let e = elem.trim();
            if !(e.starts_with('\'') || e.starts_with('"')) {
                return Err(anyhow!("enum/set member must be a quoted string: {e}"));
            }
            values.push(Value::String(unquote_literal_str(e)?));
        }
    }
    let count = values.len();
    Ok(json!({ "type": raw, "kind": kind, "values": values, "count": count }))
}

/// Build a MySQL `ENUM(...)`/`SET(...)` column type from a list of member values
/// — the inverse of `parse_enum`. Each member is quoted as a SQL string literal
/// (reusing `quote_literal_str`, so embedded `'`/`\` are escaped), the keyword is
/// upper-cased (`ENUM`/`SET`), and the list must be non-empty (an empty
/// `ENUM()`/`SET()` is invalid MySQL). `parse_enum` round-trips the result. opts:
/// `values` (non-empty array of strings, required), `kind` (`enum` default, or
/// `set`). Returns `{type, kind, values, count}`. Pure.
fn op_build_enum(opts: Value) -> Result<Value> {
    let kind = opts
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("enum")
        .to_ascii_lowercase();
    let keyword = match kind.as_str() {
        "enum" => "ENUM",
        "set" => "SET",
        other => return Err(anyhow!("kind must be `enum` or `set`, got `{other}`")),
    };
    let values = opts
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing values (array of strings)"))?;
    if values.is_empty() {
        return Err(anyhow!("{kind} needs at least one value"));
    }
    let mut members = Vec::with_capacity(values.len());
    for v in values {
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("every value must be a string"))?;
        members.push(quote_literal_str(s));
    }
    let type_decl = format!("{keyword}({})", members.join(","));
    Ok(json!({ "type": type_decl, "kind": kind, "values": values, "count": values.len() }))
}

/// The 1-based index MySQL assigns to a `value` within an `ENUM`/`SET` `type` —
/// the internal integer that `ORDER BY` sorts on. Per the MySQL reference: the
/// listed members are numbered from 1, the empty-string error value `''` is 0,
/// and a value not in the enumeration is reported as `null` (MySQL would store
/// `''` on insert, but for a lookup `null` flags "not a member"). Membership is
/// matched ASCII-case-insensitively, like the default collation. opts: `type`
/// (an `enum(...)`/`set(...)` declaration) and `value`. Returns `{value, index}`.
/// Pure.
fn op_enum_index(opts: Value) -> Result<Value> {
    let type_str = opts
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing type"))?;
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let parsed = op_parse_enum(json!({ "type": type_str }))?;
    let members = parsed["values"]
        .as_array()
        .ok_or_else(|| anyhow!("could not read enum members"))?;
    if value.is_empty() {
        return Ok(json!({ "value": value, "index": 0 }));
    }
    for (i, m) in members.iter().enumerate() {
        if m.as_str().is_some_and(|s| s.eq_ignore_ascii_case(value)) {
            return Ok(json!({ "value": value, "index": i + 1 }));
        }
    }
    Ok(json!({ "value": value, "index": Value::Null }))
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn mysql__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn mysql__version(args: *const c_char) -> *const c_char {
    ffi_call(args, op_version)
}

#[no_mangle]
pub extern "C" fn mysql__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, op_ping)
}

#[no_mangle]
pub extern "C" fn mysql__databases(args: *const c_char) -> *const c_char {
    ffi_call(args, op_databases)
}

#[no_mangle]
pub extern "C" fn mysql__tables(args: *const c_char) -> *const c_char {
    ffi_call(args, op_tables)
}

#[no_mangle]
pub extern "C" fn mysql__schema(args: *const c_char) -> *const c_char {
    ffi_call(args, op_schema)
}

#[no_mangle]
pub extern "C" fn mysql__query(args: *const c_char) -> *const c_char {
    ffi_call(args, op_query)
}

#[no_mangle]
pub extern "C" fn mysql__execute(args: *const c_char) -> *const c_char {
    ffi_call(args, op_execute)
}

#[no_mangle]
pub extern "C" fn mysql__exec(args: *const c_char) -> *const c_char {
    ffi_call(args, op_exec)
}

#[no_mangle]
pub extern "C" fn mysql__insert_many(args: *const c_char) -> *const c_char {
    ffi_call(args, op_insert_many)
}

#[no_mangle]
pub extern "C" fn mysql__dump(args: *const c_char) -> *const c_char {
    ffi_call(args, op_dump)
}

#[no_mangle]
pub extern "C" fn mysql__transaction(args: *const c_char) -> *const c_char {
    ffi_call(args, op_transaction)
}

#[no_mangle]
pub extern "C" fn mysql__call(args: *const c_char) -> *const c_char {
    ffi_call(args, op_call)
}

#[no_mangle]
pub extern "C" fn mysql__query_multi(args: *const c_char) -> *const c_char {
    ffi_call(args, op_query_multi)
}

#[no_mangle]
pub extern "C" fn mysql__explain(args: *const c_char) -> *const c_char {
    ffi_call(args, op_explain)
}

#[no_mangle]
pub extern "C" fn mysql__views(args: *const c_char) -> *const c_char {
    ffi_call(args, op_views)
}

#[no_mangle]
pub extern "C" fn mysql__procedures(args: *const c_char) -> *const c_char {
    ffi_call(args, op_procedures)
}

#[no_mangle]
pub extern "C" fn mysql__indexes(args: *const c_char) -> *const c_char {
    ffi_call(args, op_indexes)
}

#[no_mangle]
pub extern "C" fn mysql__triggers(args: *const c_char) -> *const c_char {
    ffi_call(args, op_triggers)
}

#[no_mangle]
pub extern "C" fn mysql__users(args: *const c_char) -> *const c_char {
    ffi_call(args, op_users)
}

#[no_mangle]
pub extern "C" fn mysql__db_size(args: *const c_char) -> *const c_char {
    ffi_call(args, op_db_size)
}

#[no_mangle]
pub extern "C" fn mysql__processlist(args: *const c_char) -> *const c_char {
    ffi_call(args, op_processlist)
}

#[no_mangle]
pub extern "C" fn mysql__status(args: *const c_char) -> *const c_char {
    ffi_call(args, op_status)
}

#[no_mangle]
pub extern "C" fn mysql__variables(args: *const c_char) -> *const c_char {
    ffi_call(args, op_variables)
}

#[no_mangle]
pub extern "C" fn mysql__engines(args: *const c_char) -> *const c_char {
    ffi_call(args, op_engines)
}

#[no_mangle]
pub extern "C" fn mysql__table_size(args: *const c_char) -> *const c_char {
    ffi_call(args, op_table_size)
}

#[no_mangle]
pub extern "C" fn mysql__kill(args: *const c_char) -> *const c_char {
    ffi_call(args, op_kill)
}

#[no_mangle]
pub extern "C" fn mysql__parse_dsn(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_dsn)
}

#[no_mangle]
pub extern "C" fn mysql__build_dsn(args: *const c_char) -> *const c_char {
    ffi_call(args, op_build_dsn)
}

#[no_mangle]
pub extern "C" fn mysql__quote_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote_ident)
}

#[no_mangle]
pub extern "C" fn mysql__unquote_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unquote_ident)
}

#[no_mangle]
pub extern "C" fn mysql__quote_qualified_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote_qualified_ident)
}

#[no_mangle]
pub extern "C" fn mysql__parse_qualified_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_qualified_ident)
}

#[no_mangle]
pub extern "C" fn mysql__quote_literal(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote_literal)
}

#[no_mangle]
pub extern "C" fn mysql__quote(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote)
}

#[no_mangle]
pub extern "C" fn mysql__escape_like(args: *const c_char) -> *const c_char {
    ffi_call(args, op_escape_like)
}

#[no_mangle]
pub extern "C" fn mysql__unescape_like(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unescape_like)
}

#[no_mangle]
pub extern "C" fn mysql__like_pattern(args: *const c_char) -> *const c_char {
    ffi_call(args, op_like_pattern)
}

#[no_mangle]
pub extern "C" fn mysql__unquote_literal(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unquote_literal)
}

#[no_mangle]
pub extern "C" fn mysql__format_in_list(args: *const c_char) -> *const c_char {
    ffi_call(args, op_format_in_list)
}

#[no_mangle]
pub extern "C" fn mysql__parse_in_list(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_in_list)
}

#[no_mangle]
pub extern "C" fn mysql__parse_enum(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_enum)
}

#[no_mangle]
pub extern "C" fn mysql__build_enum(args: *const c_char) -> *const c_char {
    ffi_call(args, op_build_enum)
}

#[no_mangle]
pub extern "C" fn mysql__enum_index(args: *const c_char) -> *const c_char {
    ffi_call(args, op_enum_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(f: F) {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let saved = std::env::var("MYSQL_URL").ok();
        std::env::remove_var("MYSQL_URL");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        match saved {
            Some(s) => std::env::set_var("MYSQL_URL", s),
            None => std::env::remove_var("MYSQL_URL"),
        }
        if let Err(p) = result {
            std::panic::resume_unwind(p);
        }
    }

    // ── url_from_opts ──

    #[test]
    fn url_opts_url_wins_over_env() {
        with_env(|| {
            std::env::set_var("MYSQL_URL", "mysql://env@h/db");
            assert_eq!(
                url_from_opts(&json!({"url": "mysql://opts@h/db"})),
                "mysql://opts@h/db"
            );
        });
    }

    #[test]
    fn url_falls_back_to_env() {
        with_env(|| {
            std::env::set_var("MYSQL_URL", "mysql://env@host/db");
            assert_eq!(url_from_opts(&json!({})), "mysql://env@host/db");
        });
    }

    #[test]
    fn url_default_when_unset() {
        with_env(|| {
            assert_eq!(url_from_opts(&json!({})), "mysql://root@127.0.0.1:3306/");
        });
    }

    #[test]
    fn url_built_from_host_port_user_db() {
        with_env(|| {
            let opts = json!({
                "host": "db.example.com",
                "port": 3307,
                "user": "ada",
                "database": "shop",
            });
            assert_eq!(url_from_opts(&opts), "mysql://ada@db.example.com:3307/shop");
        });
    }

    #[test]
    fn url_includes_password_when_set() {
        with_env(|| {
            let opts = json!({"user": "ada", "password": "hunter2"});
            // `mysql://ada:hunter2@127.0.0.1:3306/`
            let u = url_from_opts(&opts);
            assert!(u.contains("ada:hunter2@"), "{u}");
        });
    }

    #[test]
    fn url_omits_password_marker_when_blank() {
        with_env(|| {
            let opts = json!({"user": "ada"});
            let u = url_from_opts(&opts);
            assert!(u.contains("mysql://ada@"), "{u}");
            assert!(!u.contains(":@"), "stray colon: {u}");
        });
    }

    // ── json_to_my_value ──

    #[test]
    fn j2mv_null() {
        assert!(matches!(json_to_my_value(&Value::Null), MyValue::NULL));
    }

    #[test]
    fn j2mv_bool_maps_to_int_01() {
        match json_to_my_value(&json!(true)) {
            MyValue::Int(1) => {}
            other => panic!("expected Int(1), got {other:?}"),
        }
        match json_to_my_value(&json!(false)) {
            MyValue::Int(0) => {}
            other => panic!("expected Int(0), got {other:?}"),
        }
    }

    #[test]
    fn j2mv_signed_int() {
        match json_to_my_value(&json!(-7)) {
            MyValue::Int(-7) => {}
            other => panic!("expected Int(-7), got {other:?}"),
        }
    }

    #[test]
    fn j2mv_float() {
        match json_to_my_value(&json!(1.5)) {
            MyValue::Double(f) if (f - 1.5).abs() < 1e-9 => {}
            other => panic!("expected Double(1.5), got {other:?}"),
        }
    }

    #[test]
    fn j2mv_string_to_bytes() {
        match json_to_my_value(&json!("hi")) {
            MyValue::Bytes(b) => assert_eq!(b, b"hi"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn j2mv_array_serializes_as_json_bytes() {
        match json_to_my_value(&json!([1, 2])) {
            MyValue::Bytes(b) => assert_eq!(std::str::from_utf8(&b).unwrap(), "[1,2]"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    // ── my_value_to_json ──

    #[test]
    fn mv2j_null() {
        assert_eq!(my_value_to_json(&MyValue::NULL), Value::Null);
    }

    #[test]
    fn mv2j_bytes_utf8_string() {
        let v = my_value_to_json(&MyValue::Bytes(b"hello".to_vec()));
        assert_eq!(v, json!("hello"));
    }

    #[test]
    fn mv2j_bytes_non_utf8_falls_back_to_marker() {
        let v = my_value_to_json(&MyValue::Bytes(vec![0xFF, 0xFE, 0xFD]));
        assert_eq!(v, json!("<binary 3 bytes>"));
    }

    #[test]
    fn mv2j_int_and_float() {
        assert_eq!(my_value_to_json(&MyValue::Int(42)), json!(42));
        assert_eq!(my_value_to_json(&MyValue::UInt(99)), json!(99));
        assert_eq!(my_value_to_json(&MyValue::Double(1.25)), json!(1.25));
    }

    #[test]
    fn mv2j_date_format_zero_padded() {
        let v = my_value_to_json(&MyValue::Date(2026, 6, 9, 14, 30, 5, 0));
        assert_eq!(v, json!("2026-06-09 14:30:05"));
    }

    #[test]
    fn mv2j_time_negative_prefix() {
        let v = my_value_to_json(&MyValue::Time(true, 1, 2, 3, 4, 0));
        assert_eq!(v, json!("-1d 02:03:04"));
        let v = my_value_to_json(&MyValue::Time(false, 0, 1, 2, 3, 0));
        assert_eq!(v, json!("0d 01:02:03"));
    }

    // ── params_from_value ──

    #[test]
    fn params_array_yields_positional() {
        let p = params_from_value(&json!([1, "two", null]));
        assert!(matches!(p, Params::Positional(_)));
        if let Params::Positional(v) = p {
            assert_eq!(v.len(), 3);
        }
    }

    #[test]
    fn params_non_array_yields_empty() {
        assert!(matches!(params_from_value(&json!({"a": 1})), Params::Empty));
        assert!(matches!(params_from_value(&Value::Null), Params::Empty));
        assert!(matches!(params_from_value(&json!("scalar")), Params::Empty));
    }

    /// Empty array yields a `Positional(vec![])`, NOT `Empty` —
    /// distinguishes "supplied empty" from "not supplied". A query like
    /// `SELECT 1` with `params => []` should send 0 bind values without
    /// erroring; pin so a refactor that coerces empty→Empty (which would
    /// hide the explicit-but-empty contract) gets caught.
    #[test]
    fn params_empty_array_is_positional_not_empty() {
        let p = params_from_value(&json!([]));
        assert!(matches!(p, Params::Positional(_)));
        if let Params::Positional(v) = p {
            assert_eq!(v.len(), 0);
        }
    }

    /// JSON numbers that overflow i64 must fall back to f64 (or string),
    /// NOT panic. MySQL accepts BIGINT UNSIGNED up to 2^64-1, but serde
    /// caps at i64 — we need a graceful coercion path.
    #[test]
    fn j2mv_overflowing_number_does_not_panic() {
        let huge = json!(u64::MAX); // i64::MAX < x < u64::MAX
        let v = json_to_my_value(&huge);
        // Concrete shape can vary across serde_json versions; just pin
        // that we don't panic and produce *some* MyValue.
        let _ = v;
    }

    // Precision is preserved: a non-integral JSON number binds as f64
    // (mysql DOUBLE), not the old lossy `f as f32`. `0.1` is the canonical
    // non-representable float; the f64 approximation must survive the bind
    // round-trip bit-for-bit, so DECIMAL/monetary/coordinate values are not
    // silently truncated. A regression back to `f as f32` flips this.
    #[test]
    fn j2mv_f64_binds_as_double_without_f32_truncation() {
        let original_f64: f64 = 0.1;
        let lossy_f32_bits = (original_f64 as f32) as f64;
        match json_to_my_value(&json!(0.1_f64)) {
            MyValue::Double(f) => {
                assert_eq!(
                    f, original_f64,
                    "JSON 0.1 must bind as the exact f64, not a truncated value",
                );
                assert_ne!(
                    f, lossy_f32_bits,
                    "the bound value must NOT match the f32-rounded 0.1 — that \
                     would mean the old `f as f32` cast is back",
                );
            }
            other => panic!("JSON 0.1 must go through the Double (f64) arm, got {other:?}",),
        }
    }

    // UTF-8 fidelity: split_sql_statements must not corrupt multibyte text.
    // The old `b as char` accumulation reinterpreted each continuation byte
    // as Latin-1, so `'café'` came back as `'cafÃ©'`. Pin byte-exact survival
    // of a non-ASCII string literal through the splitter.
    #[test]
    fn split_sql_preserves_utf8_string_literals() {
        let got = split_sql_statements("SELECT 'café'; SELECT 'naïve'");
        assert_eq!(
            got,
            vec!["SELECT 'café'".to_string(), " SELECT 'naïve'".to_string()]
        );
    }

    // SQL-style `/` in a password breaks URL parsing the same way `@`
    // does (see existing `url_password_with_at_sign_is_currently_not_escaped`
    // at lib.rs:626) but with a different fingerprint: the mysql URL
    // parser interprets the first `/` after host as the
    // database-name separator. So a password `p/wd` and database `shop`
    // gets concatenated into `mysql://ada:p/wd@host:3306/shop`, which the
    // parser sees as user=`ada`, password=`p`, host=`wd@host`, port
    // garbage, dbname=`shop`. The existing `@` test only catches the
    // user/host boundary class; this catches the path-separator class.
    // If lib.rs:52-57 starts percent-encoding the password (e.g. `%2F`
    // for `/`), this test flips deliberately and the boss can confirm
    // the fix is intentional.
    #[test]
    fn url_password_with_slash_is_currently_not_escaped() {
        with_env(|| {
            let u = url_from_opts(&json!({
                "host": "real-host",
                "user": "ada",
                "password": "p/wd",
                "database": "shop",
            }));
            // Raw concat: `mysql://ada:p/wd@real-host:3306/shop`.
            assert!(
                u.contains("ada:p/wd@real-host"),
                "expected raw-concat shape with embedded slash, got {u}",
            );
            // Fingerprint: three `/` characters instead of the expected
            // two (`mysql://` + path) — the extra slash is the bug.
            assert_eq!(
                u.matches('/').count(),
                4,
                "two `mysql://` slashes + the bug slash + the dbname slash = 4; got {u}",
            );
        });
    }

    // `port` is read as `i64` (lib.rs:48) with no range check, so any
    // signed value flows directly into `format!("...:{port}/...")`.
    // A negative or zero port produces a syntactically-malformed-but-
    // accepted URL (`mysql://root@127.0.0.1:0/` / `:-1/`). The mysql
    // pool will then error at connect time with a confusing low-level
    // message instead of at config time with `invalid port`. Pin the
    // current "no validation" behavior so a future bounds check
    // (`(1..=65535).contains(&port)`) is detected and the caller-facing
    // error message can be reviewed.
    #[test]
    fn url_port_zero_and_negative_are_currently_accepted_verbatim() {
        with_env(|| {
            let u_zero = url_from_opts(&json!({"port": 0}));
            assert!(
                u_zero.contains(":0/"),
                "port 0 must currently pass through unvalidated, got {u_zero}",
            );
            let u_neg = url_from_opts(&json!({"port": -1}));
            assert!(
                u_neg.contains(":-1/"),
                "negative port must currently pass through unvalidated, got {u_neg}",
            );
            let u_huge = url_from_opts(&json!({"port": 999_999}));
            assert!(
                u_huge.contains(":999999/"),
                "out-of-u16-range port must currently pass through unvalidated, got {u_huge}",
            );
        });
    }

    // `row_to_json` (lib.rs:121-128) builds the per-row JSON object by
    // repeated `Map::insert(name, value)`. When two columns in the same
    // result set share a name — which happens naturally with `SELECT *
    // FROM a JOIN b USING (id)` (both `a.id` and `b.id` are reported as
    // `id`), or `SELECT a.name, b.name FROM ...` without aliases — the
    // second insert overwrites the first. The first column's value is
    // silently dropped, NOT surfaced as an error or disambiguated.
    //
    // This is a real data-loss class: stryke callers who do `SELECT t1.x,
    // t2.x FROM ...` see only one `x`. Pin so a future fix (suffix
    // disambiguation, error, array-valued duplicate) is a deliberate
    // behavior change. The check counts keys in the produced object — it
    // must be 1, not 2, to prove the silent overwrite happens.
    #[test]
    fn row_to_json_duplicate_column_names_silently_overwrite() {
        // Can't easily build a real `mysql::Row` without the crate's
        // private constructors, so we go through `Map::insert` directly
        // — that's the exact code at lib.rs:125. The test pins the
        // semantic contract (overwrite-on-duplicate), not the wiring.
        let mut obj = Map::new();
        let names = ["id", "id"];
        let vals = [MyValue::Int(1), MyValue::Int(2)];
        for (n, v) in names.iter().zip(vals.iter()) {
            obj.insert((*n).to_string(), my_value_to_json(v));
        }
        assert_eq!(
            obj.len(),
            1,
            "duplicate `id` columns must collapse to one entry; if this is now 2, \
             row_to_json was changed to disambiguate — confirm intentional",
        );
        assert_eq!(
            obj.get("id"),
            Some(&json!(2)),
            "second value must win (last-insert-wins on Map); got {obj:?}",
        );
    }

    // `params_from_value` (lib.rs:130-135) handles ONLY two shapes:
    // JSON array → `Params::Positional`, everything else → `Params::Empty`.
    // A JSON OBJECT (`{"name": "ada"}`) hits the `None` arm of `as_array`
    // and returns `Params::Empty` — completely silently. The MySQL crate
    // supports `Params::Named` for `:name` placeholders, but stryke
    // callers passing `params => {name: "ada"}` against `SELECT * FROM u
    // WHERE name = :name` get a prepared statement executed with ZERO
    // bind values — which either errors at the DB layer with a confusing
    // "wrong number of parameters" or, worse, executes with NULL/no
    // filter and returns the wrong rows.
    //
    // The existing `params_non_array_yields_empty` test (lib.rs:583) is
    // a smoke test that lumps object/null/scalar together. This test
    // separates out the object case specifically because it's the only
    // one where the caller has a coherent intent that's being silently
    // discarded. If `params_from_value` is updated to convert
    // `Value::Object` to `Params::Named`, this test flips deliberately.
    #[test]
    fn params_object_silently_drops_named_bindings() {
        let p = params_from_value(&json!({"name": "ada", "id": 7}));
        match p {
            Params::Empty => {} // current behavior: silently dropped
            Params::Named(_) => panic!(
                "named params now supported — update this pin and confirm \
                 lib.rs:130-135 mapped Value::Object to Params::Named",
            ),
            other => panic!(
                "unexpected Params variant for object input: {other:?}; \
                 lib.rs:130-135 was changed in a way that needs review",
            ),
        }
    }

    // `my_value_to_json` (lib.rs:106-109) formats `MyValue::Date` with
    // `{:04}-{:02}-{:02} {:02}:{:02}:{:02}` and zero range validation.
    // MySQL's "zero date" (`0000-00-00 00:00:00`) is a real value
    // returned for columns with `NOT NULL DEFAULT '0000-00-00'` in
    // strict-mode-off servers — every legacy MySQL has them. The current
    // code formats this as the literal string `"0000-00-00 00:00:00"`,
    // which:
    //   (a) cannot round-trip through chrono::NaiveDateTime (chrono
    //       rejects year 0 / month 0 / day 0),
    //   (b) is indistinguishable from a real January-of-year-0 value if
    //       one ever showed up,
    //   (c) is the canonical MySQL gotcha that every ORM has to handle
    //       explicitly (sqlx returns Option<NaiveDateTime> and gives
    //       None for zero-date; diesel errors; this crate just passes
    //       the malformed string through).
    //
    // Pin the current pass-through format so a future fix (map zero-date
    // to JSON null, or to a sentinel string) shows up as a behavior
    // change. Also pin out-of-range month/day to confirm there's NO
    // validation — important for the stryke caller who needs to know
    // they're getting raw DB bytes, not validated dates.
    #[test]
    fn mv2j_zero_date_passes_through_as_literal_string() {
        let v = my_value_to_json(&MyValue::Date(0, 0, 0, 0, 0, 0, 0));
        assert_eq!(
            v,
            json!("0000-00-00 00:00:00"),
            "zero-date must currently pass through as a literal string; \
             if this is now null/error, lib.rs:106-109 added validation — confirm",
        );
    }

    #[test]
    fn mv2j_out_of_range_date_fields_pass_through_unvalidated() {
        // Month 13, day 32, hour 25, minute 99 — all impossible.
        let v = my_value_to_json(&MyValue::Date(2026, 13, 32, 25, 99, 99, 0));
        assert_eq!(
            v,
            json!("2026-13-32 25:99:99"),
            "out-of-range date fields must currently pass through unvalidated; \
             if this is now an error/null, lib.rs:106-109 added range checks — confirm",
        );
    }

    // ── split_sql_statements ──

    // The whole reason split_sql_statements exists (lib.rs:234-248 comment) is
    // that a naive `sql.split(';')` mangles a semicolon inside a string literal.
    // This is the regression test for the exact example quoted in that comment:
    // `VALUES ('hello; world')` must stay ONE statement, not split at the
    // embedded `;`. If the splitter ever loses single-quote tracking, this
    // collapses back to the pre-fix two-fragment breakage.
    #[test]
    fn split_semicolon_inside_single_quote_stays_one_statement() {
        let got = split_sql_statements("INSERT INTO t (msg) VALUES ('hello; world')");
        assert_eq!(
            got,
            vec!["INSERT INTO t (msg) VALUES ('hello; world')".to_string()],
            "embedded `;` inside a quoted literal must not split the statement",
        );
    }

    // Doubled-quote SQL escaping (`''` = one literal quote, not string-close)
    // is handled at lib.rs:271-275. A literal `'a;b'` written as `'''a;b'''`
    // (quote-escaped) must keep the inner `;` protected AND the whole thing as
    // one statement. Off-by-one in the doubled-quote skip would either re-enter
    // string mode (eat the rest of the input) or exit early (expose the `;`).
    // This pins the escape interplay with the splitter together.
    #[test]
    fn split_doubled_quote_escape_keeps_semicolon_protected() {
        let got = split_sql_statements("SELECT '''a;b'''");
        assert_eq!(
            got,
            vec!["SELECT '''a;b'''".to_string()],
            "doubled-quote escaped literal containing `;` must remain one statement",
        );
    }

    // Block comments (`/* … */`, lib.rs:299-310) must swallow an embedded `;`.
    // The terminator scan is `i + 1 < bytes.len()` guarded, an off-by-one prone
    // loop: if the closing `*/` lands at the very end of input it must still be
    // consumed. This input puts `;` inside the comment AND a real `;` after it,
    // so a broken comment-skip reveals itself as the wrong split count.
    #[test]
    fn split_block_comment_hides_semicolon_real_one_still_splits() {
        let got = split_sql_statements("A /* x; y */ B; C");
        assert_eq!(
            got,
            vec!["A /* x; y */ B".to_string(), " C".to_string()],
            "`;` inside /* */ must not split; the `;` after the comment must",
        );
    }

    // Trailing `;` (lib.rs:311-315 pushes+clears `cur`, then lib.rs:322 only
    // pushes a NON-empty trailing `cur`) must NOT emit a spurious empty final
    // statement. op_exec (lib.rs:240-246) trims+skips empties so this is
    // belt-and-suspenders, but the off-by-one "do we append the empty tail?"
    // is exactly the kind of thing a refactor breaks. Pin: one statement, not
    // `["A", ""]`.
    #[test]
    fn split_trailing_semicolon_yields_no_empty_tail() {
        assert_eq!(
            split_sql_statements("SELECT 1;"),
            vec!["SELECT 1".to_string()],
            "trailing `;` must not produce a phantom empty final statement",
        );
        // And empty input is the empty vec, not `[""]`.
        assert!(
            split_sql_statements("").is_empty(),
            "empty SQL must yield zero statements",
        );
    }

    // ── validate_identifier ──

    // validate_identifier (lib.rs:384-406) is the SQL-injection guard for the
    // raw `format!`-interpolated identifiers in op_schema / op_dump /
    // op_insert_many. The whole point is to REJECT anything that could break
    // out of an identifier position. These are the adversarial payloads that
    // MUST be refused; if any starts passing, the injection door is reopened.
    #[test]
    fn validate_identifier_rejects_injection_payloads() {
        for bad in [
            "users; DROP TABLE x", // statement break
            "a b",                 // space
            "a-b",                 // dash (not in whitelist)
            "tbl`",                // backtick break-out
            "tbl'",                // quote
            "tbl)",                // paren
            "tbl ",                // trailing space
            " tbl",                // leading space
            "1col",                // digit start (valid_rest but not valid_start)
            "$col",                // `$` allowed in rest, NOT as first char
            "",                    // empty
            ".",                   // empty segments both sides
            "db.",                 // empty trailing segment
            ".tbl",                // empty leading segment
            "a..b",                // empty middle segment
            "naïve",               // non-ASCII (chars()-based, must reject)
        ] {
            assert!(
                validate_identifier(bad, "table").is_err(),
                "must reject injection/invalid identifier {bad:?}",
            );
        }
    }

    // Conversely, the legitimate identifier shapes the connector depends on
    // MUST pass unchanged: bare names, underscores, mid-name digits, the `$`
    // char (legal in MySQL identifiers), and the schema-qualified `db.table`
    // form that op_schema/op_dump accept. A too-strict tightening that broke
    // `schema.table` would silently make every cross-schema DESCRIBE fail; pin
    // it. Returned string must equal the input verbatim (no normalization).
    #[test]
    fn validate_identifier_accepts_legal_forms_verbatim() {
        for good in [
            "users",
            "_tmp",
            "col1",
            "wsrep$status",
            "shop.orders",
            "_a.b1",
        ] {
            assert_eq!(
                validate_identifier(good, "table").ok().as_deref(),
                Some(good),
                "legal identifier {good:?} must pass through unchanged",
            );
        }
    }

    // ── new-surface validation (rejects before opening a connection) ─────────

    #[test]
    fn transaction_requires_nonempty_valid_statements() {
        with_env(|| {
            assert!(op_transaction(json!({}))
                .unwrap_err()
                .to_string()
                .contains("missing statements"));
            assert!(op_transaction(json!({"statements": []}))
                .unwrap_err()
                .to_string()
                .contains("non-empty"));
            // A statement without a sql string is rejected before any connect.
            assert!(op_transaction(json!({"statements": [{"params": [1]}]}))
                .unwrap_err()
                .to_string()
                .contains("`sql` string"));
        });
    }

    #[test]
    fn call_requires_valid_proc_identifier() {
        with_env(|| {
            assert!(op_call(json!({}))
                .unwrap_err()
                .to_string()
                .contains("missing proc"));
            // An injection-shaped proc name must be rejected by the identifier
            // validator before it reaches the CALL string.
            assert!(op_call(json!({"proc": "p; DROP TABLE x"})).is_err());
        });
    }

    #[test]
    fn query_multi_requires_sql() {
        with_env(|| {
            assert!(op_query_multi(json!({}))
                .unwrap_err()
                .to_string()
                .contains("missing sql"));
        });
    }

    // ── pure DSN / quoting helpers (no connection) ───────────────────────────

    #[test]
    fn parse_dsn_full_uri_decomposes_every_part() {
        let v = op_parse_dsn(json!({
            "dsn": "mysql://app:s3cret@db.example.com:3307/shop?charset=utf8mb4&ssl_mode=REQUIRED"
        }))
        .unwrap();
        assert_eq!(v["scheme"], json!("mysql"));
        assert_eq!(v["user"], json!("app"));
        assert_eq!(v["password"], json!("s3cret"));
        assert_eq!(v["host"], json!("db.example.com"));
        assert_eq!(v["port"], json!(3307));
        assert_eq!(v["database"], json!("shop"));
        assert_eq!(v["params"]["charset"], json!("utf8mb4"));
        assert_eq!(v["params"]["ssl_mode"], json!("REQUIRED"));
    }

    #[test]
    fn parse_dsn_percent_decodes_userinfo_and_accepts_mariadb() {
        let v = op_parse_dsn(json!({"dsn": "mariadb://u:p%40ss@localhost/db"})).unwrap();
        assert_eq!(v["scheme"], json!("mariadb"));
        assert_eq!(v["password"], json!("p@ss"));
        assert_eq!(v["port"], Value::Null);
    }

    #[test]
    fn parse_dsn_rejects_bad_scheme_and_non_uri() {
        assert!(op_parse_dsn(json!({"dsn": "postgres://localhost/x"})).is_err());
        assert!(op_parse_dsn(json!({"dsn": "host=localhost"})).is_err());
        assert!(op_parse_dsn(json!({})).is_err());
    }

    #[test]
    fn build_dsn_round_trips_through_parse() {
        let built = op_build_dsn(json!({
            "user": "u", "password": "p@ss/word", "host": "127.0.0.1", "port": 3306, "database": "app"
        }))
        .unwrap();
        let dsn = built["dsn"].as_str().unwrap();
        let parsed = op_parse_dsn(json!({"dsn": dsn})).unwrap();
        assert_eq!(parsed["user"], json!("u"));
        assert_eq!(
            parsed["password"],
            json!("p@ss/word"),
            "round-trips @ and / in the password"
        );
        assert_eq!(parsed["database"], json!("app"));
    }

    #[test]
    fn quote_ident_uses_backticks_not_double_quotes() {
        let v = op_quote_ident(json!({"name": "weird`col"})).unwrap();
        assert_eq!(
            v["quoted"],
            json!("`weird``col`"),
            "MySQL doubles backticks"
        );
    }

    #[test]
    fn unquote_ident_inverts_quote_ident() {
        // Doubled backtick decodes to one.
        assert_eq!(
            op_unquote_ident(json!({"quoted": "`weird``col`"})).unwrap()["name"],
            json!("weird`col")
        );
        // Plain and empty quoted names.
        assert_eq!(
            op_unquote_ident(json!({"quoted": "`plain`"})).unwrap()["name"],
            json!("plain")
        );
        assert_eq!(
            op_unquote_ident(json!({"quoted": "``"})).unwrap()["name"],
            json!("")
        );
        // Round-trips quote_ident for any input.
        for raw in ["table", "weird`col", "has space", "MixedCase"] {
            let q = op_quote_ident(json!({ "name": raw })).unwrap()["quoted"].clone();
            assert_eq!(
                op_unquote_ident(json!({ "quoted": q })).unwrap()["name"],
                json!(raw),
                "round-trip {raw:?}"
            );
        }
        // Not quoted / unpaired backtick reject.
        assert!(op_unquote_ident(json!({"quoted": "plain"})).is_err());
        assert!(op_unquote_ident(json!({"quoted": "`a`b`"})).is_err());
        assert!(op_unquote_ident(json!({})).is_err());
    }

    #[test]
    fn quote_qualified_ident_backticks_each_segment() {
        let v = op_quote_qualified_ident(json!({"name": "mydb.my table"})).unwrap();
        assert_eq!(v["quoted"], json!("`mydb`.`my table`"));
        assert_eq!(v["parts"], json!(["mydb", "my table"]));
        // Embedded backtick in a segment is doubled within that segment.
        assert_eq!(
            op_quote_qualified_ident(json!({"name": "db.we`ird"})).unwrap()["quoted"],
            json!("`db`.`we``ird`")
        );
        // Bare identifier (no dot) still gets backticked.
        assert_eq!(
            op_quote_qualified_ident(json!({"name": "users"})).unwrap()["quoted"],
            json!("`users`")
        );
        // Empty segments rejected.
        assert!(op_quote_qualified_ident(json!({"name": "mydb."})).is_err());
        assert!(op_quote_qualified_ident(json!({"name": ".tbl"})).is_err());
        assert!(op_quote_qualified_ident(json!({"name": "a..b"})).is_err());
    }

    #[test]
    fn parse_qualified_ident_inverts_quote_qualified_ident() {
        // Backtick-quoted segments: a `.` inside stays, doubled backtick un-doubles.
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "`mydb`.`my table`"})).unwrap()["parts"],
            json!(["mydb", "my table"])
        );
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "`a.b`.`c`"})).unwrap()["parts"],
            json!(["a.b", "c"]),
            "dot inside backticks is literal"
        );
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "`we``ird`"})).unwrap()["parts"],
            json!(["we`ird"]),
            "doubled backtick decodes to one"
        );
        // Bare (unquoted) segments pass through.
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "mydb.users"})).unwrap()["parts"],
            json!(["mydb", "users"])
        );
        // Round-trips quote_qualified_ident across tricky names.
        for name in ["mydb.my table", "db.we`ird", "users"] {
            let quoted = op_quote_qualified_ident(json!({ "name": name })).unwrap()["quoted"]
                .as_str()
                .unwrap()
                .to_string();
            let parts =
                op_parse_qualified_ident(json!({ "name": quoted })).unwrap()["parts"].clone();
            let original: Vec<&str> = name.split('.').collect();
            assert_eq!(parts, json!(original), "round-trip for {name}");
        }
        // Unquoted empty segments and an unterminated backtick are rejected.
        assert!(op_parse_qualified_ident(json!({"name": "a..b"})).is_err());
        assert!(op_parse_qualified_ident(json!({"name": ".x"})).is_err());
        assert!(op_parse_qualified_ident(json!({"name": "`unterminated"})).is_err());
    }

    #[test]
    fn quote_literal_backslash_escapes_default_mode() {
        // MySQL default mode: backslash is an escape char, so both `'` and `\`
        // must be backslash-escaped — distinct from Postgres's `''` doubling.
        assert_eq!(
            op_quote_literal(json!({"value": "O'Brien"})).unwrap()["quoted"],
            json!("'O\\'Brien'")
        );
        assert_eq!(
            op_quote_literal(json!({"value": "a\\b"})).unwrap()["quoted"],
            json!("'a\\\\b'")
        );
    }

    #[test]
    fn quote_matches_mysql_quote_builtin() {
        // Same backslash escaping as quote_literal for the common chars.
        assert_eq!(
            op_quote(json!({"value": "O'Brien"})).unwrap()["quoted"],
            json!("'O\\'Brien'")
        );
        // NUL and Control-Z get the \0 / \Z escapes that quote_literal omits.
        assert_eq!(
            op_quote(json!({"value": "a\u{0}b\u{1a}c"})).unwrap()["quoted"],
            json!("'a\\0b\\Zc'")
        );
        // NULL (json null or absent) → the unquoted word NULL.
        assert_eq!(
            op_quote(json!({ "value": Value::Null })).unwrap()["quoted"],
            json!("NULL")
        );
        assert_eq!(op_quote(json!({})).unwrap()["quoted"], json!("NULL"));
        // A non-string, non-null value is rejected.
        assert!(op_quote(json!({"value": 7})).is_err());
    }

    #[test]
    fn escape_like_backslash_prefixes_metacharacters() {
        // `%` and `_` each get a single backslash so LIKE matches them literally.
        assert_eq!(
            op_escape_like(json!({"value": "100%"})).unwrap()["escaped"],
            json!("100\\%")
        );
        assert_eq!(
            op_escape_like(json!({"value": "a_b"})).unwrap()["escaped"],
            json!("a\\_b")
        );
        // A literal backslash is doubled (escaped first), and both wildcards in one.
        assert_eq!(
            op_escape_like(json!({"value": "c\\d"})).unwrap()["escaped"],
            json!("c\\\\d")
        );
        assert_eq!(
            op_escape_like(json!({"value": "50%_off"})).unwrap()["escaped"],
            json!("50\\%\\_off")
        );
        // A string with no metacharacters is unchanged.
        assert_eq!(
            op_escape_like(json!({"value": "plain"})).unwrap()["escaped"],
            json!("plain")
        );
        assert!(op_escape_like(json!({})).is_err());
    }

    #[test]
    fn unescape_like_inverts_escape_like() {
        let un = |s: &str| {
            op_unescape_like(json!({ "value": s })).unwrap()["value"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // The three escape forms decode back.
        assert_eq!(un("100\\%"), "100%");
        assert_eq!(un("a\\_b"), "a_b");
        assert_eq!(un("c\\\\d"), "c\\d");
        // A `\\` adjacent to a `%` must not be mis-parsed (left-to-right scan):
        // `\\%` is a literal backslash followed by an unescaped wildcard.
        assert_eq!(un("\\\\%"), "\\%");
        // A backslash not introducing an escape stays literal.
        assert_eq!(un("a\\nb"), "a\\nb");
        // Round-trips escape_like for arbitrary input, including all metachars.
        for raw in ["100%", "a_b", "c\\d", "50%_off", "plain", "\\%_\\"] {
            let esc = escape_like_str(raw);
            assert_eq!(un(&esc), raw, "round-trip for {raw:?}");
        }
        // `escaped` is accepted as an alias for `value`.
        assert_eq!(
            op_unescape_like(json!({"escaped": "x\\%y"})).unwrap()["value"],
            json!("x%y")
        );
        assert!(op_unescape_like(json!({})).is_err());
    }

    #[test]
    fn like_pattern_anchors_an_escaped_substring_per_mode() {
        let pat = |v: &str, m: Option<&str>| {
            let mut o = json!({ "value": v });
            if let Some(m) = m {
                o["mode"] = json!(m);
            }
            op_like_pattern(o).unwrap()["pattern"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Default mode is `contains`.
        assert_eq!(pat("foo", None), "%foo%");
        assert_eq!(pat("foo", Some("contains")), "%foo%");
        assert_eq!(pat("foo", Some("starts_with")), "foo%");
        assert_eq!(pat("foo", Some("prefix")), "foo%");
        assert_eq!(pat("foo", Some("ends_with")), "%foo");
        assert_eq!(pat("foo", Some("suffix")), "%foo");
        assert_eq!(pat("foo", Some("equals")), "foo");
        assert_eq!(pat("foo", Some("exact")), "foo");
        // The substring's own LIKE metacharacters are escaped before anchoring,
        // so a literal `%`/`_` in the search term doesn't become a wildcard.
        assert_eq!(pat("50%_off", Some("contains")), "%50\\%\\_off%");
        assert_eq!(pat("c\\d", Some("equals")), "c\\\\d");
        // Unknown mode and missing value reject.
        assert!(op_like_pattern(json!({"value": "x", "mode": "nope"})).is_err());
        assert!(op_like_pattern(json!({})).is_err());
    }

    #[test]
    fn unquote_literal_inverts_quote_literal_and_decodes_escapes() {
        // Inverts quote_literal for the chars it escapes (`'` and `\`).
        for raw in ["O'Brien", "a\\b", "plain", "tab\tand\nnewline", ""] {
            let q = op_quote_literal(json!({ "value": raw })).unwrap()["quoted"].clone();
            assert_eq!(
                op_unquote_literal(json!({ "value": q })).unwrap()["value"],
                json!(raw),
                "round-trip for {raw:?}"
            );
        }
        // Full control-char escape set.
        assert_eq!(
            op_unquote_literal(json!({"value": "'\\0\\b\\n\\r\\t\\Z'"})).unwrap()["value"],
            json!("\u{0}\u{8}\n\r\t\u{1a}")
        );
        // LIKE metacharacters keep their backslash; double-quoted literal works.
        assert_eq!(
            op_unquote_literal(json!({"value": "'a\\%b\\_c'"})).unwrap()["value"],
            json!("a\\%b\\_c")
        );
        assert_eq!(
            op_unquote_literal(json!({"value": "\"say \\\"hi\\\"\""})).unwrap()["value"],
            json!("say \"hi\"")
        );
        // Doubled quote inside decodes to one.
        assert_eq!(
            op_unquote_literal(json!({"value": "'it''s'"})).unwrap()["value"],
            json!("it's")
        );
        // Unquoted / unterminated input rejects.
        assert!(op_unquote_literal(json!({"value": "no quotes"})).is_err());
        assert!(op_unquote_literal(json!({"value": "'unterminated"})).is_err());
    }

    #[test]
    fn format_in_list_quotes_each_element_and_handles_empty() {
        assert_eq!(
            op_format_in_list(json!({"elements": ["a", "b", "c"]})).unwrap()["list"],
            json!("('a','b','c')")
        );
        // Each element gets MySQL literal escaping (backslash for `'`).
        assert_eq!(
            op_format_in_list(json!({"elements": ["O'Brien", "x"]})).unwrap()["list"],
            json!("('O\\'Brien','x')")
        );
        // Empty list → (NULL): valid SQL that matches nothing.
        assert_eq!(
            op_format_in_list(json!({"elements": []})).unwrap()["list"],
            json!("(NULL)")
        );
        assert!(op_format_in_list(json!({})).is_err());
    }

    #[test]
    fn parse_in_list_inverts_format_in_list() {
        // Basic: three quoted strings back to a list.
        let v = op_parse_in_list(json!({"list": "('a','b','c')"})).unwrap();
        assert_eq!(v["count"], json!(3));
        assert_eq!(v["values"], json!(["a", "b", "c"]));
        // A comma inside a quoted element does not split.
        assert_eq!(
            op_parse_in_list(json!({"list": "('a,b','c')"})).unwrap()["values"],
            json!(["a,b", "c"])
        );
        // Escaped quote inside an element decodes; whitespace around commas trims.
        assert_eq!(
            op_parse_in_list(json!({"list": "('O\\'Brien', 'x')"})).unwrap()["values"],
            json!(["O'Brien", "x"])
        );
        // An unquoted NULL becomes a JSON null; bare tokens pass through.
        assert_eq!(
            op_parse_in_list(json!({"list": "('a',NULL,42)"})).unwrap()["values"],
            json!(["a", null, "42"])
        );
        // `(NULL)` (format_in_list's empty sentinel) parses to a single null.
        assert_eq!(
            op_parse_in_list(json!({"list": "(NULL)"})).unwrap()["values"],
            json!([null])
        );
        // `()` is an empty list.
        assert_eq!(
            op_parse_in_list(json!({"list": "()"})).unwrap()["count"],
            json!(0)
        );
        // Round-trips format_in_list for ordinary string sets.
        for set in [vec!["a", "b", "c"], vec!["a,b", "c'd", "x"], vec!["plain"]] {
            let list = op_format_in_list(json!({ "elements": set })).unwrap()["list"]
                .as_str()
                .unwrap()
                .to_string();
            let parsed = op_parse_in_list(json!({ "list": list })).unwrap();
            assert_eq!(parsed["values"], json!(set), "round-trip for {set:?}");
        }
        // Errors: not parenthesized, unterminated quote, missing.
        assert!(op_parse_in_list(json!({"list": "a,b,c"})).is_err());
        assert!(op_parse_in_list(json!({"list": "('unterminated)"})).is_err());
        assert!(op_parse_in_list(json!({})).is_err());
    }

    #[test]
    fn parse_enum_extracts_members_from_column_type() {
        // ENUM members.
        let e = op_parse_enum(json!({"type": "enum('small','medium','large')"})).unwrap();
        assert_eq!(e["kind"], json!("enum"));
        assert_eq!(e["values"], json!(["small", "medium", "large"]));
        assert_eq!(e["count"], json!(3));
        // SET members, and the keyword is case-insensitive.
        let s = op_parse_enum(json!({"type": "SET('a','b')"})).unwrap();
        assert_eq!(s["kind"], json!("set"));
        assert_eq!(s["values"], json!(["a", "b"]));
        // A doubled quote inside a member decodes to one (information_schema form).
        assert_eq!(
            op_parse_enum(json!({"type": "enum('it''s','x')"})).unwrap()["values"],
            json!(["it's", "x"])
        );
        // A comma inside a member does not split it.
        assert_eq!(
            op_parse_enum(json!({"type": "enum('a,b','c')"})).unwrap()["values"],
            json!(["a,b", "c"])
        );
        // `value` alias.
        assert_eq!(
            op_parse_enum(json!({"value": "enum('y')"})).unwrap()["values"],
            json!(["y"])
        );
        // Errors: not an enum/set, unterminated, missing.
        assert!(op_parse_enum(json!({"type": "varchar(20)"})).is_err());
        assert!(op_parse_enum(json!({"type": "enum('a'"})).is_err());
        assert!(op_parse_enum(json!({})).is_err());
    }

    #[test]
    fn build_enum_inverts_parse_enum() {
        // Default kind is ENUM; members are single-quoted.
        let e = op_build_enum(json!({ "values": ["small", "medium", "large"] })).unwrap();
        assert_eq!(e["type"], json!("ENUM('small','medium','large')"));
        assert_eq!(e["kind"], json!("enum"));
        assert_eq!(e["count"], json!(3));
        // SET keyword.
        assert_eq!(
            op_build_enum(json!({ "values": ["a", "b"], "kind": "set" })).unwrap()["type"],
            json!("SET('a','b')")
        );
        // Embedded quote/backslash are escaped and round-trip through parse_enum.
        for vals in [
            json!(["small", "medium", "large"]),
            json!(["it's", "x"]),
            json!(["a,b", "c"]),
            json!(["back\\slash"]),
        ] {
            let built = op_build_enum(json!({ "values": vals })).unwrap();
            let parsed = op_parse_enum(json!({ "type": built["type"] })).unwrap();
            assert_eq!(parsed["values"], vals, "round-trip {vals}");
        }
        // Errors: empty list, non-string member, bad kind, missing values.
        assert!(op_build_enum(json!({ "values": [] })).is_err());
        assert!(op_build_enum(json!({ "values": [1, 2] })).is_err());
        assert!(op_build_enum(json!({ "values": ["a"], "kind": "varchar" })).is_err());
        assert!(op_build_enum(json!({})).is_err());
    }

    #[test]
    fn enum_index_matches_mysql_internal_numbering() {
        let idx = |ty: &str, v: &str| {
            op_enum_index(json!({ "type": ty, "value": v })).unwrap()["index"].clone()
        };
        // Listed members are numbered from 1, in declaration order.
        assert_eq!(idx("enum('Mercury','Venus','Earth')", "Mercury"), json!(1));
        assert_eq!(idx("enum('Mercury','Venus','Earth')", "Venus"), json!(2));
        assert_eq!(idx("enum('Mercury','Venus','Earth')", "Earth"), json!(3));
        // Declaration order — not lexical — drives the index.
        assert_eq!(idx("enum('b','a')", "a"), json!(2));
        assert_eq!(idx("enum('b','a')", "b"), json!(1));
        // The empty-string error value is index 0.
        assert_eq!(idx("enum('a','b')", ""), json!(0));
        // Membership is ASCII case-insensitive (default collation).
        assert_eq!(idx("enum('Small','Large')", "small"), json!(1));
        // A value that isn't a member is null (not 0).
        assert_eq!(idx("enum('a','b')", "c"), Value::Null);
        // Works for SET types too.
        assert_eq!(idx("set('x','y','z')", "z"), json!(3));
        // Missing type/value and a non-enum type error.
        assert!(op_enum_index(json!({ "value": "a" })).is_err());
        assert!(op_enum_index(json!({ "type": "enum('a')" })).is_err());
        assert!(op_enum_index(json!({ "type": "varchar(20)", "value": "a" })).is_err());
    }

    #[test]
    fn percent_decode_is_tolerant_of_bad_escapes() {
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }
}
