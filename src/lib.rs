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

use anyhow::{anyhow, Result};
use mysql::prelude::*;
use mysql::{Params, Pool, Row, Value as MyValue};
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
                MyValue::Float(f as f32)
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
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
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

fn op_exec(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let sql = opts["sql"].as_str().ok_or_else(|| anyhow!("missing sql"))?;
    // Split on `;` at the end of lines — naive but sufficient for the
    // multi-statement use case the v1 helper supported. mysql crate
    // doesn't expose a multi-statement exec without `multi-statements`
    // mode set on the URL.
    for stmt in sql.split(';').filter(|s| !s.trim().is_empty()) {
        conn.query_drop(stmt)?;
    }
    Ok(json!({"ok": true}))
}

fn op_insert_many(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let rows = opts["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (array of objects)"))?;
    if rows.is_empty() {
        return Ok(json!({"inserted": 0}));
    }
    let first = rows[0]
        .as_object()
        .ok_or_else(|| anyhow!("first row must be an object"))?;
    let cols: Vec<&str> = first.keys().map(|s| s.as_str()).collect();
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
        let vals: Vec<MyValue> = cols.iter().map(|c| json_to_my_value(&obj[*c])).collect();
        conn.exec_drop(&stmt, Params::Positional(vals))?;
        total += conn.affected_rows() as i64;
    }
    Ok(json!({"inserted": total}))
}

fn op_dump(opts: Value) -> Result<Value> {
    let p = get_pool(&opts)?;
    let mut conn = p.get_conn()?;
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
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
