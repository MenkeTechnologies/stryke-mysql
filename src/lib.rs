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
            MyValue::Float(f) if (f - 1.5).abs() < 1e-6 => {}
            other => panic!("expected Float(1.5), got {other:?}"),
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

    // Silent precision loss class. JSON number `0.1` is the canonical
    // float not exactly representable in either f32 or f64, but the f64
    // approximation (0.1000000000000000055...) is closer to true 0.1
    // than the f32 approximation (0.100000001490116...). The cast at
    // lib.rs:85 (`f as f32`) throws away ~32 bits of mantissa for *every*
    // non-integral JSON number that flows through bind params — including
    // every DECIMAL, every monetary value, every coordinate. Pin the
    // current lossy behavior so a future fix that switches to
    // `MyValue::Double(f)` (the obvious 1-line correction) shows up as a
    // deliberate behavior change. The assertion compares against the
    // f32-rounded value, not the original f64, which is the *whole point*.
    #[test]
    fn j2mv_f64_zero_point_one_loses_precision_via_f32_cast() {
        let original_f64: f64 = 0.1;
        let lossy_f32: f32 = original_f64 as f32;
        // Sanity: the cast actually does lose information.
        assert_ne!(
            lossy_f32 as f64, original_f64,
            "test premise broken: f64 0.1 must differ from (f32)0.1 widened back",
        );
        match json_to_my_value(&json!(0.1_f64)) {
            MyValue::Float(f) => {
                assert_eq!(
                    f.to_bits(),
                    lossy_f32.to_bits(),
                    "expected the truncated f32 bit pattern; if this flips, lib.rs:85 \
                     stopped doing `f as f32` — confirm intentional",
                );
                assert_ne!(
                    f as f64, original_f64,
                    "if the round-trip is now lossless, lib.rs:85 must have switched \
                     to MyValue::Double — update this pin",
                );
            }
            other => panic!(
                "JSON 0.1 must go through the Float (f32) arm, got {other:?} — \
                 lib.rs:84-85 fallback ordering or variant choice changed",
            ),
        }
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
}
