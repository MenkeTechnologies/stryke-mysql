```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ m y s q l ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-mysql/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-mysql/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[MYSQL / MARIADB CLIENT FOR STRYKE // OPT-IN PACKAGE]`

> *"SQL without the connection ceremony."*

MySQL / MariaDB client for stryke. Opt-in package, kept out of the stryke
core binary so the daily-driver install stays slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-mongo`](https://github.com/MenkeTechnologies/stryke-mongo) · [`stryke-redis`](https://github.com/MenkeTechnologies/stryke-redis) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] CLI: `mysql`](#0x03-cli-mysql)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
- [\[0x06\] Type encoding](#0x06-type-encoding)
- [\[0x07\] Tests](#0x07-tests)
- [\[0x08\] Dev workflow](#0x08-dev-workflow)
- [\[0x09\] Layout](#0x09-layout)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

Same rationale as [stryke-arrow](../stryke-arrow): MySQL clients pull in a
big native dependency (`mysql` + TLS stack + Tokio for async variants). Most
stryke one-liners don't touch a database; for the ones that do, opt in with
this package.

`stryke-mysql` ships as a thin stryke library plus a Rust cdylib
(`libstryke_mysql.{dylib,so}`) built from this repo and dlopened
in-process on first `use MySQL` — no fork-per-call, no pipe parsing.

## [0x01] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-mysql
```

From a local checkout:

```sh
cd ~/projects/stryke-mysql
cargo build --release        # produces target/release/libstryke_mysql.{dylib,so}
s pkg install -g .           # cdylib lands in ~/.stryke/store/mysql@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use MySQL`. A `mysql::Pool`
cache keyed by connection URL is held in `OnceCell` — no fork-per-call,
no fresh TCP+auth handshake.

## [0x02] Quick start

```stryke
use MySQL

# Set $MYSQL_URL once, omit the named arg everywhere.
$ENV{MYSQL_URL} = "mysql://root:secret@127.0.0.1:3306/test"

# Single-row scalar.
p MySQL::query_scalar "SELECT COUNT(*) FROM users"

# Rows with parameter binding (positional `?`).
my @rows = MySQL::query "SELECT id, name FROM users WHERE created_at > ?",
                        bind => ["2025-01-01"]
@rows |> ep

# Callback-per-row variant (cdylib returns all rows in one call).
MySQL::query_stream "SELECT * FROM big_table",
    callback => sub ($row) { process $row }

# Write paths return { affected, last_insert_id }.
my $r = MySQL::execute "UPDATE users SET name = ? WHERE id = ?",
                       bind => ["alice", 42]
p "updated $r->{affected}"

# Bulk insert (array of hashes; columns inferred from first row's keys).
MySQL::insert_many "users",
    [{ name => "x", score => 1 },
     { name => "y", score => 2 }]

# Schema introspection.
p to_json MySQL::schema "users"
p MySQL::tables |> ep
```

Connection URL sources (priority order):

1. `url => "mysql://user:pass@host:port/db"` named arg
2. Individual named args: `host`, `port`, `user`, `password`, `database`
3. `$ENV{MYSQL_URL}`

## [0x03] CLI: `mysql`

```sh
mysql query   "SELECT * FROM users WHERE id = ?" --bind='[42]'
mysql execute "UPDATE users SET active = 1 WHERE id = ?" --bind='[42]'
mysql exec   --file=migrate.sql
mysql dump   --table=users --where='active = 1' --order-by=id --limit=100
mysql tables
mysql databases
mysql schema --table=users
mysql ping
mysql build                                # `cargo build --release`
mysql version
```

Connection flags (also accept env vars):

```
--dsn URL          $MYSQL_DSN
--host H           $MYSQL_HOST
--port P           $MYSQL_PORT
--user U           $MYSQL_USER
--password PW      $MYSQL_PASSWORD
--database D       $MYSQL_DATABASE
--socket PATH      (Unix socket)
--ssl              enable TLS
--ssl-ca PATH      CA bundle (implies --ssl)
--connect-timeout SECONDS
```

## [0x04] API reference

### Read paths

```stryke
MySQL::query        $sql, %opts → @rows
MySQL::query_stream $sql, %opts → $count       # callback per row
MySQL::query_one    $sql, %opts → \%row | undef
MySQL::query_col    $sql, %opts → @values      # first column, all rows
MySQL::query_scalar $sql, %opts → $value | undef
MySQL::query_multi  $sql, %opts → @result_sets # each { columns, rows }
MySQL::call         $proc, %opts → @result_sets # CALL proc(args); opts: args => [...]
MySQL::dump         $table, %opts → @rows      # opts: limit
```

`call` runs a stored procedure and returns every result set it emits;
`query_multi` does the same for a multi-statement SQL string.

`%opts` keys: `url`, `host`, `port`, `user`, `password`, `database`,
`bind`, `limit` (dump only), `callback` (stream only). `bind` is an
arrayref bound to positional `?` placeholders.

### Write paths

```stryke
MySQL::execute     $sql, %opts → { affected, last_insert_id }
MySQL::exec_file   $path, %opts → { ok }       # multi-statement script
MySQL::insert_many $table, $rows_aref, %opts → $inserted_count
MySQL::upsert      $table, $row_href, %opts → $affected   # INSERT … ON DUPLICATE KEY UPDATE
MySQL::update      $table, $set_href, $where?, %opts → $affected   # UPDATE … SET … [WHERE]
MySQL::delete      $table, $where?, %opts → $affected               # DELETE FROM … [WHERE]
MySQL::truncate    $table, %opts → 1           # TRUNCATE TABLE
```

`update` and `delete` complete the CRUD surface. `update` binds the `$set`
values (`SET col = ?, …`) and interpolates `$where`; `delete` interpolates
`$where`. Both omit `$where` to affect every row and return the
affected-row count. Table and SET column names are identifier-validated;
pass trusted values in `$where` (use `execute` for a parameterized one).

```stryke
MySQL::update "users", { status => "active", seen => 1 }, "id = 42"
MySQL::delete "sessions", "expired_at < now()"
```

`upsert` inserts a single row and, on a duplicate unique/PK key, updates
the `update` columns from the proposed row (MySQL `VALUES(col)`). MySQL's
`ON DUPLICATE KEY` differs from Postgres/DuckDB: it fires on **any**
unique/PK collision (no per-target conflict clause) and there is **no
RETURNING** (passing `returning` dies). Options: `update => \@cols`
(defaults to every row column not named in the advisory `conflict =>
\@cols`; an empty list is a no-op self-assignment, i.e. insert-or-ignore).
Returns the affected-row count — MySQL reports 1 for an insert, 2 for an
update, 0 when a duplicate left the row unchanged.

```stryke
MySQL::upsert "kv", { id => 1, name => "a", hits => 1 }              # insert or update all
MySQL::upsert "kv", { id => 1, name => "x", hits => 9 },
             conflict => ["id"], update => ["hits"]                  # only bump hits
```

### Transactions

```stryke
MySQL::transaction \@statements, %opts → @results   # each { affected, last_insert_id }
```

The cdylib caches a `mysql::Pool` per URL, so back-to-back FFI calls may run
on **different pooled connections** — a `START TRANSACTION` and a later
`COMMIT`/`ROLLBACK` issued as separate calls would not share a session.
`MySQL::transaction` runs the whole batch on **one** checked-out connection
inside a real `START TRANSACTION`: every statement commits together, and any
error rolls the batch back (the connection's `Transaction` rolls back on
drop). Each entry is `{ sql => "...", params => [...] }`.

```stryke
MySQL::transaction [
    { sql => "UPDATE accounts SET bal = bal - 100 WHERE id = ?", params => [1] },
    { sql => "UPDATE accounts SET bal = bal + 100 WHERE id = ?", params => [2] },
], url => $dsn
```

### Metadata

```stryke
MySQL::ping         %opts → 1 | ""
MySQL::tables       %opts → @names
MySQL::databases    %opts → @names
MySQL::schema       $table, %opts → { table, columns => [...] }
MySQL::count        $table, $where?, %opts → $row_count   # SELECT count(*) [WHERE $where]
MySQL::exists       $table, $where?, %opts → 1 | 0        # SELECT EXISTS(…) — short-circuits
MySQL::table_exists $name, %opts → 1 | 0                  # $name must be a plain identifier
MySQL::views        %opts → @names                       # view names in current db
MySQL::procedures   %opts → @{ {ROUTINE_NAME, ROUTINE_TYPE} }
MySQL::indexes      $table, %opts → @rows                 # SHOW INDEX FROM $table
MySQL::triggers     %opts → @rows                         # SHOW TRIGGERS
MySQL::users        %opts → @{ {user, host} }            # from mysql.user (needs privilege)
MySQL::explain      $sql, %opts → @plan_rows              # opt: params
MySQL::db_size      %opts → $bytes                        # data + index length of current db
MySQL::table_size   $table, %opts → { table, data_bytes, index_bytes, bytes }
MySQL::processlist  %opts → @rows                         # SHOW FULL PROCESSLIST
MySQL::status       %opts → @{ {Variable_name, Value} }  # SHOW GLOBAL STATUS; opt global => 0
MySQL::variables    %opts → @{ {Variable_name, Value} }  # SHOW GLOBAL VARIABLES; opt global => 0
MySQL::engines      %opts → @rows                         # SHOW ENGINES
MySQL::kill         $id, %opts → { id, killed }           # KILL a thread
MySQL::current_database %opts → $database                 # current db name (SELECT DATABASE())
MySQL::column_names  $table, %opts → @names               # ordinal column names of $table
MySQL::create_database $name, %opts → { database, created }  # CREATE DATABASE [IF NOT EXISTS]; opt if_not_exists (default true)
MySQL::drop_database $name, %opts → { database, dropped }    # DROP DATABASE [IF EXISTS]; opt if_exists (default true)
```

`exists` uses SQL `EXISTS`, which stops at the first matching row — prefer
it over `count(…) > 0` when you only need a yes/no. The table name and
`$where` are interpolated; pass trusted/validated values.

### Plumbing

```stryke
MySQL::version()           → $version_string   # cdylib's CARGO_PKG_VERSION
MySQL::server_version(%opts) → $server_version # live `SELECT VERSION()`
```

### Pure helpers (no connection)

```stryke
MySQL::parse_dsn($dsn)      → { scheme, user, password, host, port, database, params }
MySQL::build_dsn(%opts)     → $dsn        # parts → URI DSN; inverse of parse_dsn
MySQL::quote_ident($name)   → $quoted     # `weird``col` (backticks, MySQL style)
MySQL::unquote_ident($quoted) → $name     # inverse of quote_ident: strip backticks, un-double
MySQL::quote_qualified_ident($name) → $quoted  # mydb.my table → `mydb`.`my table`
MySQL::parse_qualified_ident($name) → \@parts  # `mydb`.`my table` → ["mydb","my table"]; inverse of quote_qualified_ident
MySQL::quote_literal($val)  → $quoted     # 'O\'Brien' (backslash-escapes, default mode)
MySQL::quote($val)          → $quoted     # MySQL QUOTE(): escapes \ ' NUL Ctrl-Z; undef → NULL (unquoted)
MySQL::escape_like($val)    → $escaped    # backslash-escapes LIKE metachars % _ \ (wrap with quote_literal to inline)
MySQL::unescape_like($escaped) → $val     # inverse of escape_like: \\ → \, \% → %, \_ → _ (single left-to-right scan)
MySQL::like_pattern($val, $mode?) → $pattern  # build a LIKE pattern: contains→%v%, starts_with→v%, ends_with→%v, equals→v (term escaped)
MySQL::unquote_literal($lit) → $val       # 'O\'Brien' → O'Brien; decodes \0\b\n\r\t\Z, keeps \%\_; inverse of quote_literal
MySQL::format_in_list(\@elems) → $list    # ["a","b"] → ('a','b'); empty → (NULL)
MySQL::parse_in_list($list) → { values, count }  # inverse: ('a','b',NULL) → ["a","b",undef]; splits at top-level commas
MySQL::parse_enum($type) → { type, kind, values, count }  # enum('a','b')/set(...) COLUMN_TYPE → member list (kind enum|set)
MySQL::build_enum(%opts) → { type, kind, values, count }  # inverse: {values=>[...], kind=>enum|set} → ENUM('a','b') type decl (round-trips parse_enum)
MySQL::enum_index($type, $value) → { value, index }       # MySQL's internal 1-based ENUM index (ORDER BY key); '' → 0, non-member → undef; ASCII case-insensitive
MySQL::enum_value($type, $index) → { index, value }       # inverse of enum_index: 1-based index → member; 0 → '', out-of-range → undef (the stored-int lookup)
MySQL::set_mask($type, $value)   → { value, mask, members }  # bitmask MySQL stores for a SET value (member N = 2^(N-1)); comma-separated subset, case-insensitive, empty → 0
MySQL::set_from_mask($type, $mask) → { mask, value, members }  # inverse of set_mask: decode a stored SET bitmask back to its members (definition order); a bit beyond the members errors
MySQL::parse_column_type($type) → { type, base, args, attributes, unsigned, zerofill }  # parse a COLUMN_TYPE decl (varchar(64)/decimal(10,2)/int unsigned); enum/set bodies kept whole
MySQL::normalize_type($type) → $normalized  # canonicalize type for comparison: drops integer display width (INT(11)→int), keeps decimal/char length + unsigned/zerofill
MySQL::format_assignments(\@cols) → { clause, columns, count }  # comma-joined "col = ?" SET-clause (each ident-validated + backtick-quoted)
MySQL::format_placeholders($cols, $rows?) → { placeholders, cols, rows, count }  # multi-row VALUES grid e.g. cols=3 rows=2 → "(?, ?, ?), (?, ?, ?)"; $rows defaults to 1
MySQL::escape_string($val) → $escaped  # mysql_real_escape_string: backslash-escapes NUL \n \r \\ ' " Ctrl-Z, no surrounding quotes
MySQL::redact_dsn($dsn) → $redacted  # mask password in a URI DSN for logging (mysql://u:s3cret@h/db → mysql://u:***@h/db); no-password DSN unchanged
MySQL::parse_set_value($val) → { members, count }  # split a stored SET value ("a,b,c") into members; empty/undef → empty list
MySQL::build_where_eq(\%eq) → { clause, params, columns, count }  # "col = ? AND col2 = ?" bound-equality WHERE from hashref keys (sorted, ident-validated); empty → "1=1"
```

## [0x05] FFI layer

Each `MySQL::*` wrapper builds a JSON args dict and calls a sibling
`mysql__*` symbol resolved out of `libstryke_mysql.{dylib,so}`. The
cdylib is dlopened in-process on first `use MySQL` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook). Its exports cover the
query/introspection surface (`mysql__pkg_version`, `mysql__version`,
`mysql__ping`, `mysql__databases`, `mysql__tables`, `mysql__schema`,
`mysql__query`, `mysql__execute`, `mysql__insert_many`, `mysql__dump`, …)
plus connection-free helpers (`mysql__parse_dsn`, `mysql__build_dsn`,
`mysql__quote_ident`, `mysql__quote_qualified_ident`,
`mysql__quote_literal`, `mysql__quote`, `mysql__format_in_list`). The authoritative list is
`[ffi].exports` in `stryke.toml`.

A `mysql::Pool` cache keyed by connection URL is held in `OnceCell`,
so back-to-back calls reuse the same connection pool.

Errors come back as a `{error}` JSON payload; the stryke wrapper dies
with `MySQL::<op>: <reason>`.

<details>
<summary>v1 wire shape (historical helper binary)</summary>

```sh
stryke-mysql-helper --dsn 'mysql://…' query 'SELECT * FROM t WHERE id = ?' --bind '[42]'
stryke-mysql-helper --dsn 'mysql://…' execute 'UPDATE …' --bind '["x", 1]'
stryke-mysql-helper --dsn 'mysql://…' exec --file migrate.sql
stryke-mysql-helper --dsn 'mysql://…' schema --table users
stryke-mysql-helper --dsn 'mysql://…' ping
```

Output:

* `query` → NDJSON rows on stdout. `--columnar` emits one `{columns, rows}`
  object. `--with-meta` prepends a `{"meta":{columns:[...]}}` line.
* `execute` → `{affected_rows, last_insert_id, warnings, info}`
* `exec` → array of per-statement objects
* `schema` → `{table, columns:[...], indexes:[...]}`
* `tables`, `databases` → NDJSON `{"name": ...}`
* `ping` → `ok` on stdout, exit 0; non-zero on failure

The helper also supported an experimental long-running JSON-RPC daemon
on a Unix socket (`serve --socket-path …`). The in-process cdylib +
pool cache replaced both modes.

</details>

## [0x06] Type encoding

MySQL → JSON encoding:

| MySQL | JSON | Notes |
|---|---|---|
| `INT`, `BIGINT` | number | |
| `FLOAT`, `DOUBLE` | number | |
| `DECIMAL` | string | sent as bytes by the protocol, decoded as UTF-8 |
| `VARCHAR`, `TEXT`, `CHAR` | string | |
| `BLOB`, `VARBINARY` | string | UTF-8 if valid; otherwise `"base64:…"` |
| `DATE` | `"YYYY-MM-DD"` | |
| `DATETIME`, `TIMESTAMP` | `"YYYY-MM-DD HH:MM:SS.ffffff"` | |
| `TIME` | `"[-]HHH:MM:SS.ffffff"` | |
| `NULL` | null | |
| `JSON` | string | raw JSON text — `from_json` it stryke-side if you want |

`BLOB` columns that aren't valid UTF-8 come back with a `"base64:"` prefix so
consumers can detect and decode them.

## [0x07] Tests

```sh
cargo test                                  # unit + contract tests, no live calls
MYSQL_URL='mysql://…' s test t/             # end-to-end against live MySQL
```

The end-to-end suite skips cleanly when `$MYSQL_URL` (or legacy
`$MYSQL_DSN`) is unset or the server isn't reachable.

## [0x08] Dev workflow

```sh
make             # release build
make debug       # faster compile
make test        # cargo test + s test t/
make install     # release + pkg install -g .
make clean
```

## [0x09] Layout

```
stryke-mysql/
  stryke.toml                  # stryke package manifest
  Cargo.toml                   # Rust helper crate manifest
  Makefile                     # convenience targets
  src/lib.rs                   # single-file cdylib
  lib/
    MySQL.stk                  # `use MySQL`
  t/
    test_mysql.stk             # end-to-end (gated on $MYSQL_URL)
    test_stryke_mysql_surface.stk
  examples/
    quick_query.stk
    bulk_load.stk
    crud.stk
    dump_table.stk
    discover.stk
    explain.stk
  .github/workflows/
    ci.yml                     # cargo check/test on PRs
    release.yml                # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
