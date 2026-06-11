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
MySQL::dump         $table, %opts → @rows      # opts: limit
```

`%opts` keys: `url`, `host`, `port`, `user`, `password`, `database`,
`bind`, `limit` (dump only), `callback` (stream only). `bind` is an
arrayref bound to positional `?` placeholders.

### Write paths

```stryke
MySQL::execute     $sql, %opts → { affected, last_insert_id }
MySQL::exec_file   $path, %opts → { ok }       # multi-statement script
MySQL::insert_many $table, $rows_aref, %opts → $inserted_count
```

### Metadata

```stryke
MySQL::ping       %opts → 1 | ""
MySQL::tables     %opts → @names
MySQL::databases %opts → @names
MySQL::schema     $table, %opts → { table, columns => [...] }
```

### Plumbing

```stryke
MySQL::version()           → $version_string   # cdylib's CARGO_PKG_VERSION
MySQL::server_version(%opts) → $server_version # live `SELECT VERSION()`
```

## [0x05] FFI layer

Each `MySQL::*` wrapper builds a JSON args dict and calls a sibling
`mysql__*` symbol resolved out of `libstryke_mysql.{dylib,so}`. The
cdylib is dlopened in-process on first `use MySQL` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook) and exposes 11 entry
points: `mysql__pkg_version`, `mysql__version`, `mysql__ping`,
`mysql__databases`, `mysql__tables`, `mysql__schema`, `mysql__query`,
`mysql__execute`, `mysql__exec`, `mysql__insert_many`, `mysql__dump`.

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
    dump_table.stk
    discover.stk
    explain.stk
  .github/workflows/
    ci.yml                     # cargo check/test on PRs
    release.yml                # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
