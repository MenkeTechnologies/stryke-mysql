# stryke-mysql

MySQL / MariaDB client for stryke. Opt-in package, kept out of the stryke
core binary so the daily-driver install stays slim.

Created by MenkeTechnologies.

## Why this is a package, not a builtin

Same rationale as [stryke-arrow](../stryke-arrow): MySQL clients pull in a
big native dependency (`mysql` + TLS stack + Tokio for async variants). Most
stryke one-liners don't touch a database; for the ones that do, opt in with
this package.

`stryke-mysql` ships as a thin stryke library plus a Rust helper binary
(`stryke-mysql-helper`) built from this repo. The stryke side spawns the
helper per call and parses NDJSON over a pipe.

## Install

```sh
cd ~/projects/stryke-mysql
cargo build --release        # produces target/release/stryke-mysql-helper
s pkg install -g .           # installs `mysql` and `mysql-build` CLIs
```

Or:

```sh
make install
```

## Quick start

```stryke
use MySQL

# Set $MYSQL_DSN once, omit the named arg everywhere.
$ENV{MYSQL_DSN} = "mysql://root:secret@127.0.0.1:3306/test"

# Single-row scalar.
p MySQL::query_scalar "SELECT COUNT(*) FROM users"

# Rows with parameter binding (positional `?`).
my @rows = MySQL::query "SELECT id, name FROM users WHERE created_at > ?",
                        bind => ["2025-01-01"]
@rows |> ep

# Streaming variant — no full-result buffering.
MySQL::query_stream "SELECT * FROM big_table",
    callback => sub ($row) { process $row }

# Write paths return { affected_rows, last_insert_id, warnings, info }.
my $r = MySQL::execute "UPDATE users SET name = ? WHERE id = ?",
                       bind => ["alice", 42]
p "updated $r->{affected_rows}"

# Bulk insert (array of hashes; columns inferred from first row's keys).
MySQL::insert_many "users",
    [{ name => "x", score => 1 },
     { name => "y", score => 2 }]

# Schema introspection.
p to_json MySQL::schema "users"
p MySQL::tables |> ep
```

DSN sources (priority order):

1. `dsn => "mysql://user:pass@host:port/db"` named arg
2. `$ENV{MYSQL_DSN}`
3. Individual flags: `host`, `port`, `user`, `password`, `database`, `socket`

## CLI: `mysql`

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

## API reference

### Read paths

```stryke
MySQL::query        $sql, %opts → @rows | \@rows
MySQL::query_stream $sql, %opts → $count       # callback per row
MySQL::query_one    $sql, %opts → \%row | undef
MySQL::query_col    $sql, %opts → @values      # first column, all rows
MySQL::query_scalar $sql, %opts → $value | undef
MySQL::dump         $table, %opts → @rows
```

`%opts` keys: `dsn`, `host`, `port`, `user`, `password`, `database`, `socket`,
`ssl`, `ssl_ca`, `connect_timeout`, `bind`, `columnar`, `with_meta`, `limit`,
`callback` (stream only). `bind` is an arrayref (positional `?`) or hashref
(named `:name`).

### Write paths

```stryke
MySQL::execute     $sql, %opts → { affected_rows, last_insert_id, warnings, info }
MySQL::exec_file   $path, %opts → [{ sql, affected_rows, ... }, ...]
MySQL::insert_many $table, $rows_aref, %opts → { affected_rows, ... }
```

### Metadata

```stryke
MySQL::ping       %opts → 1 | ""
MySQL::tables     %opts → @names
MySQL::databases %opts → @names
MySQL::schema     $table, %opts → { table, columns => [...], indexes => [...] }
```

### Helper plumbing

```stryke
MySQL::helper_path()    → $abs_path
MySQL::ensure_built()   → $abs_path     # cargo-builds if missing
MySQL::version()        → "stryke-mysql-helper 0.1.0"
```

## Helper protocol

The Rust helper speaks JSON over stdin/stdout/argv — useful directly:

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

### Persistent serve mode (experimental)

The helper also supports a long-running JSON-RPC daemon on a Unix socket:

```sh
stryke-mysql-helper --dsn 'mysql://…' serve --socket-path /tmp/sm.sock &
```

Wire format: one JSON request per line over the socket
(`{"id":N,"method":"query|execute|tables|databases|schema|ping|close","params":{...}}`),
one response per line. The connection is reused across requests.

The stryke side's persistent-connect API will pick this up once stryke gains a
Unix-socket client builtin. For now the lib is single-shot; the daemon is
useful directly from any language with a Unix-socket client.

## Type encoding

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

## Tests

```sh
cargo test                                  # unit tests (none yet — scaffold)
MYSQL_DSN='mysql://…' s test t/             # end-to-end against live MySQL
```

The end-to-end suite skips cleanly when `$MYSQL_DSN` is unset or the helper
isn't built.

## Dev workflow

```sh
make             # release build
make debug       # faster compile
make test        # cargo test + s test t/
make install     # release + pkg install -g .
make clean
```

## Layout

```
stryke-mysql/
  stryke.toml                  # stryke package manifest
  Cargo.toml                   # Rust helper crate manifest
  Makefile                     # convenience targets
  src/main.rs                  # stryke-mysql-helper binary
  lib/
    MySQL.stk                  # `use MySQL`
  bin/
    mysql.stk                  # `mysql` CLI
    mysql-build.stk            # `mysql-build` CLI (cargo build wrapper)
  t/
    test_mysql.stk             # end-to-end (gated on $MYSQL_DSN)
  examples/
    quick_query.stk
    bulk_load.stk
    dump_table.stk
  .github/workflows/
    ci.yml                     # cargo check/test on PRs
    release.yml                # cross-compile + GH release on tag push
```

## License

MIT.
