# CLAUDE.md - AI Assistant Guide for dbsqlx

## Project Overview

**dbsqlx** is a Go CLI tool that parses SQL statements and extracts structural information (tables, columns, actions, WHERE filters). It generates `mysqldump` commands for backup before destructive operations. Built on TiDB Parser for robust MySQL-compatible SQL parsing, with Cobra for CLI management.

## Quick Reference

```bash
# Build
go build -o dbsqlx main.go

# Run all tests
go test -v ./...

# Run tests for a specific package
go test -v ./cmd/...

# Build for CI (output to target/)
go build -o target/dbsqlx main.go
```

## Repository Structure

```
dbsqlx/
├── main.go                  # Entry point - calls cmd.Execute()
├── go.mod                   # Go 1.25, module name: dbsqlx
├── cmd/
│   ├── root.go              # Root command, SQL parsing core (ColX visitor, Extract, ParseAll)
│   ├── root_test.go         # Unit tests for parsing logic
│   ├── dump.go              # "dump" subcommand - generates mysqldump commands
│   ├── dump_test.go         # Dump command tests
│   ├── check.go             # "check" subcommand - SQL syntax validation
│   └── check_test.go        # Check command tests
├── dbsqlx_test.go           # Integration tests (CLI output capture)
├── .github/workflows/go.yml # CI: build, test, artifact upload, release
├── README.md                # User-facing documentation
└── .gitignore               # Ignores: coverage.*, dbsqlx binary, test.sql, .vscode, target/
```

## Architecture

### CLI Commands (Cobra)

- **root** (`dbsqlx [sql]`): Parse SQL and display extracted tables, columns, action, WHERE filter
- **dump** (`dbsqlx dump [sql]`): Generate `mysqldump` commands from DML statements
- **check** (`dbsqlx check [sql]`): Validate SQL syntax only

### Global Flags (defined in `cmd/root.go` init())

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--file` | `-f` | | Read SQL from file |
| `--user` | `-u` | | Database user |
| `--password` | `-p` | | Database password |
| `--host` | `-h` | | Database host |
| `--ip` | | | Database IP (overrides host) |
| `--port` | `-P` | `3306` | Database port |
| `--database` | `-d` | `database_name` | Database name |

Note: `-h` is used for `--host`, not help. Help is available only via `--help`.

### Core Types and Functions (`cmd/root.go`)

- **`ColX` struct**: AST visitor implementing `ast.Visitor` interface (`Enter`/`Leave`). Collects column names, table names, primary table, action type, WHERE filter, and alias mappings.
- **`Extract(rootNode *ast.StmtNode)`**: Main extraction function - returns `colNames, tableNames, action, whereFilter, primaryTable`.
- **`ParseAll(sql string)`**: Parses SQL string into `[]ast.StmtNode` using TiDB parser.
- **`CheckSQLSyntax(sql string)`**: Validates SQL syntax, returns error if invalid.
- **`FilterWhereForTable(whereFilter, tableName string, allTables []string)`**: Extracts WHERE conditions relevant to a specific table from multi-table queries.
- **`ResetGlobals()`**: Resets all global flag variables and Cobra flag state (used in tests).

### Supported SQL Actions

| Action | Parse | Dump | Notes |
|--------|-------|------|-------|
| INSERT | Yes | Yes | Dumps target table with WHERE filter |
| UPDATE | Yes | Yes | Dumps primary table only (first table in FROM) |
| DELETE | Yes | Yes | Dumps primary table only |
| SELECT | Yes | Skipped | Queries don't modify data |
| ALTER | Yes | Skipped | DDL, no data changes |
| CREATE | Yes | Skipped | DDL, no data changes |
| DROP | Yes | Yes | Dumps entire table (no WHERE filter) |
| TRUNCATE | Yes | Yes | Dumps entire table (no WHERE filter) |

### Dump Command Behavior (`cmd/dump.go`)

- Deduplicates dump commands using `map[string]bool` keyed by `table|filter|database`
- For UPDATE/DELETE with JOINs: only dumps the primary table, provides helper comments for exact-row matching
- Port `3306` is omitted from output (default)
- IP flag (`--ip`) overrides host flag (`--host`)

## Key Conventions

### Code Patterns

1. **Visitor pattern** for AST traversal - `ColX` implements `Enter`/`Leave` on `ast.Node`
2. **Global variables** for CLI flags with `ResetGlobals()` for test isolation
3. **Cobra command registration** via `init()` functions calling `rootCmd.AddCommand()`
4. **WHERE filter processing**: Removes backticks, replaces `_UTF8MB4'...'` encoding prefixes, resolves aliases to real table names, normalizes `AND` to lowercase `and`
5. **No external test framework** - uses Go's built-in `testing` package only

### Testing Conventions

- **Stdout capture**: Tests capture CLI output using `os.Pipe()` to verify printed results
- **Table-driven tests**: Test cases defined as struct slices with subtests via `t.Run()`
- **Flag reset**: Every test must call `cmd.ResetGlobals()` before execution to avoid state leakage
- **Test file naming**: `*_test.go` alongside the file they test
- **Integration tests** in `dbsqlx_test.go` test the full CLI pipeline

### Dependencies

| Dependency | Purpose |
|------------|---------|
| `github.com/pingcap/tidb/pkg/parser` | SQL parsing engine (TiDB Parser) |
| `github.com/spf13/cobra` | CLI framework |

All other dependencies are indirect (pulled in by the above two).

## CI/CD

GitHub Actions workflow (`.github/workflows/go.yml`):
- **Triggers**: Push to `main`, version tags (`v*.*.*`), PRs to `main`
- **Steps**: Checkout, setup Go 1.25, build to `target/dbsqlx`, run `go test -v ./...`, upload artifact, create GitHub release on tag push

## Development Guidelines

- Keep all command implementations in the `cmd/` package
- Add new subcommands by creating a new file in `cmd/` with an `init()` function that calls `rootCmd.AddCommand()`
- SQL input always flows through `getSQLInput()` which handles both inline args and `-f` file input
- When adding new SQL action types, update the `Enter()` method on `ColX` and add corresponding dump logic if needed
- Always add tests using table-driven patterns and remember to call `ResetGlobals()` in test setup
- The `-h` flag is `--host`, not help - this is intentional for MySQL CLI compatibility
