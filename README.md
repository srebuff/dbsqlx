# dbsqlx 

A powerful SQL parser and analyzer tool built base [TiDB Parser](https://github.com/pingcap/tidb).

## Features

- ✅ Parse SQL statements and extract tables, columns, and actions
- ✅ Validate SQL syntax
- ✅ Generate mysqldump commands with intelligent WHERE filtering
- ✅ Support for multi-table JOINs with per-table condition filtering
- ✅ Professional CLI with subcommands


## Installation

```bash
go build -o dbsqlx main.go
```

## Usage

### Basic Parsing

Extract tables, columns, and SQL structure:

```bash
# Parse SQL statement
dbsqlx "SELECT * FROM users WHERE id = 1"

# Parse from file
dbsqlx --file query.sql
dbsqlx -f query.sql
```

### Check Syntax

Validate SQL syntax without parsing:

```bash
# Check syntax
dbsqlx check "SELECT * FROM users WHERE id = 1"

# Check file
dbsqlx check --file query.sql
dbsqlx check -f query.sql
```

### Generate mysqldump Commands

Generate mysqldump commands with intelligent filtering:

```bash
# Basic dump command
dbsqlx dump "SELECT * FROM users WHERE id = 1" -d mydb

# Dump with full connection details
dbsqlx dump --file query.sql \
  --user root \
  --password secret \
  --host localhost \
  --database production

# Short form
dbsqlx dump -f query.sql -u admin -p pass -H db.local -d mydb

# Use IP instead of host
dbsqlx dump -f query.sql -u admin --ip 192.168.1.100 -d mydb
```

## Flags

### Global Flags

Available for all commands:

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--file` | `-f` | Read SQL from file | - |
| `--user` | `-u` | Database user | - |
| `--password` | `-p` | Database password | - |
| `--host` | `-H` | Database host | - |
| `--ip` | - | Database IP (overrides host) | - |
| `--database` | `-d` | Database name | `database_name` |
| `--help` | `-h` | Show help | - |

## Commands

### `dbsqlx [sql]`

**Root command**: Parse and analyze SQL statements.

```bash
dbsqlx "SELECT u.name, p.title FROM users u JOIN posts p WHERE u.active = 1"
```

Output:
```
Columns: [name title id user_id active]
Tables: [users posts]
Action: SELECT
WHERE filter: users.active=1
```

### `dbsqlx check [sql]`

**Check command**: Validate SQL syntax.

```bash
dbsqlx check "SELECT * FROM users"
```

Output:
```
✓ SQL syntax is valid
```

### `dbsqlx dump [sql]`

**Dump command**: Generate mysqldump commands.

#### Simple Query

```bash
dbsqlx dump "SELECT * FROM users WHERE id = 1" -d mydb
```

Output:
```bash
mysqldump --where="id=1" mydb users
```

#### JOIN Query with Smart Filtering

```bash
dbsqlx dump "SELECT * FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = 1 AND p.published = TRUE" -d mydb
```

Output:
```bash
mysqldump --where="active=1" mydb users
mysqldump --where="published=TRUE" mydb posts
```

#### UPDATE with JOIN (Primary Table Only)

```bash
dbsqlx dump -f update.sql -u root -h localhost -d prod
```

For `UPDATE Employees e JOIN Departments d WHERE d.Name='Sales' AND e.Years>=5`:

Output:
```bash
# To get exact rows matching all JOIN conditions:
# Step 1: Get matching IDs
# mysql -N -e "SELECT e.id FROM Employees e JOIN Departments d ON <join_condition> WHERE Departments.Name='Sales' and Employees.Years>=5" prod > /tmp/Employees_ids.txt
# Step 2: Dump exact rows
# mysqldump -h localhost -u root --where="id IN ($(cat /tmp/Employees_ids.txt | tr '\n' ',' | sed 's/,$//' ))" prod Employees
#
# Or use partial filter (may include extra rows):
mysqldump -h localhost -u root --where="Years>=5" prod Employees
```

## Shell Completion

Generate shell completion scripts:

```bash
# Bash
dbsqlx completion bash > /etc/bash_completion.d/dbsqlx

# Zsh
dbsqlx completion zsh > "${fpath[1]}/_dbsqlx"

# Fish
dbsqlx completion fish > ~/.config/fish/completions/dbsqlx.fish

# PowerShell
dbsqlx completion powershell > dbsqlx.ps1
```

## Advanced Features

### Multi-Statement Files

Process multiple SQL statements from a file:

```bash
dbsqlx dump --file statements.sql -d mydb
```

The tool automatically:
- Parses all statements
- Generates separate mysqldump commands
- Filters WHERE conditions per table
- Handles UPDATE/DELETE to only dump primary tables

### Smart WHERE Filtering

For JOIN queries, the tool intelligently filters WHERE conditions:

```sql
SELECT * FROM users u 
JOIN orders o ON u.id = o.user_id 
WHERE u.status = 'active' AND o.total > 100
```

Generates:
```bash
mysqldump --where="status='active'" mydb users
mysqldump --where="total>100" mydb orders
```

### UPDATE/DELETE Special Handling

For UPDATE/DELETE with JOINs, only the primary (modified) table is dumped:

```sql
UPDATE Employees e 
JOIN Departments d ON e.dept_id = d.id 
SET e.salary = e.salary * 1.1 
WHERE d.name = 'Sales'
```

Generates mysqldump for **Employees only** (not Departments).



## Examples

### Example 1: Quick Syntax Check

```bash
dbsqlx check "SELECT name FROM users WHERE id IN (1,2,3)"
```

### Example 2: Parse Complex JOIN

```bash
dbsqlx -f complex_query.sql
```

### Example 3: Production Dump

```bash
dbsqlx dump \
  --file backup_queries.sql \
  --user backup_user \
  --password $(cat ~/.db_pass) \
  --ip 10.0.1.100 \
  --database production_db
```

### Example 4: Development Workflow

```bash
# Check syntax first
dbsqlx check -f migration.sql

# If valid, generate dumps
dbsqlx dump -f migration.sql -d test_db -u dev -h localhost
```

## License

Apache 2.0

## Credits

Built with:
- [Cobra](https://github.com/spf13/cobra) - CLI framework
- [TiDB Parser](https://github.com/pingcap/tidb) - SQL parsing

