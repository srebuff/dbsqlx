package main

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func parseSQL(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	stmtNodes, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

func TestExtract(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		wantColNames    []string
		wantTableNames  []string
		wantAction      string
		wantWhereFilter string
	}{
		{
			name:            "INSERT statement",
			sql:             "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')",
			wantColNames:    []string{"id", "name", "email"},
			wantTableNames:  []string{"users"},
			wantAction:      "INSERT",
			wantWhereFilter: "",
		},
		{
			name:            "UPDATE statement",
			sql:             "UPDATE users SET name = 'Jane' WHERE id = 1",
			wantColNames:    []string{"name", "id"},
			wantTableNames:  []string{"users"},
			wantAction:      "UPDATE",
			wantWhereFilter: "id=1",
		},
		{
			name:            "DELETE statement",
			sql:             "DELETE FROM users WHERE id = 1",
			wantColNames:    []string{"id"},
			wantTableNames:  []string{"users"},
			wantAction:      "DELETE",
			wantWhereFilter: "id=1",
		},
		{
			name:            "UPDATE with complex WHERE",
			sql:             "UPDATE users SET name = 'Jane', email = 'jane@example.com' WHERE id = 1 AND status = 'active'",
			wantColNames:    []string{"name", "email", "id", "status"},
			wantTableNames:  []string{"users"},
			wantAction:      "UPDATE",
			wantWhereFilter: "id=1 and status='active'",
		},
		{
			name:            "DELETE with complex WHERE",
			sql:             "DELETE FROM users WHERE id = 1 AND created_at < '2023-01-01'",
			wantColNames:    []string{"id", "created_at"},
			wantTableNames:  []string{"users"},
			wantAction:      "DELETE",
			wantWhereFilter: "id=1 and created_at<'2023-01-01'",
		},
		{
			name:            "Complex DELETE with JOIN and aliases",
			sql:             "DELETE u FROM users u JOIN profiles p ON u.id = p.user_id WHERE u.status = 'inactive' AND p.last_login < '2023-01-01'",
			wantColNames:    []string{"id", "user_id", "status", "last_login"},
			wantTableNames:  []string{"users", "profiles"},
			wantAction:      "DELETE",
			wantWhereFilter: "users.status='inactive' and profiles.last_login<'2023-01-01'",
		},
		{
			name:            "UPDATE with alias",
			sql:             "UPDATE users u SET u.name = 'Jane' WHERE u.id = 1",
			wantColNames:    []string{"name", "id"},
			wantTableNames:  []string{"users"},
			wantAction:      "UPDATE",
			wantWhereFilter: "users.id=1",
		},
		{
			name:            "SELECT statement",
			sql:             "SELECT id, name FROM users WHERE id = 1",
			wantColNames:    []string{"id", "name", "id"},
			wantTableNames:  []string{"users"},
			wantAction:      "SELECT",
			wantWhereFilter: "id=1",
		},
		{
			name:            "INSERT with multiple columns",
			sql:             "INSERT INTO products (name, price, category_id) VALUES ('Product1', 10.99, 1)",
			wantColNames:    []string{"name", "price", "category_id"},
			wantTableNames:  []string{"products"},
			wantAction:      "INSERT",
			wantWhereFilter: "",
		},
		{
			name:            "UPDATE with multiple conditions",
			sql:             "UPDATE orders SET status = 'shipped', shipped_date = '2023-01-01' WHERE id = 1 AND customer_id = 2",
			wantColNames:    []string{"status", "shipped_date", "id", "customer_id"},
			wantTableNames:  []string{"orders"},
			wantAction:      "UPDATE",
			wantWhereFilter: "id=1 and customer_id=2",
		},
		{
			name:            "DELETE with multiple conditions",
			sql:             "DELETE FROM logs WHERE level = 'info' AND created_at < '2023-01-01'",
			wantColNames:    []string{"level", "created_at"},
			wantTableNames:  []string{"logs"},
			wantAction:      "DELETE",
			wantWhereFilter: "level='info' and created_at<'2023-01-01'",
		},
		{
			name:            "SELECT with JOIN and aliases",
			sql:             "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = 1",
			wantColNames:    []string{"name", "title", "id", "user_id", "active"},
			wantTableNames:  []string{"users", "posts"},
			wantAction:      "SELECT",
			wantWhereFilter: "users.active=1",
		},
		{
			name:            "UPDATE with no WHERE clause",
			sql:             "UPDATE users SET last_login = NOW()",
			wantColNames:    []string{"last_login"},
			wantTableNames:  []string{"users"},
			wantAction:      "UPDATE",
			wantWhereFilter: "",
		},
		// {
		// 	name:           "DELETE with no WHERE clause",
		// 	sql:            "DELETE FROM temp_data",
		// 	wantColNames:   []string{},
		// 	wantTableNames: []string{"temp_data"},
		// 	wantAction:     "DELETE",
		// 	wantWhereFilter: "",
		// },
		{
			name:            "ALTER TABLE ADD COLUMN",
			sql:             "ALTER TABLE users ADD COLUMN age INT DEFAULT 0",
			wantColNames:    []string{"age"},
			wantTableNames:  []string{"users"},
			wantAction:      "ALTER",
			wantWhereFilter: "",
		},
		{
			name:            "ALTER TABLE with Chinese comment",
			sql:             "ALTER TABLE ai_mig_project_space ADD COLUMN manual tinyint(1) DEFAULT 0 COMMENT '是否手动迁移3.0项目到工作空间的：0-否 1-是'",
			wantColNames:    []string{"manual"},
			wantTableNames:  []string{"ai_mig_project_space"},
			wantAction:      "ALTER",
			wantWhereFilter: "",
		},
		{
			name:            "CREATE TABLE statement",
			sql:             "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
			wantColNames:    []string{"id", "name"},
			wantTableNames:  []string{"users"},
			wantAction:      "CREATE",
			wantWhereFilter: "",
		},
		{
			name:            "DROP TABLE statement",
			sql:             "DROP TABLE users",
			wantColNames:    nil,
			wantTableNames:  []string{"users"},
			wantAction:      "DROP",
			wantWhereFilter: "",
		},
		{
			name:            "DROP TABLE multiple tables",
			sql:             "DROP TABLE users, orders, products",
			wantColNames:    nil,
			wantTableNames:  []string{"users", "orders", "products"},
			wantAction:      "DROP",
			wantWhereFilter: "",
		},
		{
			name:            "TRUNCATE TABLE statement",
			sql:             "TRUNCATE TABLE logs",
			wantColNames:    nil,
			wantTableNames:  []string{"logs"},
			wantAction:      "TRUNCATE",
			wantWhereFilter: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			astNode, err := parseSQL(tt.sql)
			if err != nil {
				t.Fatalf("parseSQL() error = %v", err)
			}

			gotColNames, gotTableNames, gotAction, gotWhereFilter := extract(astNode)

			if !reflect.DeepEqual(gotColNames, tt.wantColNames) {
				t.Errorf("extract() gotColNames = %v, want %v", gotColNames, tt.wantColNames)
			}

			if !reflect.DeepEqual(gotTableNames, tt.wantTableNames) {
				t.Errorf("extract() gotTableNames = %v, want %v", gotTableNames, tt.wantTableNames)
			}

			if gotAction != tt.wantAction {
				t.Errorf("extract() gotAction = %v, want %v", gotAction, tt.wantAction)
			}

			if gotWhereFilter != tt.wantWhereFilter {
				t.Errorf("extract() gotWhereFilter = %v, want %v", gotWhereFilter, tt.wantWhereFilter)
			}
		})
	}
}

func TestColXEnter(t *testing.T) {
	// Test that the visitor correctly identifies different statement types
	tests := []struct {
		name       string
		sql        string
		wantAction string
	}{
		{
			name:       "INSERT statement",
			sql:        "INSERT INTO users (id) VALUES (1)",
			wantAction: "INSERT",
		},
		{
			name:       "UPDATE statement",
			sql:        "UPDATE users SET id = 1",
			wantAction: "UPDATE",
		},
		{
			name:       "DELETE statement",
			sql:        "DELETE FROM users",
			wantAction: "DELETE",
		},
		{
			name:       "ALTER TABLE statement",
			sql:        "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			wantAction: "ALTER",
		},
		{
			name:       "CREATE TABLE statement",
			sql:        "CREATE TABLE products (id INT PRIMARY KEY)",
			wantAction: "CREATE",
		},
		{
			name:       "DROP TABLE statement",
			sql:        "DROP TABLE users",
			wantAction: "DROP",
		},
		{
			name:       "TRUNCATE TABLE statement",
			sql:        "TRUNCATE TABLE logs",
			wantAction: "TRUNCATE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			astNode, err := parseSQL(tt.sql)
			if err != nil {
				t.Fatalf("parseSQL() error = %v", err)
			}

			v := &colX{
				aliasMap: make(map[string]string),
			}
			(*astNode).Accept(v)

			if v.action != tt.wantAction {
				t.Errorf("colX.Enter() action = %v, want %v", v.action, tt.wantAction)
			}
		})
	}
}

func TestCheckSQLSyntax(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Valid SELECT statement",
			sql:     "SELECT id, name FROM users WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "Valid INSERT statement",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John')",
			wantErr: false,
		},
		{
			name:    "Valid UPDATE statement",
			sql:     "UPDATE users SET name = 'Jane' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "Valid DELETE statement",
			sql:     "DELETE FROM users WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "Invalid SQL - incomplete WHERE clause",
			sql:     "SELECT id, name FROM users WHERE id =",
			wantErr: true,
		},
		{
			name:    "Invalid SQL - missing table name",
			sql:     "SELECT id, name FROM WHERE id = 1",
			wantErr: true,
		},
		{
			name:    "Invalid SQL - incomplete INSERT",
			sql:     "INSERT INTO users (id, name) VALUES",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSQLSyntax(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkSQLSyntax() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGenerateMysqldumpCommand(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantCommand string
		expectError bool
	}{
		{
			name:        "SELECT with WHERE clause",
			sql:         "SELECT * FROM users WHERE id = 1",
			wantCommand: "mysqldump --where=\"id=1\" database_name users",
		},
		{
			name:        "SELECT without WHERE clause",
			sql:         "SELECT * FROM users",
			wantCommand: "mysqldump database_name users",
		},
		{
			name:        "DELETE with complex WHERE clause",
			sql:         "DELETE FROM users WHERE status = 'inactive' AND last_login < '2023-01-01'",
			wantCommand: "mysqldump --where=\"status='inactive' and last_login<'2023-01-01'\" database_name users",
		},
		{
			name:        "UPDATE statement",
			sql:         "UPDATE users SET name = 'Jane' WHERE id = 1",
			wantCommand: "mysqldump --where=\"id=1\" database_name users",
		},
		{
			name:        "INSERT statement",
			sql:         "INSERT INTO users (id, name) VALUES (1, 'John')",
			wantCommand: "mysqldump database_name users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			astNode, err := parseSQL(tt.sql)
			if err != nil {
				t.Fatalf("parseSQL() error = %v", err)
			}

			_, tableNames, _, whereFilter := extract(astNode)

			if len(tableNames) == 0 {
				if !tt.expectError {
					t.Errorf("Expected table names but got none")
				}
				return
			}

			// Generate mysqldump command
			var command string
			tableName := tableNames[0]
			if whereFilter != "" {
				command = fmt.Sprintf("mysqldump --where=\"%s\" database_name %s", whereFilter, tableName)
			} else {
				command = fmt.Sprintf("mysqldump database_name %s", tableName)
			}

			if command != tt.wantCommand {
				t.Errorf("generateMysqldumpCommand() = %v, want %v", command, tt.wantCommand)
			}
		})
	}
}

func TestReadSQLFromFile(t *testing.T) {
	// Create a temporary SQL file for testing
	sqlContent := "SELECT id, name FROM users WHERE id = 1"
	tmpfile, err := os.CreateTemp("", "test*.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(sqlContent)); err != nil {
		t.Fatal(err)
	}

	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	// Read SQL from file
	content, err := os.ReadFile(tmpfile.Name())
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}

	sql := string(content)
	if sql != sqlContent {
		t.Errorf("ReadSQLFromFile() = %v, want %v", sql, sqlContent)
	}

	// Test parsing the SQL from file
	astNode, err := parseSQL(sql)
	if err != nil {
		t.Fatalf("parseSQL() error = %v", err)
	}

	colNames, tableNames, action, whereFilter := extract(astNode)

	expectedColNames := []string{"id", "name", "id"}
	expectedTableNames := []string{"users"}
	expectedAction := "SELECT"
	expectedWhereFilter := "id=1"

	if !reflect.DeepEqual(colNames, expectedColNames) {
		t.Errorf("colNames = %v, want %v", colNames, expectedColNames)
	}

	if !reflect.DeepEqual(tableNames, expectedTableNames) {
		t.Errorf("tableNames = %v, want %v", tableNames, expectedTableNames)
	}

	if action != expectedAction {
		t.Errorf("action = %v, want %v", action, expectedAction)
	}

	if whereFilter != expectedWhereFilter {
		t.Errorf("whereFilter = %v, want %v", whereFilter, expectedWhereFilter)
	}
}

func TestDumpWithUserAndHost(t *testing.T) {
	// Save original args and stdout
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// Prepare CLI args
	os.Args = []string{
		"dbsqlx",
		"-dump",
		"-user", "root",
		"-host", "db.example.local",
		"SELECT * FROM users WHERE id = 42",
	}

	// Run main()
	main()

	// Close writer and read captured output
	_ = w.Close()
	captured, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read captured stdout: %v", err)
	}

	output := string(captured)

	// Expect mysqldump with -h and -u and where clause
	expected := "mysqldump -h db.example.local -u root --where=\"id=42\" database_name users\n"
	if output != expected {
		t.Errorf("unexpected output.\nGot:  %q\nWant: %q", output, expected)
	}
}

func TestParseAll(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantStmtCount int
		wantErr       bool
	}{
		{
			name:          "Single statement",
			sql:           "SELECT * FROM users",
			wantStmtCount: 1,
			wantErr:       false,
		},
		{
			name:          "Two statements",
			sql:           "SELECT * FROM users; SELECT * FROM orders",
			wantStmtCount: 2,
			wantErr:       false,
		},
		{
			name:          "Multiple ALTER statements",
			sql:           "ALTER TABLE users ADD COLUMN age INT; ALTER TABLE orders ADD COLUMN status VARCHAR(50)",
			wantStmtCount: 2,
			wantErr:       false,
		},
		{
			name: "Multiple DDL statements from file",
			sql: `ALTER TABLE ai_mig_project_space ADD COLUMN manual tinyint(1) DEFAULT 0 COMMENT '是否手动迁移3.0项目到工作空间的：0-否 1-是';

ALTER TABLE deploy_env_ref_info ADD COLUMN manual tinyint(1) DEFAULT 0 COMMENT '是否手动迁移3.0环境到主机组的：0-否 1-是';`,
			wantStmtCount: 2,
			wantErr:       false,
		},
		{
			name:          "Three mixed statements",
			sql:           "INSERT INTO users (id) VALUES (1); UPDATE users SET name = 'John' WHERE id = 1; DELETE FROM users WHERE id = 2",
			wantStmtCount: 3,
			wantErr:       false,
		},
		{
			name:          "Invalid SQL",
			sql:           "SELECT * FROM WHERE",
			wantStmtCount: 0,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmtNodes, err := parseAll(tt.sql)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(stmtNodes) != tt.wantStmtCount {
				t.Errorf("parseAll() got %d statements, want %d", len(stmtNodes), tt.wantStmtCount)
			}
		})
	}
}

func TestMultipleStatementsExtraction(t *testing.T) {
	sql := `ALTER TABLE ai_mig_project_space ADD COLUMN manual tinyint(1) DEFAULT 0;
	        ALTER TABLE deploy_env_ref_info ADD COLUMN status VARCHAR(50)`

	stmtNodes, err := parseAll(sql)
	if err != nil {
		t.Fatalf("parseAll() error = %v", err)
	}

	if len(stmtNodes) != 2 {
		t.Fatalf("Expected 2 statements, got %d", len(stmtNodes))
	}

	// Test first statement
	colNames1, tableNames1, action1, _ := extract(&stmtNodes[0])
	expectedColNames1 := []string{"manual"}
	expectedTableNames1 := []string{"ai_mig_project_space"}
	expectedAction1 := "ALTER"

	if !reflect.DeepEqual(colNames1, expectedColNames1) {
		t.Errorf("Statement 1: colNames = %v, want %v", colNames1, expectedColNames1)
	}
	if !reflect.DeepEqual(tableNames1, expectedTableNames1) {
		t.Errorf("Statement 1: tableNames = %v, want %v", tableNames1, expectedTableNames1)
	}
	if action1 != expectedAction1 {
		t.Errorf("Statement 1: action = %v, want %v", action1, expectedAction1)
	}

	// Test second statement
	colNames2, tableNames2, action2, _ := extract(&stmtNodes[1])
	expectedColNames2 := []string{"status"}
	expectedTableNames2 := []string{"deploy_env_ref_info"}
	expectedAction2 := "ALTER"

	if !reflect.DeepEqual(colNames2, expectedColNames2) {
		t.Errorf("Statement 2: colNames = %v, want %v", colNames2, expectedColNames2)
	}
	if !reflect.DeepEqual(tableNames2, expectedTableNames2) {
		t.Errorf("Statement 2: tableNames = %v, want %v", tableNames2, expectedTableNames2)
	}
	if action2 != expectedAction2 {
		t.Errorf("Statement 2: action = %v, want %v", action2, expectedAction2)
	}
}

func TestDDLStatementsWithCheckSyntax(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Valid ALTER TABLE",
			sql:     "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			wantErr: false,
		},
		{
			name:    "Valid CREATE TABLE",
			sql:     "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100))",
			wantErr: false,
		},
		{
			name:    "Valid DROP TABLE",
			sql:     "DROP TABLE temp_users",
			wantErr: false,
		},
		{
			name:    "Valid TRUNCATE TABLE",
			sql:     "TRUNCATE TABLE logs",
			wantErr: false,
		},
		{
			name:    "Invalid ALTER TABLE",
			sql:     "ALTER TABLE users ADD COLUMN",
			wantErr: true,
		},
		{
			name:    "Invalid CREATE TABLE",
			sql:     "CREATE TABLE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSQLSyntax(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkSQLSyntax() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDumpMultipleStatements(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantCommands []string
	}{
		{
			name: "Multiple statements with different tables",
			sql: `SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = 1 AND p.published = TRUE;
UPDATE users SET name = 'Jane' WHERE id = 1;
SELECT * FROM stat_git_record WHERE id IN ('gitee-22593-5e0e62e88c01e289be0b602ba553cdaec3fd084c')`,
			wantCommands: []string{
				"mysqldump --where=\"active=1\" database_name users",
				"mysqldump --where=\"published=TRUE\" database_name posts",
				"mysqldump --where=\"id=1\" database_name users",
				"mysqldump --where=\"id IN ('gitee-22593-5e0e62e88c01e289be0b602ba553cdaec3fd084c')\" database_name stat_git_record",
			},
		},
		{
			name: "Two SELECT statements with JOINs",
			sql: `SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active';
SELECT p.name, c.title FROM products p JOIN categories c ON p.category_id = c.id WHERE c.active = 1`,
			wantCommands: []string{
				"mysqldump --where=\"status='active'\" database_name users",
				"mysqldump database_name orders",
				"mysqldump database_name products",
				"mysqldump --where=\"active=1\" database_name categories",
			},
		},
		{
			name: "Mixed DML statements",
			sql: `DELETE FROM logs WHERE created_at < '2023-01-01';
UPDATE users SET last_login = NOW() WHERE id = 5;
INSERT INTO audit_log (action, timestamp) VALUES ('cleanup', NOW())`,
			wantCommands: []string{
				"mysqldump --where=\"created_at<'2023-01-01'\" database_name logs",
				"mysqldump --where=\"id=5\" database_name users",
				"mysqldump database_name audit_log",
			},
		},
		{
			name: "Single statement with multiple tables",
			sql:  `SELECT u.name, o.total, p.title FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id WHERE u.active = 1`,
			wantCommands: []string{
				"mysqldump --where=\"active=1\" database_name users",
				"mysqldump database_name orders",
				"mysqldump database_name products",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmtNodes, err := parseAll(tt.sql)
			if err != nil {
				t.Fatalf("parseAll() error = %v", err)
			}

			var commands []string
			for _, stmtNode := range stmtNodes {
				_, tableNames, _, whereFilter := extract(&stmtNode)

				for _, tableName := range tableNames {
					// Filter the WHERE clause to only include conditions relevant to this table
					tableSpecificFilter := filterWhereForTable(whereFilter, tableName, tableNames)

					var command string
					if tableSpecificFilter != "" {
						command = fmt.Sprintf("mysqldump --where=\"%s\" database_name %s", tableSpecificFilter, tableName)
					} else {
						command = fmt.Sprintf("mysqldump database_name %s", tableName)
					}
					commands = append(commands, command)
				}
			}

			if !reflect.DeepEqual(commands, tt.wantCommands) {
				t.Errorf("Generated commands mismatch")
				t.Errorf("Got:")
				for i, cmd := range commands {
					t.Errorf("  [%d] %s", i, cmd)
				}
				t.Errorf("Want:")
				for i, cmd := range tt.wantCommands {
					t.Errorf("  [%d] %s", i, cmd)
				}
			}
		})
	}
}

func TestDumpMultipleStatementsWithConnection(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		user         string
		password     string
		host         string
		ip           string
		wantCommands []string
	}{
		{
			name: "Multiple statements with user and host",
			sql: `SELECT * FROM users WHERE id = 1;
SELECT * FROM orders WHERE user_id = 1`,
			user: "root",
			host: "localhost",
			wantCommands: []string{
				"mysqldump -h localhost -u root --where=\"id=1\" database_name users",
				"mysqldump -h localhost -u root --where=\"user_id=1\" database_name orders",
			},
		},
		{
			name: "Multiple statements with ip, user and password",
			sql: `UPDATE users SET status = 'active' WHERE id = 5;
DELETE FROM logs WHERE level = 'debug'`,
			user:     "admin",
			password: "secret",
			ip:       "192.168.1.100",
			wantCommands: []string{
				"mysqldump -h 192.168.1.100 -u admin --password=secret --where=\"id=5\" database_name users",
				"mysqldump -h 192.168.1.100 -u admin --password=secret --where=\"level='debug'\" database_name logs",
			},
		},
		{
			name: "Statement with JOIN and connection options",
			sql:  `SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = 1`,
			user: "dbuser",
			host: "db.example.com",
			wantCommands: []string{
				"mysqldump -h db.example.com -u dbuser --where=\"active=1\" database_name users",
				"mysqldump -h db.example.com -u dbuser database_name posts",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmtNodes, err := parseAll(tt.sql)
			if err != nil {
				t.Fatalf("parseAll() error = %v", err)
			}

			// Build connection options
			connTarget := ""
			if tt.ip != "" {
				connTarget = tt.ip
			} else if tt.host != "" {
				connTarget = tt.host
			}

			connOpts := ""
			if connTarget != "" {
				connOpts += fmt.Sprintf(" -h %s", connTarget)
			}
			if tt.user != "" {
				connOpts += fmt.Sprintf(" -u %s", tt.user)
			}
			if tt.password != "" {
				connOpts += fmt.Sprintf(" --password=%s", tt.password)
			}

			var commands []string
			for _, stmtNode := range stmtNodes {
				_, tableNames, _, whereFilter := extract(&stmtNode)

				for _, tableName := range tableNames {
					// Filter the WHERE clause to only include conditions relevant to this table
					tableSpecificFilter := filterWhereForTable(whereFilter, tableName, tableNames)

					var command string
					if tableSpecificFilter != "" {
						command = fmt.Sprintf("mysqldump%s --where=\"%s\" database_name %s", connOpts, tableSpecificFilter, tableName)
					} else {
						command = fmt.Sprintf("mysqldump%s database_name %s", connOpts, tableName)
					}
					commands = append(commands, command)
				}
			}

			if !reflect.DeepEqual(commands, tt.wantCommands) {
				t.Errorf("Generated commands mismatch")
				t.Errorf("Got:")
				for i, cmd := range commands {
					t.Errorf("  [%d] %s", i, cmd)
				}
				t.Errorf("Want:")
				for i, cmd := range tt.wantCommands {
					t.Errorf("  [%d] %s", i, cmd)
				}
			}
		})
	}
}

func TestFilterWhereForTable(t *testing.T) {
	tests := []struct {
		name        string
		whereFilter string
		tableName   string
		allTables   []string
		want        string
	}{
		{
			name:        "Single table - return all conditions",
			whereFilter: "id=1 and status='active'",
			tableName:   "users",
			allTables:   []string{"users"},
			want:        "id=1 and status='active'",
		},
		{
			name:        "Multi-table - filter users conditions",
			whereFilter: "users.active=1 and posts.published=TRUE",
			tableName:   "users",
			allTables:   []string{"users", "posts"},
			want:        "active=1",
		},
		{
			name:        "Multi-table - filter posts conditions",
			whereFilter: "users.active=1 and posts.published=TRUE",
			tableName:   "posts",
			allTables:   []string{"users", "posts"},
			want:        "published=TRUE",
		},
		{
			name:        "Multi-table - multiple conditions for same table",
			whereFilter: "users.active=1 and users.status='premium' and posts.published=TRUE",
			tableName:   "users",
			allTables:   []string{"users", "posts"},
			want:        "active=1 and status='premium'",
		},
		{
			name:        "Multi-table - no relevant conditions",
			whereFilter: "users.active=1 and posts.published=TRUE",
			tableName:   "orders",
			allTables:   []string{"users", "posts", "orders"},
			want:        "",
		},
		{
			name:        "Empty filter",
			whereFilter: "",
			tableName:   "users",
			allTables:   []string{"users", "posts"},
			want:        "",
		},
		{
			name:        "Three tables - mixed conditions",
			whereFilter: "users.active=1 and orders.status='pending' and products.available=TRUE",
			tableName:   "orders",
			allTables:   []string{"users", "orders", "products"},
			want:        "status='pending'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterWhereForTable(tt.whereFilter, tt.tableName, tt.allTables)
			if got != tt.want {
				t.Errorf("filterWhereForTable() = %q, want %q", got, tt.want)
			}
		})
	}
}
