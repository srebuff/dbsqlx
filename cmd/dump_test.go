package cmd

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

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

			_, tableNames, _, whereFilter, _ := Extract(astNode)

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

	colNames, tableNames, action, whereFilter, _ := Extract(astNode)

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
			stmtNodes, err := ParseAll(tt.sql)
			if err != nil {
				t.Fatalf("ParseAll() error = %v", err)
			}

			var commands []string
			for _, stmtNode := range stmtNodes {
				_, tableNames, _, whereFilter, _ := Extract(&stmtNode)

				for _, tableName := range tableNames {
					// Filter the WHERE clause to only include conditions relevant to this table
					tableSpecificFilter := FilterWhereForTable(whereFilter, tableName, tableNames)

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
			stmtNodes, err := ParseAll(tt.sql)
			if err != nil {
				t.Fatalf("ParseAll() error = %v", err)
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
				_, tableNames, _, whereFilter, _ := Extract(&stmtNode)

				for _, tableName := range tableNames {
					// Filter the WHERE clause to only include conditions relevant to this table
					tableSpecificFilter := FilterWhereForTable(whereFilter, tableName, tableNames)

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

func TestDumpUpdateWithJoin(t *testing.T) {
	sql := `UPDATE Employees e
INNER JOIN Departments d ON e.DepartmentID = d.DepartmentID
SET e.Salary = e.Salary * 1.10
WHERE d.DepartmentName = 'Sales' AND e.YearsOfService >= 5`

	stmtNodes, err := ParseAll(sql)
	if err != nil {
		t.Fatalf("ParseAll() error = %v", err)
	}

	if len(stmtNodes) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmtNodes))
	}

	_, tableNames, action, whereFilter, primaryTable := Extract(&stmtNodes[0])

	// For UPDATE with JOIN, should only dump the primary table (Employees)
	if action != "UPDATE" {
		t.Errorf("Expected action UPDATE, got %v", action)
	}

	if primaryTable != "Employees" {
		t.Errorf("Expected primaryTable 'Employees', got %v", primaryTable)
	}

	expectedTables := []string{"Employees", "Departments"}
	if !reflect.DeepEqual(tableNames, expectedTables) {
		t.Errorf("Expected tables %v, got %v", expectedTables, tableNames)
	}

	// The WHERE filter should include conditions from both tables
	expectedWhere := "Departments.DepartmentName='Sales' and Employees.YearsOfService>=5"
	if whereFilter != expectedWhere {
		t.Errorf("Expected WHERE %q, got %q", expectedWhere, whereFilter)
	}

	// When generating mysqldump, we should only dump the primary table
	// and filter WHERE conditions for that table
	tableSpecificFilter := FilterWhereForTable(whereFilter, primaryTable, tableNames)

	// For UPDATE/DELETE with JOIN, we need to handle cross-table conditions differently
	// mysqldump can't use conditions from other tables
	if !strings.Contains(tableSpecificFilter, "YearsOfService>=5") {
		t.Errorf("Expected filtered WHERE to contain YearsOfService condition, got %q", tableSpecificFilter)
	}
}

