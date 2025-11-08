package cmd

import (
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

			gotColNames, gotTableNames, gotAction, gotWhereFilter, _ := Extract(astNode)

			if !reflect.DeepEqual(gotColNames, tt.wantColNames) {
				t.Errorf("Extract() gotColNames = %v, want %v", gotColNames, tt.wantColNames)
			}

			if !reflect.DeepEqual(gotTableNames, tt.wantTableNames) {
				t.Errorf("Extract() gotTableNames = %v, want %v", gotTableNames, tt.wantTableNames)
			}

			if gotAction != tt.wantAction {
				t.Errorf("Extract() gotAction = %v, want %v", gotAction, tt.wantAction)
			}

			if gotWhereFilter != tt.wantWhereFilter {
				t.Errorf("Extract() gotWhereFilter = %v, want %v", gotWhereFilter, tt.wantWhereFilter)
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

			v := &ColX{
				AliasMap: make(map[string]string),
			}
			(*astNode).Accept(v)

			if v.Action != tt.wantAction {
				t.Errorf("ColX.Enter() action = %v, want %v", v.Action, tt.wantAction)
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
			err := CheckSQLSyntax(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckSQLSyntax() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
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
			stmtNodes, err := ParseAll(tt.sql)

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(stmtNodes) != tt.wantStmtCount {
				t.Errorf("ParseAll() got %d statements, want %d", len(stmtNodes), tt.wantStmtCount)
			}
		})
	}
}

func TestMultipleStatementsExtraction(t *testing.T) {
	sql := `ALTER TABLE ai_mig_project_space ADD COLUMN manual tinyint(1) DEFAULT 0;
	        ALTER TABLE deploy_env_ref_info ADD COLUMN status VARCHAR(50)`

	stmtNodes, err := ParseAll(sql)
	if err != nil {
		t.Fatalf("ParseAll() error = %v", err)
	}

	if len(stmtNodes) != 2 {
		t.Fatalf("Expected 2 statements, got %d", len(stmtNodes))
	}

	// Test first statement
	colNames1, tableNames1, action1, _, _ := Extract(&stmtNodes[0])
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
	colNames2, tableNames2, action2, _, _ := Extract(&stmtNodes[1])
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
			err := CheckSQLSyntax(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckSQLSyntax() error = %v, wantErr %v", err, tt.wantErr)
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
			got := FilterWhereForTable(tt.whereFilter, tt.tableName, tt.allTables)
			if got != tt.want {
				t.Errorf("FilterWhereForTable() = %q, want %q", got, tt.want)
			}
		})
	}
}
