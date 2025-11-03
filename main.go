package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

type colX struct {
	colNames    []string
	tableNames  []string
	action      string
	whereFilter string
	aliasMap    map[string]string // Map of alias to table name
}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
	// Extract column names
	if name, ok := in.(*ast.ColumnName); ok {
		v.colNames = append(v.colNames, name.Name.O)
	}

	// Extract table names and action type
	switch stmt := in.(type) {
	case *ast.InsertStmt:
		v.action = "INSERT"
		if stmt.Table != nil && stmt.Table.TableRefs != nil {
			v.extractTableNames(stmt.Table.TableRefs)
		}
	case *ast.UpdateStmt:
		v.action = "UPDATE"
		if stmt.TableRefs != nil {
			v.extractTableNames(stmt.TableRefs.TableRefs)
		}
		// Extract WHERE filter
		if stmt.Where != nil {
			buf := new(bytes.Buffer)
			ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
			err := stmt.Where.Restore(ctx)
			if err == nil {
				// Clean up the filter text to make it more readable
				filter := buf.String()
				// Remove backticks
				filter = strings.ReplaceAll(filter, "`", "")
				// Replace _UTF8MB4'...' with just the string value
				re := regexp.MustCompile(`_UTF8MB4'(.*?)'`)
				filter = re.ReplaceAllString(filter, "'$1'")
				// Replace aliases with actual table names
				for alias, tableName := range v.aliasMap {
					// Replace alias.column with tableName.column
					aliasPattern := fmt.Sprintf(`\b%s\.`, regexp.QuoteMeta(alias))
					tableNameReplacement := fmt.Sprintf("%s.", tableName)
					filter = regexp.MustCompile(aliasPattern).ReplaceAllString(filter, tableNameReplacement)
				}
				// Convert AND to lowercase "and"
				filter = strings.ReplaceAll(filter, " AND ", " and ")
				v.whereFilter = filter
			}
		}
	case *ast.DeleteStmt:
		v.action = "DELETE"
		if stmt.TableRefs != nil {
			v.extractTableNames(stmt.TableRefs.TableRefs)
		}
		v.extractWhereFilter(stmt.Where)
	case *ast.SelectStmt:
		v.action = "SELECT"
		if stmt.From != nil && stmt.From.TableRefs != nil {
			v.extractTableNames(stmt.From.TableRefs)
		}
		v.extractWhereFilter(stmt.Where)
	case *ast.AlterTableStmt:
		v.action = "ALTER"
		if stmt.Table != nil {
			tableName := stmt.Table.Name.O
			if tableName != "" {
				v.tableNames = append(v.tableNames, tableName)
			}
		}
	case *ast.CreateTableStmt:
		v.action = "CREATE"
		if stmt.Table != nil {
			tableName := stmt.Table.Name.O
			if tableName != "" {
				v.tableNames = append(v.tableNames, tableName)
			}
		}
	case *ast.DropTableStmt:
		v.action = "DROP"
		for _, table := range stmt.Tables {
			if table != nil {
				tableName := table.Name.O
				if tableName != "" {
					v.tableNames = append(v.tableNames, tableName)
				}
			}
		}
	case *ast.TruncateTableStmt:
		v.action = "TRUNCATE"
		if stmt.Table != nil {
			tableName := stmt.Table.Name.O
			if tableName != "" {
				v.tableNames = append(v.tableNames, tableName)
			}
		}
	case *ast.TableName:
		// We'll handle table names through TableSource to avoid processing aliases
		// Table names are handled in extractTableNames through TableSource
		// This prevents aliases from being added to the table names list
		_ = stmt
	}

	return in, false
}

func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// extractTableNames recursively extracts table names from Join nodes
func (v *colX) extractTableNames(join *ast.Join) {
	if join == nil {
		return
	}

	// Process left side
	if join.Left != nil {
		switch left := join.Left.(type) {
		case *ast.TableSource:
			if tableName, ok := left.Source.(*ast.TableName); ok {
				// Use alias if available, otherwise use table name
				tableNameStr := tableName.Name.O
				aliasStr := left.AsName.O

				// Add to alias map if there's an alias
				if aliasStr != "" {
					v.aliasMap[aliasStr] = tableNameStr
				}

				// Check if table name already exists to avoid duplicates
				found := false
				for _, name := range v.tableNames {
					if name == tableNameStr {
						found = true
						break
					}
				}
				if !found && tableNameStr != "" {
					v.tableNames = append(v.tableNames, tableNameStr)
				}
			}
		case *ast.Join:
			v.extractTableNames(left)
		}
	}

	// Process right side
	if join.Right != nil {
		switch right := join.Right.(type) {
		case *ast.TableSource:
			if tableName, ok := right.Source.(*ast.TableName); ok {
				// Use alias if available, otherwise use table name
				tableNameStr := tableName.Name.O
				aliasStr := right.AsName.O

				// Add to alias map if there's an alias
				if aliasStr != "" {
					v.aliasMap[aliasStr] = tableNameStr
				}

				// Check if table name already exists to avoid duplicates
				found := false
				for _, name := range v.tableNames {
					if name == tableNameStr {
						found = true
						break
					}
				}
				if !found && tableNameStr != "" {
					v.tableNames = append(v.tableNames, tableNameStr)
				}
			}
		case *ast.Join:
			v.extractTableNames(right)
		}
	}
}

// extractWhereFilter extracts and cleans up the WHERE filter text
func (v *colX) extractWhereFilter(whereExpr ast.ExprNode) {
	if whereExpr != nil {
		buf := new(bytes.Buffer)
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err := whereExpr.Restore(ctx)
		if err == nil {
			// Clean up the filter text to make it more readable
			filter := buf.String()
			// Remove backticks
			filter = strings.ReplaceAll(filter, "`", "")
			// Replace _UTF8MB4'...' with just the string value
			re := regexp.MustCompile(`_UTF8MB4'(.*?)'`)
			filter = re.ReplaceAllString(filter, "'$1'")
			// Replace aliases with actual table names
			for alias, tableName := range v.aliasMap {
				// Replace alias.column with tableName.column
				aliasPattern := fmt.Sprintf(`\b%s\.`, regexp.QuoteMeta(alias))
				tableNameReplacement := fmt.Sprintf("%s.", tableName)
				filter = regexp.MustCompile(aliasPattern).ReplaceAllString(filter, tableNameReplacement)
			}
			// Convert AND to lowercase "and"
			filter = strings.ReplaceAll(filter, " AND ", " and ")
			v.whereFilter = filter
		}
	}
}

func extract(rootNode *ast.StmtNode) (colNames, tableNames []string, action, whereFilter string) {
	v := &colX{
		aliasMap: make(map[string]string),
	}
	(*rootNode).Accept(v)
	return v.colNames, v.tableNames, v.action, v.whereFilter
}

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

// parseAll parses SQL and returns all statement nodes
func parseAll(sql string) ([]ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	return stmtNodes, nil
}

// checkSQLSyntax validates the SQL syntax and returns any errors found
func checkSQLSyntax(sql string) error {
	p := parser.New()
	_, _, err := p.ParseSQL(sql)
	return err
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: dbsqlx 'SQL statement'")
		fmt.Println("   or: dbsqlx -check 'SQL statement' (to check syntax only)")
		fmt.Println("   or: dbsqlx -dump 'SQL statement' (to generate mysqldump command)")
		fmt.Println("   or: dbsqlx -file 'file.sql' (to read SQL from file)")
		fmt.Println("   or: dbsqlx -check -file 'file.sql' (to check syntax from file)")
		fmt.Println("   or: dbsqlx -dump -file 'file.sql' (to generate mysqldump from file)")
		fmt.Println("        Optional connection flags for -dump: -user USER -password PASS -host HOST -ip IP")
		return
	}

	// Check if the first argument is a flag
	dumpMode := false
	checkOnly := false
	fileMode := false
	sql := ""
	user := ""
	password := ""
	host := ""
	ip := ""

	// Parse command line arguments
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-check":
			checkOnly = true
		case "-dump":
			dumpMode = true
		case "-file":
			fileMode = true
			i++
			if i < len(args) {
				// Read SQL from file
				content, err := os.ReadFile(args[i])
				if err != nil {
					fmt.Printf("Error reading file: %v\n", err)
					return
				}
				sql = string(content)
			} else {
				fmt.Println("Missing file name after -file flag")
				return
			}
		case "-user":
			i++
			if i < len(args) {
				user = args[i]
			} else {
				fmt.Println("Missing value after -user flag")
				return
			}
		case "-password":
			i++
			if i < len(args) {
				password = args[i]
			} else {
				fmt.Println("Missing value after -password flag")
				return
			}
		case "-host":
			i++
			if i < len(args) {
				host = args[i]
			} else {
				fmt.Println("Missing value after -host flag")
				return
			}
		case "-ip":
			i++
			if i < len(args) {
				ip = args[i]
			} else {
				fmt.Println("Missing value after -ip flag")
				return
			}
		default:
			if !fileMode {
				sql = args[i]
			}
		}
	}

	// If no SQL was provided and not in file mode, show usage
	if sql == "" && !fileMode {
		fmt.Println("usage: dbsqlx 'SQL statement'")
		fmt.Println("   or: dbsqlx -check 'SQL statement' (to check syntax only)")
		fmt.Println("   or: dbsqlx -dump 'SQL statement' (to generate mysqldump command)")
		fmt.Println("   or: dbsqlx -file 'file.sql' (to read SQL from file)")
		fmt.Println("   or: dbsqlx -check -file 'file.sql' (to check syntax from file)")
		fmt.Println("   or: dbsqlx -dump -file 'file.sql' (to generate mysqldump from file)")
		fmt.Println("        Optional connection flags for -dump: -user USER -password PASS -host HOST -ip IP")
		return
	}

	// If check only mode, just validate syntax
	if checkOnly {
		if err := checkSQLSyntax(sql); err != nil {
			fmt.Printf("SQL syntax error: %v\n", err.Error())
			return
		}
		fmt.Println("SQL syntax is valid")
		return
	}

	// Otherwise, process the SQL as usual
	stmtNodes, err := parseAll(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}

	// Process each statement
	for idx, stmtNode := range stmtNodes {
		colNames, tableNames, action, whereFilter := extract(&stmtNode)

		// If dump mode, generate mysqldump command
		if dumpMode {
			if len(tableNames) == 0 {
				fmt.Println("No tables found in SQL statement")
				continue
			}

			// For now, we'll use the first table name for the mysqldump command
			tableName := tableNames[0]

			// Build connection options, prefer ip over host if provided
			connTarget := ""
			if ip != "" {
				connTarget = ip
			} else if host != "" {
				connTarget = host
			}

			connOpts := ""
			if connTarget != "" {
				connOpts += fmt.Sprintf(" -h %s", connTarget)
			}
			if user != "" {
				connOpts += fmt.Sprintf(" -u %s", user)
			}
			if password != "" {
				// Use --password=... syntax to avoid spacing issues
				connOpts += fmt.Sprintf(" --password=%s", password)
			}

			// Generate mysqldump command
			if whereFilter != "" {
				fmt.Printf("mysqldump%s --where=\"%s\" database_name %s\n", connOpts, whereFilter, tableName)
			} else {
				fmt.Printf("mysqldump%s database_name %s\n", connOpts, tableName)
			}
			continue
		}

		// Normal output mode
		// If there are multiple statements, add a separator and statement number
		if len(stmtNodes) > 1 {
			if idx > 0 {
				fmt.Println("---")
			}
			fmt.Printf("Statement %d:\n", idx+1)
		}
		fmt.Printf("Columns: %v\n", colNames)
		fmt.Printf("Tables: %v\n", tableNames)
		fmt.Printf("Action: %s\n", action)
		if whereFilter != "" {
			fmt.Printf("WHERE filter: %s\n", whereFilter)
		}
	}
}
