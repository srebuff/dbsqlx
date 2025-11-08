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
	colNames     []string
	tableNames   []string
	primaryTable string // The main table being modified (for UPDATE/DELETE)
	action       string
	whereFilter  string
	aliasMap     map[string]string // Map of alias to table name
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
			// For UPDATE, the first table is the primary table being updated
			if len(v.tableNames) > 0 {
				v.primaryTable = v.tableNames[0]
			}
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
			// For DELETE, the first table is the primary table being deleted from
			if len(v.tableNames) > 0 {
				v.primaryTable = v.tableNames[0]
			}
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

// filterWhereForTable extracts only the WHERE conditions relevant to a specific table
func filterWhereForTable(whereFilter string, tableName string, allTables []string) string {
	if whereFilter == "" {
		return ""
	}

	// If there's only one table, return the whole filter
	if len(allTables) <= 1 {
		return whereFilter
	}

	// Split by "and" to get individual conditions
	conditions := strings.Split(whereFilter, " and ")
	var relevantConditions []string

	for _, condition := range conditions {
		condition = strings.TrimSpace(condition)

		// Check if this condition references the specific table
		// Look for "tableName." prefix
		if strings.Contains(condition, tableName+".") {
			// Remove the table name prefix for mysqldump
			condition = strings.ReplaceAll(condition, tableName+".", "")
			relevantConditions = append(relevantConditions, condition)
		} else {
			// Check if condition has no table prefix (applies to current table)
			hasTablePrefix := false
			for _, tbl := range allTables {
				if strings.Contains(condition, tbl+".") {
					hasTablePrefix = true
					break
				}
			}
			// If no table prefix found, it might apply to this table
			if !hasTablePrefix {
				relevantConditions = append(relevantConditions, condition)
			}
		}
	}

	if len(relevantConditions) == 0 {
		return ""
	}

	return strings.Join(relevantConditions, " and ")
}

// Note: For UPDATE/DELETE with JOINs, mysqldump can only filter on columns
// from the target table. Cross-table conditions would require manual subqueries.

func extract(rootNode *ast.StmtNode) (colNames, tableNames []string, action, whereFilter, primaryTable string) {
	v := &colX{
		aliasMap: make(map[string]string),
	}
	(*rootNode).Accept(v)
	return v.colNames, v.tableNames, v.action, v.whereFilter, v.primaryTable
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
		fmt.Println("        Optional connection flags for -dump: -user USER -password PASS -host HOST -ip IP -database DATABASE")
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
	database := "database_name"

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
		case "-database":
			i++
			if i < len(args) {
				database = args[i]
			} else {
				fmt.Println("Missing value after -database flag")
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
		fmt.Println("        Optional connection flags for -dump: -user USER -password PASS -host HOST -ip IP -database DATABASE")
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
		colNames, tableNames, action, whereFilter, primaryTable := extract(&stmtNode)

		// If dump mode, generate mysqldump command
		if dumpMode {
			if len(tableNames) == 0 {
				fmt.Println("No tables found in SQL statement")
				continue
			}

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

			// For UPDATE/DELETE statements, only dump the primary table being modified
			// For SELECT statements, dump all tables in the JOIN
			tablesToDump := tableNames
			if (action == "UPDATE" || action == "DELETE") && primaryTable != "" {
				tablesToDump = []string{primaryTable}
			}

			// Generate mysqldump command for each table
			for _, tableName := range tablesToDump {
				// Filter the WHERE clause to only include conditions relevant to this table
				tableSpecificFilter := filterWhereForTable(whereFilter, tableName, tableNames)

				// Note: mysqldump --where can only use columns from the target table
				// For cross-table conditions, provide a helper query to get exact IDs
				if (action == "UPDATE" || action == "DELETE") && tableName == primaryTable && len(tableNames) > 1 {
					// Check if there are conditions from other tables
					allConditionsFilter := whereFilter
					for _, tbl := range tableNames {
						allConditionsFilter = strings.ReplaceAll(allConditionsFilter, tbl+".", "")
					}

					if allConditionsFilter != tableSpecificFilter && tableSpecificFilter != "" {
						// Generate helper SQL to get exact matching IDs
						fmt.Printf("# To get exact rows matching all JOIN conditions:\n")

						// Build the JOIN query to get primary keys
						// Assume first column is the primary key (common convention)
						fmt.Printf("# Step 1: Get matching IDs\n")
						fmt.Printf("# mysql -N -e \"SELECT e.id FROM %s e ", tableName)

						// Add JOIN clauses for other tables
						for _, tbl := range tableNames {
							if tbl != tableName {
								// Use simple alias (first letter)
								alias := string(strings.ToLower(tbl)[0])
								if alias == string(strings.ToLower(tableName)[0]) {
									alias = string(strings.ToLower(tbl)[0:2])
								}
								fmt.Printf("JOIN %s %s ON <join_condition> ", tbl, alias)
							}
						}

						// Restore original filter with table prefixes
						fmt.Printf("WHERE %s\" %s > /tmp/%s_ids.txt\n", whereFilter, database, tableName)

						fmt.Printf("# Step 2: Dump exact rows\n")
						fmt.Printf("# mysqldump%s --where=\"id IN ($(cat /tmp/%s_ids.txt | tr '\\n' ',' | sed 's/,$//' ))\" %s %s\n", connOpts, tableName, database, tableName)
						fmt.Printf("#\n")
						fmt.Printf("# Or use partial filter (may include extra rows):\n")
					}
				}

				if tableSpecificFilter != "" {
					fmt.Printf("mysqldump%s --where=\"%s\" %s %s\n", connOpts, tableSpecificFilter, database, tableName)
				} else {
					fmt.Printf("mysqldump%s %s %s\n", connOpts, database, tableName)
				}
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
