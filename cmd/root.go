package cmd

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
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	// Global flags
	fileInput string
	user      string
	password  string
	host      string
	ip        string
	database  string
)

// ResetGlobals resets all global variables (for testing)
func ResetGlobals() {
	fileInput = ""
	user = ""
	password = ""
	host = ""
	ip = ""
	database = "database_name"

	// Reset cobra command flags to prevent conflicts between test runs
	rootCmd.Flags().VisitAll(func(f *pflag.Flag) {
		f.Value.Set(f.DefValue)
	})
	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		f.Value.Set(f.DefValue)
	})
}

// ColX represents the visitor for extracting SQL information
type ColX struct {
	ColNames     []string
	TableNames   []string
	PrimaryTable string
	Action       string
	WhereFilter  string
	AliasMap     map[string]string
}

// rootCmd represents the base command
var rootCmd = &cobra.Command{
	Use:   "dbsqlx [sql-statement]",
	Short: "A SQL parser and analyzer tool",
	Long: `dbsqlx is a powerful SQL parser that extracts tables, columns, 
and generates mysqldump commands from SQL statements.

Examples:
  dbsqlx "SELECT * FROM users WHERE id = 1"
  dbsqlx -f query.sql
  dbsqlx dump -f query.sql -d mydb -u root -h localhost`,
	Args:              cobra.MaximumNArgs(1),
	RunE:              runParse,
	DisableAutoGenTag: true,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	// Disable automatic help command to avoid conflict with -h for host
	rootCmd.SetHelpCommand(&cobra.Command{Hidden: true})

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&fileInput, "file", "f", "", "Read SQL from file")
	rootCmd.PersistentFlags().StringVarP(&user, "user", "u", "", "Database user")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "P", "", "Database password")
	rootCmd.PersistentFlags().StringVarP(&host, "host", "h", "", "Database host")
	rootCmd.PersistentFlags().StringVar(&ip, "ip", "", "Database IP (overrides host)")
	rootCmd.PersistentFlags().StringVarP(&database, "database", "d", "database_name", "Database name")

	// Add manual help flag with --help only (no short flag)
	rootCmd.PersistentFlags().Bool("help", false, "Show help information")
}

func runParse(cmd *cobra.Command, args []string) error {
	sql, err := getSQLInput(args)
	if err != nil {
		return err
	}

	stmtNodes, err := ParseAll(sql)
	if err != nil {
		return fmt.Errorf("parse error: %v", err)
	}

	// Display parsed information
	for idx, stmtNode := range stmtNodes {
		colNames, tableNames, action, whereFilter, _ := Extract(&stmtNode)

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

	return nil
}

func getSQLInput(args []string) (string, error) {
	if fileInput != "" {
		content, err := os.ReadFile(fileInput)
		if err != nil {
			return "", fmt.Errorf("error reading file: %v", err)
		}
		return string(content), nil
	}

	if len(args) == 0 {
		return "", fmt.Errorf("no SQL statement provided")
	}

	return args[0], nil
}

// Helper functions from original code
func (v *ColX) Enter(in ast.Node) (ast.Node, bool) {
	if name, ok := in.(*ast.ColumnName); ok {
		v.ColNames = append(v.ColNames, name.Name.O)
	}

	switch stmt := in.(type) {
	case *ast.InsertStmt:
		v.Action = "INSERT"
		if stmt.Table != nil && stmt.Table.TableRefs != nil {
			v.extractTableNames(stmt.Table.TableRefs)
		}
	case *ast.UpdateStmt:
		v.Action = "UPDATE"
		if stmt.TableRefs != nil {
			v.extractTableNames(stmt.TableRefs.TableRefs)
			if len(v.TableNames) > 0 {
				v.PrimaryTable = v.TableNames[0]
			}
		}
		if stmt.Where != nil {
			v.extractWhereFilterFromExpr(stmt.Where)
		}
	case *ast.DeleteStmt:
		v.Action = "DELETE"
		if stmt.TableRefs != nil {
			v.extractTableNames(stmt.TableRefs.TableRefs)
			if len(v.TableNames) > 0 {
				v.PrimaryTable = v.TableNames[0]
			}
		}
		v.extractWhereFilter(stmt.Where)
	case *ast.SelectStmt:
		v.Action = "SELECT"
		if stmt.From != nil && stmt.From.TableRefs != nil {
			v.extractTableNames(stmt.From.TableRefs)
		}
		v.extractWhereFilter(stmt.Where)
	case *ast.AlterTableStmt:
		v.Action = "ALTER"
		if stmt.Table != nil {
			tableName := stmt.Table.Name.O
			if tableName != "" {
				v.TableNames = append(v.TableNames, tableName)
			}
		}
	case *ast.CreateTableStmt:
		v.Action = "CREATE"
		if stmt.Table != nil {
			tableName := stmt.Table.Name.O
			if tableName != "" {
				v.TableNames = append(v.TableNames, tableName)
			}
		}
	case *ast.DropTableStmt:
		v.Action = "DROP"
		for _, table := range stmt.Tables {
			if table != nil {
				tableName := table.Name.O
				if tableName != "" {
					v.TableNames = append(v.TableNames, tableName)
				}
			}
		}
	case *ast.TruncateTableStmt:
		v.Action = "TRUNCATE"
		if stmt.Table != nil {
			tableName := stmt.Table.Name.O
			if tableName != "" {
				v.TableNames = append(v.TableNames, tableName)
			}
		}
	}

	return in, false
}

func (v *ColX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (v *ColX) extractTableNames(join *ast.Join) {
	if join == nil {
		return
	}

	if join.Left != nil {
		switch left := join.Left.(type) {
		case *ast.TableSource:
			if tableName, ok := left.Source.(*ast.TableName); ok {
				tableNameStr := tableName.Name.O
				aliasStr := left.AsName.O

				if aliasStr != "" {
					v.AliasMap[aliasStr] = tableNameStr
				}

				found := false
				for _, name := range v.TableNames {
					if name == tableNameStr {
						found = true
						break
					}
				}
				if !found && tableNameStr != "" {
					v.TableNames = append(v.TableNames, tableNameStr)
				}
			}
		case *ast.Join:
			v.extractTableNames(left)
		}
	}

	if join.Right != nil {
		switch right := join.Right.(type) {
		case *ast.TableSource:
			if tableName, ok := right.Source.(*ast.TableName); ok {
				tableNameStr := tableName.Name.O
				aliasStr := right.AsName.O

				if aliasStr != "" {
					v.AliasMap[aliasStr] = tableNameStr
				}

				found := false
				for _, name := range v.TableNames {
					if name == tableNameStr {
						found = true
						break
					}
				}
				if !found && tableNameStr != "" {
					v.TableNames = append(v.TableNames, tableNameStr)
				}
			}
		case *ast.Join:
			v.extractTableNames(right)
		}
	}
}

func (v *ColX) extractWhereFilter(whereExpr ast.ExprNode) {
	if whereExpr != nil {
		buf := new(bytes.Buffer)
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err := whereExpr.Restore(ctx)
		if err == nil {
			filter := buf.String()
			filter = strings.ReplaceAll(filter, "`", "")
			re := regexp.MustCompile(`_UTF8MB4'(.*?)'`)
			filter = re.ReplaceAllString(filter, "'$1'")
			for alias, tableName := range v.AliasMap {
				aliasPattern := fmt.Sprintf(`\b%s\.`, regexp.QuoteMeta(alias))
				tableNameReplacement := fmt.Sprintf("%s.", tableName)
				filter = regexp.MustCompile(aliasPattern).ReplaceAllString(filter, tableNameReplacement)
			}
			filter = strings.ReplaceAll(filter, " AND ", " and ")
			v.WhereFilter = filter
		}
	}
}

func (v *ColX) extractWhereFilterFromExpr(whereExpr ast.ExprNode) {
	if whereExpr != nil {
		buf := new(bytes.Buffer)
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err := whereExpr.Restore(ctx)
		if err == nil {
			filter := buf.String()
			filter = strings.ReplaceAll(filter, "`", "")
			re := regexp.MustCompile(`_UTF8MB4'(.*?)'`)
			filter = re.ReplaceAllString(filter, "'$1'")
			for alias, tableName := range v.AliasMap {
				aliasPattern := fmt.Sprintf(`\b%s\.`, regexp.QuoteMeta(alias))
				tableNameReplacement := fmt.Sprintf("%s.", tableName)
				filter = regexp.MustCompile(aliasPattern).ReplaceAllString(filter, tableNameReplacement)
			}
			filter = strings.ReplaceAll(filter, " AND ", " and ")
			v.WhereFilter = filter
		}
	}
}

// Extract parses an AST node and extracts SQL information
func Extract(rootNode *ast.StmtNode) (colNames, tableNames []string, action, whereFilter, primaryTable string) {
	v := &ColX{
		AliasMap: make(map[string]string),
	}
	(*rootNode).Accept(v)
	return v.ColNames, v.TableNames, v.Action, v.WhereFilter, v.PrimaryTable
}

// ParseAll parses SQL and returns all statement nodes
func ParseAll(sql string) ([]ast.StmtNode, error) {
	p := parser.New()
	stmtNodes, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return stmtNodes, nil
}

// CheckSQLSyntax validates the SQL syntax and returns any errors found
func CheckSQLSyntax(sql string) error {
	p := parser.New()
	_, _, err := p.ParseSQL(sql)
	return err
}

// FilterWhereForTable extracts only the WHERE conditions relevant to a specific table
func FilterWhereForTable(whereFilter string, tableName string, allTables []string) string {
	if whereFilter == "" {
		return ""
	}

	if len(allTables) <= 1 {
		return whereFilter
	}

	conditions := strings.Split(whereFilter, " and ")
	var relevantConditions []string

	for _, condition := range conditions {
		condition = strings.TrimSpace(condition)

		if strings.Contains(condition, tableName+".") {
			condition = strings.ReplaceAll(condition, tableName+".", "")
			relevantConditions = append(relevantConditions, condition)
		} else {
			hasTablePrefix := false
			for _, tbl := range allTables {
				if strings.Contains(condition, tbl+".") {
					hasTablePrefix = true
					break
				}
			}
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
