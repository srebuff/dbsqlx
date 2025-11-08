package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var checkCmd = &cobra.Command{
	Use:   "check [sql-statement]",
	Short: "Check SQL syntax",
	Long: `Validate SQL syntax without parsing or analyzing.

Examples:
  dbsqlx check "SELECT * FROM users"
  dbsqlx check -f query.sql`,
	Args: cobra.MaximumNArgs(1),
	RunE: runCheck,
}

func init() {
	rootCmd.AddCommand(checkCmd)
}

func runCheck(cmd *cobra.Command, args []string) error {
	sql, err := getSQLInput(args)
	if err != nil {
		return err
	}

	if err := CheckSQLSyntax(sql); err != nil {
		return fmt.Errorf("SQL syntax error: %v", err)
	}

	fmt.Println("✓ SQL syntax is valid")
	return nil
}
