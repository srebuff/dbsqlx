package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var dumpCmd = &cobra.Command{
	Use:   "dump [sql-statement]",
	Short: "Generate mysqldump commands",
	Long: `Generate mysqldump commands from SQL statements.

Automatically filters WHERE conditions per table and provides helpers
for JOINed queries.

Examples:
  dbsqlx dump "SELECT * FROM users WHERE id = 1" -d mydb
  dbsqlx dump -f query.sql -u root -h localhost -d production
  dbsqlx dump -f query.sql -u admin -p secret -d mydb --ip 192.168.1.100`,
	Args: cobra.MaximumNArgs(1),
	RunE: runDump,
}

func init() {
	rootCmd.AddCommand(dumpCmd)
}

func runDump(cmd *cobra.Command, args []string) error {
	sql, err := getSQLInput(args)
	if err != nil {
		return err
	}

	stmtNodes, err := ParseAll(sql)
	if err != nil {
		return fmt.Errorf("parse error: %v", err)
	}

	// Trim whitespace from connection parameters
	user = strings.TrimSpace(user)
	password = strings.TrimSpace(password)
	host = strings.TrimSpace(host)
	ip = strings.TrimSpace(ip)
	database = strings.TrimSpace(database)

	// Build connection options
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
		connOpts += fmt.Sprintf(" --password=%s", password)
	}

	// Process each statement
	for _, stmtNode := range stmtNodes {
		_, tableNames, action, whereFilter, primaryTable := Extract(&stmtNode)

		if len(tableNames) == 0 {
			fmt.Println("# No tables found in SQL statement")
			continue
		}

		// For UPDATE/DELETE, only dump the primary table
		tablesToDump := tableNames
		if (action == "UPDATE" || action == "DELETE") && primaryTable != "" {
			tablesToDump = []string{primaryTable}
		}

		// Generate mysqldump command for each table
		for _, tableName := range tablesToDump {
			tableSpecificFilter := FilterWhereForTable(whereFilter, tableName, tableNames)

			// For cross-table conditions, provide helper
			if (action == "UPDATE" || action == "DELETE") && tableName == primaryTable && len(tableNames) > 1 {
				allConditionsFilter := whereFilter
				for _, tbl := range tableNames {
					allConditionsFilter = strings.ReplaceAll(allConditionsFilter, tbl+".", "")
				}

				if allConditionsFilter != tableSpecificFilter && tableSpecificFilter != "" {
					fmt.Println("# To get exact rows matching all JOIN conditions:")
					fmt.Println("# Step 1: Get matching IDs")
					fmt.Printf("# mysql -N -e \"SELECT e.id FROM %s e ", tableName)

					for _, tbl := range tableNames {
						if tbl != tableName {
							alias := string(strings.ToLower(tbl)[0])
							if alias == string(strings.ToLower(tableName)[0]) {
								alias = string(strings.ToLower(tbl)[0:2])
							}
							fmt.Printf("JOIN %s %s ON <join_condition> ", tbl, alias)
						}
					}

					fmt.Printf("WHERE %s\" %s > /tmp/%s_ids.txt\n", whereFilter, database, tableName)
					fmt.Println("# Step 2: Dump exact rows")
					fmt.Printf("# mysqldump%s --where=\"id IN ($(cat /tmp/%s_ids.txt | tr '\\n' ',' | sed 's/,$//' ))\" %s %s\n", connOpts, tableName, database, tableName)
					fmt.Println("#")
					fmt.Println("# Or use partial filter (may include extra rows):")
				}
			}

			if tableSpecificFilter != "" {
				fmt.Printf("mysqldump%s --where=\"%s\" %s %s\n", connOpts, tableSpecificFilter, database, tableName)
			} else {
				fmt.Printf("mysqldump%s %s %s\n", connOpts, database, tableName)
			}
		}
	}

	return nil
}
