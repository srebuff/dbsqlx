package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var dumpArgs string

var dumpCmd = &cobra.Command{
	Use:   "dump [sql-statement]",
	Short: "Generate mysqldump commands",
	Long: `Generate mysqldump commands from SQL statements.

Automatically filters WHERE conditions per table and provides helpers
for JOINed queries.

Note: SELECT and DDL statements are skipped (only INSERT/UPDATE/DELETE generate dumps).

Examples:
  dbsqlx dump "UPDATE users SET name='John' WHERE id = 1" -d mydb
  dbsqlx dump -f query.sql -u root -h localhost -d production
  dbsqlx dump "DELETE FROM users WHERE id=1" --dump-args "--skip-lock-tables --single-transaction"`,
	Args: cobra.MaximumNArgs(1),
	RunE: runDump,
}

func init() {
	rootCmd.AddCommand(dumpCmd)
	dumpCmd.Flags().StringVar(&dumpArgs, "dump-args", "", "Additional arguments to pass to mysqldump (space-separated)")
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
	port = strings.TrimSpace(port)
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
	// Only include port if it's not the default 3306
	if port != "" && port != "3306" {
		connOpts += fmt.Sprintf(" -P %s", port)
	}
	if user != "" {
		connOpts += fmt.Sprintf(" -u %s", user)
	}
	if password != "" {
		connOpts += fmt.Sprintf(" --password=%s", password)
	}

	// Add additional mysqldump arguments
	dumpArgsStr := ""
	dumpArgs = strings.TrimSpace(dumpArgs)
	if dumpArgs != "" {
		// Split by spaces and filter out empty strings
		args := strings.Fields(dumpArgs)
		if len(args) > 0 {
			dumpArgsStr = " " + strings.Join(args, " ")
		}
	}

	// Track unique dump commands (deduplicate)
	type DumpCommand struct {
		table   string
		filter  string
		comment string
	}
	seenCommands := make(map[string]bool)
	var dumpCommands []DumpCommand

	// Process each statement
	for _, stmtNode := range stmtNodes {
		_, tableNames, action, whereFilter, primaryTable := Extract(&stmtNode)

		if len(tableNames) == 0 {
			continue
		}

		// Skip SELECT statements - they're just queries, not data modifications
		if action == "SELECT" {
			continue
		}

		// Skip DDL statements that don't affect data
		if action == "ALTER" || action == "CREATE" {
			continue
		}

		// For DROP/TRUNCATE, dump the entire table (no WHERE filter needed)
		if action == "DROP" || action == "TRUNCATE" {
			// These operations will destroy data, so dump the entire table
			for _, tableName := range tableNames {
				uniqueKey := fmt.Sprintf("%s||%s", tableName, database)
				if seenCommands[uniqueKey] {
					continue
				}
				seenCommands[uniqueKey] = true

				dumpCommands = append(dumpCommands, DumpCommand{
					table:   tableName,
					filter:  "",
					comment: fmt.Sprintf("# Backup before %s", action),
				})
			}
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

			// Create unique key for deduplication
			uniqueKey := fmt.Sprintf("%s|%s|%s", tableName, tableSpecificFilter, database)
			if seenCommands[uniqueKey] {
				continue // Skip duplicate
			}
			seenCommands[uniqueKey] = true

			comment := ""
			// For cross-table conditions, provide helper
			if (action == "UPDATE" || action == "DELETE") && tableName == primaryTable && len(tableNames) > 1 {
				allConditionsFilter := whereFilter
				for _, tbl := range tableNames {
					allConditionsFilter = strings.ReplaceAll(allConditionsFilter, tbl+".", "")
				}

				if allConditionsFilter != tableSpecificFilter && tableSpecificFilter != "" {
					comment = fmt.Sprintf("# To get exact rows matching all JOIN conditions:\n"+
						"# Step 1: Get matching IDs\n"+
						"# mysql -N -e \"SELECT e.id FROM %s e ", tableName)

					for _, tbl := range tableNames {
						if tbl != tableName {
							alias := string(strings.ToLower(tbl)[0])
							if alias == string(strings.ToLower(tableName)[0]) {
								alias = string(strings.ToLower(tbl)[0:2])
							}
							comment += fmt.Sprintf("JOIN %s %s ON <join_condition> ", tbl, alias)
						}
					}

					comment += fmt.Sprintf("WHERE %s\" %s > /tmp/%s_ids.txt\n"+
						"# Step 2: Dump exact rows\n"+
						"# mysqldump%s%s --where=\"id IN ($(cat /tmp/%s_ids.txt | tr '\\n' ',' | sed 's/,$//' ))\" %s %s\n"+
						"#\n"+
						"# Or use partial filter (may include extra rows):",
						whereFilter, database, tableName, connOpts, dumpArgsStr, tableName, database, tableName)
				}
			}

			dumpCommands = append(dumpCommands, DumpCommand{
				table:   tableName,
				filter:  tableSpecificFilter,
				comment: comment,
			})
		}
	}

	// Output all unique commands
	for _, dc := range dumpCommands {
		if dc.comment != "" {
			fmt.Println(dc.comment)
		}
		if dc.filter != "" {
			fmt.Printf("mysqldump%s%s --where=\"%s\" %s %s\n", connOpts, dumpArgsStr, dc.filter, database, dc.table)
		} else {
			fmt.Printf("mysqldump%s%s %s %s\n", connOpts, dumpArgsStr, database, dc.table)
		}
	}

	return nil
}
