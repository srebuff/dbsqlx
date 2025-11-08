package cmd

import (
	"testing"
)

// TestCheckCommand tests the check command functionality
// The core CheckSQLSyntax function is tested in root_test.go
// This file can be extended with additional check command-specific tests

func TestCheckCommandValidSQL(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Valid simple SELECT",
			sql:     "SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "Valid SELECT with JOIN",
			sql:     "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = 1",
			wantErr: false,
		},
		{
			name:    "Valid UPDATE",
			sql:     "UPDATE users SET name = 'John' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "Valid DELETE",
			sql:     "DELETE FROM users WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "Valid INSERT",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John')",
			wantErr: false,
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

func TestCheckCommandInvalidSQL(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Invalid - missing table",
			sql:     "SELECT * FROM WHERE id = 1",
			wantErr: true,
		},
		{
			name:    "Invalid - incomplete WHERE",
			sql:     "SELECT * FROM users WHERE id =",
			wantErr: true,
		},
		{
			name:    "Invalid - incomplete INSERT",
			sql:     "INSERT INTO users (id, name) VALUES",
			wantErr: true,
		},
		{
			name:    "Invalid - syntax error",
			sql:     "SELCT * FROM users",
			wantErr: true,
		},
		{
			name:    "Invalid - garbage text",
			sql:     "THIS IS NOT SQL",
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

func TestCheckCommandMultipleStatements(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Multiple valid statements",
			sql:     "SELECT * FROM users; SELECT * FROM posts;",
			wantErr: false,
		},
		{
			name:    "First valid, second invalid",
			sql:     "SELECT * FROM users; SELECT * FROM WHERE;",
			wantErr: true,
		},
		{
			name:    "First invalid, second valid",
			sql:     "SELECT * FROM WHERE; SELECT * FROM users;",
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
