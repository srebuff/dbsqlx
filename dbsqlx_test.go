package main

import (
	"dbsqlx/cmd"
	"io"
	"os"
	"strings"
	"testing"
)

// Integration tests that test the entire CLI by calling main()
// Unit tests have been moved to cmd/ package

func TestDumpWithUserAndHost(t *testing.T) {
	// Save original args and stdout
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	defer cmd.ResetGlobals()

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
		"dump",
		"SELECT * FROM users WHERE id = 42",
		"--user", "root",
		"--host", "db.example.local",
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

func TestDumpWithDatabase(t *testing.T) {
	// Save original args and stdout
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	defer cmd.ResetGlobals()

	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// Prepare CLI args with database flag
	os.Args = []string{
		"dbsqlx",
		"dump",
		"SELECT * FROM users WHERE id = 42",
		"--user", "ops_admin",
		"--host", "10.101.101.101",
		"--database", "devops_base",
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

	// Expect mysqldump with database name devops_base
	expected := "mysqldump -h 10.101.101.101 -u ops_admin --where=\"id=42\" devops_base users\n"
	if output != expected {
		t.Errorf("unexpected output.\nGot:  %q\nWant: %q", output, expected)
	}
}

func TestDumpWithoutDatabase(t *testing.T) {
	// Save original args and stdout
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Reset cmd package globals
	defer func() {
		cmd.ResetGlobals()
	}()

	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// Prepare CLI args without database flag (should use default)
	os.Args = []string{
		"dbsqlx",
		"dump",
		"SELECT * FROM products WHERE price > 100",
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

	// Expect mysqldump with default database_name
	expected := "mysqldump --where=\"price>100\" database_name products\n"
	if output != expected {
		t.Errorf("unexpected output.\nGot:  %q\nWant: %q", output, expected)
	}
}

func TestDumpWithDatabaseAndFile(t *testing.T) {
	// Create a temporary SQL file for testing
	sqlContent := `SELECT * FROM users WHERE id = 1;
SELECT * FROM orders WHERE user_id = 1;`
	tmpfile, err := os.CreateTemp("", "test*.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	defer cmd.ResetGlobals()

	if _, err := tmpfile.Write([]byte(sqlContent)); err != nil {
		t.Fatal(err)
	}

	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

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

	// Prepare CLI args with file and database
	os.Args = []string{
		"dbsqlx",
		"dump",
		"--file", tmpfile.Name(),
		"--database", "test_db",
		"--user", "testuser",
		"--host", "localhost",
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

	// Expect two mysqldump commands with test_db database
	expectedLines := []string{
		"mysqldump -h localhost -u testuser --where=\"id=1\" test_db users",
		"mysqldump -h localhost -u testuser --where=\"user_id=1\" test_db orders",
	}

	for _, expected := range expectedLines {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected output to contain:\n%s\nGot:\n%s", expected, output)
		}
	}
}

func TestDumpWithAllConnectionFlags(t *testing.T) {
	// Save original args and stdout
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	defer cmd.ResetGlobals()

	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// Prepare CLI args with all connection flags
	os.Args = []string{
		"dbsqlx",
		"dump",
		"SELECT * FROM logs WHERE level = 'error'",
		"--user", "admin",
		"--password", "secret123",
		"--ip", "192.168.1.100",
		"--database", "production_db",
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

	// Expect mysqldump with all connection flags and custom database
	expected := "mysqldump -h 192.168.1.100 -u admin --password=secret123 --where=\"level='error'\" production_db logs\n"
	if output != expected {
		t.Errorf("unexpected output.\nGot:  %q\nWant: %q", output, expected)
	}
}
