package luna

import (
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestDriverRegistered(t *testing.T) {
	// Test that the driver is registered with database/sql
	drivers := sql.Drivers()
	found := false
	for _, d := range drivers {
		if d == "luna" {
			found = true
			break
		}
	}
	if !found {
		t.Error("luna driver not registered")
	}
}

func TestDSNParsing(t *testing.T) {
	testCases := []struct {
		name        string
		dsn         string
		shouldError bool
	}{
		{"simple host:port", "localhost:7688", false},
		{"with luna scheme", "luna://localhost:7688", false},
		{"with tcp scheme", "tcp://localhost:7688", false},
		{"with auth", "user:pass@localhost:7688", false},
		{"full format", "luna://user:pass@localhost:7688", false},
		{"ipv4", "192.168.1.1:7688", false},
		{"ipv6", "[::1]:7688", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			connector, err := NewConnector(tc.dsn, nil)
			if tc.shouldError {
				if err == nil {
					t.Errorf("expected error for DSN %s", tc.dsn)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for DSN %s: %v", tc.dsn, err)
				}
				if connector == nil {
					t.Errorf("expected non-nil connector for DSN %s", tc.dsn)
				}
			}
		})
	}
}

func TestConnectorDriver(t *testing.T) {
	connector, err := NewConnector("localhost:7688", nil)
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}

	driver := connector.Driver()
	if driver == nil {
		t.Error("expected non-nil driver")
	}

	// Check that it's the luna driver
	_, ok := driver.(Driver)
	if !ok {
		t.Error("expected driver to be of type luna.Driver")
	}
}

func TestConnectorClose(t *testing.T) {
	connector, err := NewConnector("localhost:7688", nil)
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}

	// First close should succeed
	if err := connector.Close(); err != nil {
		t.Errorf("first close failed: %v", err)
	}

	// Second close should also succeed (idempotent)
	if err := connector.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}
}

func TestResultInterface(t *testing.T) {
	r := &result{rowsAffected: 42}

	// Test LastInsertId (should return ErrSkip)
	_, err := r.LastInsertId()
	if err != driver.ErrSkip {
		t.Errorf("expected ErrSkip, got %v", err)
	}

	// Test RowsAffected
	affected, err := r.RowsAffected()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if affected != 42 {
		t.Errorf("expected 42 rows affected, got %d", affected)
	}
}

func TestArgsToNamedArgs(t *testing.T) {
	values := []driver.Value{1, "test", 3.14}
	namedArgs := argsToNamedArgs(values)

	if len(namedArgs) != len(values) {
		t.Errorf("expected %d named args, got %d", len(values), len(namedArgs))
	}

	for i, arg := range namedArgs {
		if arg.Ordinal != i+1 {
			t.Errorf("arg %d: expected ordinal %d, got %d", i, i+1, arg.Ordinal)
		}
		if arg.Value != values[i] {
			t.Errorf("arg %d: expected value %v, got %v", i, values[i], arg.Value)
		}
	}
}

func TestOpenReturnsConnection(t *testing.T) {
	// This tests the Driver.Open method without actually connecting
	// It should return an error since we can't connect without a server
	d := Driver{}
	_, err := d.Open("invalid-host:99999")
	if err == nil {
		t.Skip("unexpectedly connected (or skipped connection)")
	}
	// We expect an error since there's no server, which is fine
}
