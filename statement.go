package luna

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type Stmt struct {
	conn   *Conn
	query  string
	closed bool
}

// Implements the driver.Stmt interface.
func (s *Stmt) Close() error {
	if s.closed {
		return fmt.Errorf("statement already closed")
	}
	s.closed = true
	return nil
}

// Implements the driver.Stmt interface.
func (s *Stmt) NumInput() int {
	if s.closed {
		panic("database/sql/driver: misuse of luna driver: NumInput after Close")
	}
	return -1 // -1 means the driver doesn't know
}

// Deprecated: Use ExecContext instead.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), argsToNamedArgs(args))
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
// It implements the driver.StmtExecContext interface.
func (s *Stmt) ExecContext(ctx context.Context, nargs []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, fmt.Errorf("statement is closed")
	}

	// Use the connection's ExecContext
	return s.conn.ExecContext(ctx, s.query, nargs)
}

// Deprecated: Use QueryContext instead.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), argsToNamedArgs(args))
}

// QueryContext executes a query that may return rows, such as a SELECT.
// It implements the driver.StmtQueryContext interface.
func (s *Stmt) QueryContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, fmt.Errorf("statement is closed")
	}

	// Use the connection's QueryContext
	return s.conn.QueryContext(ctx, s.query, nargs)
}

func argsToNamedArgs(values []driver.Value) []driver.NamedValue {
	args := make([]driver.NamedValue, len(values))
	for n, param := range values {
		args[n].Value = param
		args[n].Ordinal = n + 1
	}
	return args
}
