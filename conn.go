package luna

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net"
)

type Conn struct {
	conn net.Conn
	// True, if the connection has been closed, else false.
	closed bool
	// True, if the connection has an open transaction.
	tx bool
}

// It implements the driver.ExecerContext interface.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	slog.Info("ExecContext called")
	return nil, nil
}

// TODO: Implements the driver.Pinger interface.
func (c *Conn) Ping(context.Context) error { return nil }

// Implements the driver.Conn interface.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, fmt.Errorf("luna: connection closed")
	}

	return nil, nil
}

// Begin is deprecated: Use BeginTx instead.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// Implements the driver.ConnBeginTx interface.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx {
		return nil, fmt.Errorf("luna: there is already an open transaction")
	}

	if _, err := c.ExecContext(ctx, `BEGIN TRANSACTION`, nil); err != nil {
		return nil, err
	}

	c.tx = true
	return nil, nil
}

// Implements the driver.Conn interface.
func (c *Conn) Close() error {
	if c.closed {
		return fmt.Errorf("luna: connection already closed")
	}

	c.closed = true
	return nil
}
