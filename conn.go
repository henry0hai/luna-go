package luna

import (
	"bufio"
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net"

	"github.com/apache/arrow/go/v17/arrow"
)

type Conn struct {
	// For test stubbing: if true, return temp table results
	tempTableQuery bool
	conn           net.Conn
	reader         *bufio.Reader // Buffered reader for the connection
	// True, if the connection has been closed, else false.
	closed bool
	// True, if the connection has an open transaction.
	tx bool
}

// It implements the driver.ExecerContext interface.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	slog.Info("ExecContext called", "query", query)

	// Send execute command
	if err := sendCommand(c.conn, cmdExecute, query); err != nil {
		return nil, fmt.Errorf("failed to send command: %w", err)
	}

	// Read response
	respType, data, err := readResponse(c.reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Handle errors
	if respType == "error" {
		return nil, fmt.Errorf("luna error: %s", string(data))
	}

	// Luna might return Arrow IPC data even for ExecContext
	// We need to consume it but don't use it for DDL/DML
	if respType == "arrow-stream" {
		// Read and discard the Arrow data
		_, err = parseArrowIPCFromReader(c.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Arrow IPC: %w", err)
		}
	}

	// For DDL/DML, we typically don't get row counts from Luna
	// Return a result with 0 rows affected
	return &result{rowsAffected: 0}, nil
}

// Implements the driver.QueryerContext interface.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	slog.Info("QueryContext called", "query", query)

	// Send query command
	if err := sendCommand(c.conn, cmdQuery, query); err != nil {
		return nil, fmt.Errorf("failed to send command: %w", err)
	}

	// Read response
	respType, data, err := readResponse(c.reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Handle errors
	if respType == "error" {
		return nil, fmt.Errorf("luna error: %s", string(data))
	}

	// Handle Arrow IPC stream
	var records []arrow.Record
	if respType == "arrow-stream" {
		// Read Arrow IPC directly from the buffered reader
		records, err = parseArrowIPCFromReader(c.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Arrow IPC: %w", err)
		}
	} else {
		// Parse Arrow IPC from buffered data (old path)
		records, err = parseArrowIPC(data)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Arrow IPC: %w", err)
		}
	}

	// Create Rows from Arrow records
	return newRowsFromArrow(records), nil
}

// Ping implements the driver.Pinger interface.
// It verifies the connection to Luna server is still alive.
func (c *Conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}

	// Execute a simple query to verify the connection
	rows, err := c.QueryContext(ctx, "SELECT 1", nil)
	if err != nil {
		return err
	}

	// Close the rows to release resources
	if rows != nil {
		rows.Close()
	}

	return nil
}

// Implements the driver.Conn interface.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, fmt.Errorf("luna: connection closed")
	}
	stmt := &Stmt{conn: c, query: query}
	return stmt, nil
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
	return &tx{c: c}, nil
}

// Implements the driver.Conn interface.
func (c *Conn) Close() error {
	if c.closed {
		return fmt.Errorf("luna: connection already closed")
	}

	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
