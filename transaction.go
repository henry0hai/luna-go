package luna

import "context"

type tx struct {
	c *Conn
}

// TODO: Since Luna server may not support transactions, we might need to simulate them client-side.
// Implements the driver.Tx interface.
func (t *tx) Commit() error {
	if t.c == nil || !t.c.tx {
		panic("database/sql/driver: misuse of duckdb driver: extra Commit")
	}

	t.c.tx = false
	_, err := t.c.ExecContext(context.Background(), "COMMIT TRANSACTION", nil)
	t.c = nil

	return err
}

// Implements the driver.Tx interface.
func (t *tx) Rollback() error {
	if t.c == nil || !t.c.tx {
		panic("database/sql/driver: misuse of duckdb driver: extra Rollback")
	}

	t.c.tx = false
	_, err := t.c.ExecContext(context.Background(), "ROLLBACK", nil)
	t.c = nil

	return err
}
