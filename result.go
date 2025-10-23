package luna

import "database/sql/driver"

type result struct {
	rowsAffected int64
}

// Implements the driver.Result interface.
func (r *result) LastInsertId() (int64, error) {
	// Luna doesn't support last insert ID
	return 0, driver.ErrSkip
}

// Implements the driver.Result interface.
func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
