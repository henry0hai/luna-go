package luna

type result struct {
	rowsAffected int64
}

// Implements the driver.Result interface.
func (r result) LastInsertId() (int64, error) {
	return 0, nil
}

// Implements the driver.Result interface.
func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
