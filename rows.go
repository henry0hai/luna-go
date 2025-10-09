package luna

import (
	"database/sql/driver"
	"reflect"
)

// rows is a helper struct for scanning a duckdb result.
type rows struct {
	// stmt is a pointer to the stmt of which we are scanning the result.
	stmt *Stmt
	// closeChunk is true after the first iteration of Next.
	closeChunk bool
	// rowCount is the number of scanned rows.
	rowCount int
	// cached column metadata to avoid repeated CGO calls
	scanTypes   []reflect.Type
	dbTypeNames []string
}

// Implements the driver.Rows interface.
func (r *rows) Columns() []string {
	return []string{}
}

// Implements the driver.Rows interface.
func (r *rows) Next(dst []driver.Value) error {
	return nil
}

// Implements the driver.Rows interface.
func (r *rows) Close() error {
	return nil
}
