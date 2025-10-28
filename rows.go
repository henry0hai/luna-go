package luna

import (
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type Rows struct {
	records   []arrow.Record
	recordIdx int
	rowIdx    int64
	columns   []string
	closed    bool
}

// newRowsFromArrow creates a new Rows from Arrow records
func newRowsFromArrow(records []arrow.Record) *Rows {
	var columns []string
	if len(records) > 0 && records[0].Schema() != nil {
		schema := records[0].Schema()
		for i := 0; i < int(schema.NumFields()); i++ {
			columns = append(columns, schema.Field(i).Name)
		}
	}

	return &Rows{
		records:   records,
		recordIdx: 0,
		rowIdx:    0,
		columns:   columns,
	}
}

func (r *Rows) Columns() []string {
	return r.columns
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	// Check if we need to move to the next record
	for r.recordIdx < len(r.records) {
		record := r.records[r.recordIdx]

		if r.rowIdx < record.NumRows() {
			// Extract values from current row
			for i := 0; i < int(record.NumCols()); i++ {
				col := record.Column(i)
				val, err := getValueFromColumn(col, int(r.rowIdx))
				if err != nil {
					return err
				}
				dest[i] = val
			}
			r.rowIdx++
			return nil
		}

		// Move to next record
		r.recordIdx++
		r.rowIdx = 0
	}

	return io.EOF
}

func (r *Rows) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	// Release Arrow records
	for _, record := range r.records {
		record.Release()
	}
	r.records = nil

	return nil
}

// getValueFromColumn extracts a value from an Arrow column at the given row index
func getValueFromColumn(col arrow.Array, rowIdx int) (interface{}, error) {
	if col.IsNull(rowIdx) {
		return nil, nil
	}

	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(rowIdx), nil
	case *array.Int8:
		return arr.Value(rowIdx), nil
	case *array.Int16:
		return arr.Value(rowIdx), nil
	case *array.Int32:
		return arr.Value(rowIdx), nil
	case *array.Int64:
		return arr.Value(rowIdx), nil
	case *array.Uint8:
		return arr.Value(rowIdx), nil
	case *array.Uint16:
		return arr.Value(rowIdx), nil
	case *array.Uint32:
		return arr.Value(rowIdx), nil
	case *array.Uint64:
		return arr.Value(rowIdx), nil
	case *array.Float32:
		return arr.Value(rowIdx), nil
	case *array.Float64:
		return arr.Value(rowIdx), nil
	case *array.String:
		return arr.Value(rowIdx), nil
	case *array.Binary:
		return arr.Value(rowIdx), nil
	case *array.Date32:
		return arr.Value(rowIdx).ToTime(), nil
	case *array.Date64:
		return arr.Value(rowIdx).ToTime(), nil
	case *array.Timestamp:
		return arr.Value(rowIdx).ToTime(arr.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Decimal128:
		// Convert to string for simplicity
		return arr.Value(rowIdx).ToString(int32(arr.DataType().(*arrow.Decimal128Type).Scale)), nil
	case *array.Decimal256:
		return arr.Value(rowIdx).ToString(int32(arr.DataType().(*arrow.Decimal256Type).Scale)), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %T", arr)
	}
}
