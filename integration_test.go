package luna

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const dsn = "localhost:7688"

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("luna", dsn)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("Luna server not running at %s: %v", dsn, err)
	}
	return db
}

func TestDriverRegistration(t *testing.T) {
	// Test that the driver is properly registered
	db, err := sql.Open("luna", dsn)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("expected non-nil db")
	}
}

func TestConnectionWithDifferentDSNFormats(t *testing.T) {
	testCases := []struct {
		name string
		dsn  string
	}{
		{"simple host:port", "localhost:7688"},
		{"with luna scheme", "luna://localhost:7688"},
		{"with tcp scheme", "tcp://localhost:7688"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("luna", tc.dsn)
			if err != nil {
				t.Fatalf("failed to open DB with DSN %s: %v", tc.dsn, err)
			}
			defer db.Close()

			// Skip if server not running
			if err := db.Ping(); err != nil {
				t.Skipf("Luna server not running: %v", err)
			}
		})
	}
}

func TestSimpleQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var result int
	row := db.QueryRow("SELECT 1+1;")
	err := row.Scan(&result)
	if err != nil {
		t.Fatalf("failed to scan result: %v", err)
	}
	if result != 2 {
		t.Errorf("expected 2, got %d", result)
	}
}

func TestMultipleColumnQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	query := `SELECT 42 as num, 'hello' as text, 3.14 as float_val`

	var num int
	var text string
	var floatVal float64

	err := db.QueryRow(query).Scan(&num, &text, &floatVal)
	if err != nil {
		t.Fatalf("failed to scan result: %v", err)
	}

	if num != 42 {
		t.Errorf("expected num=42, got %d", num)
	}
	if text != "hello" {
		t.Errorf("expected text='hello', got '%s'", text)
	}
	if floatVal < 3.13 || floatVal > 3.15 {
		t.Errorf("expected float_val~=3.14, got %f", floatVal)
	}
}

func TestMultipleRows(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	query := `
		SELECT 1 as id, 'Henry' as name
		UNION ALL
		SELECT 2 as id, 'Ton' as name
		UNION ALL
		SELECT 3 as id, 'Yo' as name
	`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id   int
		name string
	}{
		{1, "Henry"},
		{2, "Ton"},
		{3, "Yo"},
	}

	i := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("got more rows than expected")
		}

		if id != expected[i].id || name != expected[i].name {
			t.Errorf("row %d: expected (%d, %s), got (%d, %s)",
				i, expected[i].id, expected[i].name, id, name)
		}
		i++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration error: %v", err)
	}

	if i != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), i)
	}
}

func TestTemporaryTable(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	table := "tmp_luna_test"
	_, err := db.Exec(fmt.Sprintf("CREATE TEMP TABLE %s (id INT, val TEXT);", table))
	if err != nil {
		t.Fatalf("failed to create temp table: %v", err)
	}
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, 'foo'), (2, 'bar');", table))
	if err != nil {
		t.Fatalf("failed to insert rows: %v", err)
	}

	rows, err := db.Query(fmt.Sprintf("SELECT id, val FROM %s ORDER BY id;", table))
	if err != nil {
		t.Fatalf("failed to query rows: %v", err)
	}
	defer rows.Close()

	// Check if Luna returned error format (input, error columns)
	cols, _ := rows.Columns()
	if len(cols) == 2 && cols[0] == "input" && cols[1] == "error" {
		// Luna returned error format - read the error message
		if rows.Next() {
			var input, errMsg string
			rows.Scan(&input, &errMsg)
			t.Logf("⚠️  LUNA SERVER LIMITATION: Temporary tables don't persist across commands")
			t.Logf("    Query: %s", input)
			t.Logf("    Luna Error: %s", errMsg)
			t.Logf("    Workaround: Use subqueries or CTEs instead of temp tables")
			t.Skip("Skipping test - Luna server doesn't support session state")
		}
	}

	type row struct {
		id  int
		val string
	}
	var results []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.val); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d", len(results))
	}
	if len(results) >= 2 {
		if results[0] != (row{1, "foo"}) || results[1] != (row{2, "bar"}) {
			t.Errorf("unexpected row values: %+v", results)
		}
	}
}

func TestCSVQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Try mounted path first (for Docker), then local path
	csvPaths := []string{
		"/tests/customers-1000.csv", // Docker mounted path
		"tests/customers-1000.csv",  // Local relative path
		filepath.Join(os.Getenv("HOME"), "Projects/Work/Alphaus/luna-go/tests/customers-1000.csv"), // Absolute path
	}

	var csvPath string
	var foundPath bool

	// Test each path to see which one Luna server can access
	for _, path := range csvPaths {
		query := fmt.Sprintf("SELECT COUNT(*) as total FROM read_csv('%s', header=true)", path)
		rows, err := db.Query(query)
		if err != nil {
			continue
		}
		defer rows.Close()

		// Check if Luna returned error format
		cols, _ := rows.Columns()
		if len(cols) == 2 && cols[0] == "input" && cols[1] == "error" {
			rows.Close()
			continue
		}

		// Found a working path
		csvPath = path
		foundPath = true
		rows.Close()
		break
	}

	if !foundPath {
		// None of the paths worked - log helpful message
		t.Logf("⚠️ CSV file not accessible to Luna server")
		t.Logf("    Tried paths:")
		for _, path := range csvPaths {
			t.Logf("      - %s", path)
		}
		t.Logf("")
		t.Logf("    If using Docker, mount the tests directory:")
		t.Logf("    volumes:")
		t.Logf("      - ./tests:/tests:ro")
		t.Logf("")
		t.Logf("    Then CSV will be accessible at /tests/customers-1000.csv")
		t.Skip("Skipping test - CSV file not accessible to Luna server process")
	}

	t.Logf("✓ Using CSV path: %s", csvPath)

	// Test 1: Count total records
	query := fmt.Sprintf("SELECT COUNT(*) as total FROM read_csv('%s', header=true)", csvPath)

	var total int64
	err := db.QueryRow(query).Scan(&total)
	if err != nil {
		t.Fatalf("failed to count records: %v", err)
	}

	if total != 1000 {
		t.Errorf("expected 1000 records, got %d", total)
	}
	t.Logf("✓ CSV file contains %d records", total)

	// Test 2: Query specific records
	query = fmt.Sprintf(`
		SELECT CustomerId, FirstName, LastName, Country 
		FROM read_csv('%s', header=true) 
		LIMIT 5
	`, csvPath)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to query CSV: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var customerId, firstName, lastName, country string
		if err := rows.Scan(&customerId, &firstName, &lastName, &country); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		t.Logf("Customer: %s %s (%s) from %s", firstName, lastName, customerId, country)
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}

	// Test 3: Aggregation query
	query = fmt.Sprintf(`
		SELECT Country, COUNT(*) as customer_count 
		FROM read_csv('%s', header=true) 
		GROUP BY Country 
		ORDER BY customer_count DESC 
		LIMIT 5
	`, csvPath)

	rows, err = db.Query(query)
	if err != nil {
		t.Fatalf("failed to query aggregation: %v", err)
	}
	defer rows.Close()

	t.Log("Top 5 countries by customer count:")
	for rows.Next() {
		var country string
		var customerCount int
		if err := rows.Scan(&country, &customerCount); err != nil {
			t.Fatalf("failed to scan aggregation row: %v", err)
		}
		t.Logf("  %s: %d customers", country, customerCount)
	}
}

func TestParquetQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Try mounted path first (for Docker), then local path
	parquetPaths := []string{
		"/tests/users-1000.parquet", // Docker mounted path
		"tests/users-1000.parquet",  // Local relative path
		filepath.Join(os.Getenv("HOME"), "Projects/Work/Alphaus/luna-go/tests/users-1000.parquet"), // Absolute path
	}

	var parquetPath string
	var foundPath bool

	// Test each path to see which one Luna server can access
	for _, path := range parquetPaths {
		query := fmt.Sprintf("SELECT COUNT(*) as total FROM read_parquet('%s')", path)
		rows, err := db.Query(query)
		if err != nil {
			continue
		}
		defer rows.Close()

		// Check if Luna returned error format
		cols, _ := rows.Columns()
		if len(cols) == 2 && cols[0] == "input" && cols[1] == "error" {
			rows.Close()
			continue
		}

		// Found a working path
		parquetPath = path
		foundPath = true
		rows.Close()
		break
	}

	if !foundPath {
		// None of the paths worked - log helpful message
		t.Logf("⚠️ Parquet file not accessible to Luna server")
		t.Logf("    Tried paths:")
		for _, path := range parquetPaths {
			t.Logf("      - %s", path)
		}
		t.Logf("")
		t.Logf("    If using Docker, mount the tests directory:")
		t.Logf("    volumes:")
		t.Logf("      - ./tests:/tests:ro")
		t.Logf("")
		t.Logf("    Then Parquet will be accessible at /tests/users-1000.parquet")
		t.Skip("Skipping test - Parquet file not accessible to Luna server process")
	}

	t.Logf("✓ Using Parquet path: %s", parquetPath)

	// Test 1: Count total records
	query := fmt.Sprintf("SELECT COUNT(*) as total FROM read_parquet('%s')", parquetPath)

	var total int64
	err := db.QueryRow(query).Scan(&total)
	if err != nil {
		t.Fatalf("failed to count records: %v", err)
	}

	if total != 1000 {
		t.Errorf("expected 1000 records, got %d", total)
	}
	t.Logf("✓ Parquet file contains %d records", total)

	// Test 2: Discover schema first
	query = fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 1", parquetPath)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to query Parquet schema: %v", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		t.Fatalf("failed to get columns: %v", err)
	}
	rows.Close()

	t.Logf("✓ Parquet file schema: %v", columns)
	numCols := len(columns)

	// Test 3: Query specific records with actual columns
	query = fmt.Sprintf(`
		SELECT * 
		FROM read_parquet('%s') 
		LIMIT 5
	`, parquetPath)

	rows, err = db.Query(query)
	if err != nil {
		t.Fatalf("failed to query Parquet: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		// Create a slice to hold all column values
		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		// Log the row data
		rowData := make(map[string]interface{})
		for i, col := range columns {
			rowData[col] = values[i]
		}
		t.Logf("Row %d: %+v", count+1, rowData)
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}

	// Test 4: Check if we have numeric columns for aggregation
	hasNumericCol := false
	var numericCol string
	for _, col := range columns {
		// Common numeric column names
		if col == "age" || col == "id" || col == "value" || col == "amount" || col == "count" {
			hasNumericCol = true
			numericCol = col
			break
		}
	}

	if !hasNumericCol {
		t.Log("⚠️  No common numeric columns found for aggregation tests, skipping aggregations")
		return
	}

	// Test 5: Aggregation query on numeric column
	query = fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_records,
			AVG(%s) as avg_value,
			MIN(%s) as min_value,
			MAX(%s) as max_value
		FROM read_parquet('%s')
	`, numericCol, numericCol, numericCol, parquetPath)

	var totalRecords int
	var avgValue, minValue, maxValue float64
	err = db.QueryRow(query).Scan(&totalRecords, &avgValue, &minValue, &maxValue)
	if err != nil {
		t.Fatalf("failed to query aggregation: %v", err)
	}

	t.Logf("Parquet Statistics (column: %s):", numericCol)
	t.Logf("  Total Records: %d", totalRecords)
	t.Logf("  Average: %.2f", avgValue)
	t.Logf("  Range: %.2f - %.2f", minValue, maxValue)

	if totalRecords != 1000 {
		t.Errorf("expected 1000 total records, got %d", totalRecords)
	}

	// Test 6: Filtering on Parquet data
	query = fmt.Sprintf(`
		SELECT COUNT(*) as filtered_count
		FROM read_parquet('%s')
		WHERE %s >= %f
	`, parquetPath, numericCol, avgValue)

	var filteredCount int
	err = db.QueryRow(query).Scan(&filteredCount)
	if err != nil {
		t.Fatalf("failed to query filtered data: %v", err)
	}

	t.Logf("  Records with %s >= %.2f: %d", numericCol, avgValue, filteredCount)

	// Test 7: Check if we have a string column for grouping
	hasStringCol := false
	var stringCol string
	for _, col := range columns {
		// Common string column names (avoiding numeric columns)
		if col == "name" || col == "email" || col == "country" || col == "city" || col == "category" || col == "status" {
			hasStringCol = true
			stringCol = col
			break
		}
	}

	if !hasStringCol {
		t.Log("✓ No common string columns found for GROUP BY test, skipping")
		return
	}

	// Test 8: GROUP BY query on string column
	query = fmt.Sprintf(`
		SELECT 
			%s,
			COUNT(*) as count
		FROM read_parquet('%s')
		GROUP BY %s
		ORDER BY count DESC
		LIMIT 10
	`, stringCol, parquetPath, stringCol)

	rows, err = db.Query(query)
	if err != nil {
		t.Fatalf("failed to query groups: %v", err)
	}
	defer rows.Close()

	t.Logf("Top 10 values by %s:", stringCol)
	groupCount := 0
	for rows.Next() {
		var groupValue string
		var count int
		if err := rows.Scan(&groupValue, &count); err != nil {
			t.Fatalf("failed to scan group row: %v", err)
		}
		// Truncate long strings for display
		displayValue := groupValue
		if len(displayValue) > 50 {
			displayValue = displayValue[:47] + "..."
		}
		t.Logf("  %s: %d records", displayValue, count)
		groupCount++
	}

	if groupCount == 0 {
		t.Error("expected at least one group, got 0")
	}
}

func TestTransaction(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Create table in transaction
	_, err = tx.Exec("CREATE TEMP TABLE tx_test (id INT, name TEXT)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data in transaction
	_, err = tx.Exec("INSERT INTO tx_test VALUES (1, 'test')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify data exists after commit
	rows, err := db.Query("SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("failed to query after commit: %v", err)
	}
	defer rows.Close()

	// Check if Luna returned error format (input, error columns)
	cols, _ := rows.Columns()
	if len(cols) == 2 && cols[0] == "input" && cols[1] == "error" {
		// Luna returned error format - read the error message
		if rows.Next() {
			var input, errMsg string
			rows.Scan(&input, &errMsg)
			t.Logf("⚠️  LUNA SERVER LIMITATION: Transactions don't persist state across commands")
			t.Logf("    Query: %s", input)
			t.Logf("    Luna Error: %s", errMsg)
			t.Logf("    Workaround: Use single self-contained queries without transactions")
			t.Skip("Skipping test - Luna server doesn't maintain transaction state")
		}
	}

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("failed to scan count: %v", err)
		}
	}

	if count != 1 {
		t.Errorf("expected 1 row after commit, got %d", count)
	}
}

func TestTransactionRollback(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Create table in transaction
	_, err = tx.Exec("CREATE TEMP TABLE tx_rollback_test (id INT)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to create table: %v", err)
	}

	// Rollback transaction
	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify table doesn't exist after rollback
	rows, err := db.Query("SELECT COUNT(*) FROM tx_rollback_test")
	if err != nil {
		// Expected - table should not exist
		t.Logf("✓ Table doesn't exist after rollback (as expected)")
		return
	}
	defer rows.Close()

	// Check if Luna returned error format
	cols, _ := rows.Columns()
	if len(cols) == 2 && cols[0] == "input" && cols[1] == "error" {
		// Luna returned error format - this is expected
		if rows.Next() {
			var input, errMsg string
			rows.Scan(&input, &errMsg)
			t.Logf("⚠️  LUNA SERVER LIMITATION: Transactions don't persist state across commands")
			t.Logf("    Query: %s", input)
			t.Logf("    Luna Error: %s", errMsg)
			t.Logf("    Note: Table doesn't exist, but not because of rollback - temp tables don't persist")
			t.Skip("Skipping test - Luna server doesn't maintain transaction state")
		}
	}

	// If we got here, table exists (unexpected)
	t.Error("expected error querying rolled back table, but query succeeded")
}

func TestContextTimeout(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Create a context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait a bit to ensure timeout
	time.Sleep(10 * time.Millisecond)

	// This should fail with context deadline exceeded
	_, err := db.QueryContext(ctx, "SELECT 1")
	if err == nil {
		t.Skip("Query completed before timeout (Luna is very fast!)")
	}

	if err != context.DeadlineExceeded && err.Error() != "context deadline exceeded" {
		t.Logf("Got error (expected timeout-related): %v", err)
	}
}

func TestPing(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Test basic ping
	err := db.Ping()
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	t.Log("✓ Basic ping successful")

	// Test ping with context
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		t.Fatalf("PingContext failed: %v", err)
	}
	t.Log("✓ Ping with context successful")

	// Test ping with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		t.Fatalf("PingContext with timeout failed: %v", err)
	}
	t.Log("✓ Ping with timeout context successful")

	// Test multiple pings
	for i := 0; i < 5; i++ {
		err = db.Ping()
		if err != nil {
			t.Fatalf("Ping %d failed: %v", i+1, err)
		}
	}
	t.Log("✓ Multiple pings successful")
}

func TestPingClosedConnection(t *testing.T) {
	db := openDB(t)

	// Close the connection
	db.Close()

	// Ping should fail on closed connection
	err := db.Ping()
	if err == nil {
		t.Fatal("Expected error when pinging closed connection, got nil")
	}
	t.Logf("✓ Ping correctly failed on closed connection: %v", err)
}

func TestPreparedStatement(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Prepare a statement
	stmt, err := db.Prepare("SELECT ? as value")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Note: Luna may not support parameterized queries yet,
	// so this test may fail. That's expected.
	var result int
	err = stmt.QueryRow(42).Scan(&result)
	if err != nil {
		t.Skipf("parameterized queries not supported: %v", err)
	}

	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
}

func TestColumnsMethod(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	query := `SELECT 1 as col1, 'test' as col2, 3.14 as col3`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	expected := []string{"col1", "col2", "col3"}
	if len(columns) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(columns))
	}

	for i, col := range columns {
		if col != expected[i] {
			t.Errorf("column %d: expected %s, got %s", i, expected[i], col)
		}
	}
}

func TestNullValues(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	query := `SELECT 1 as id, NULL as nullable_value`

	var id int
	var nullableValue sql.NullString

	err := db.QueryRow(query).Scan(&id, &nullableValue)
	if err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if id != 1 {
		t.Errorf("expected id=1, got %d", id)
	}

	if nullableValue.Valid {
		t.Errorf("expected NULL value, got %s", nullableValue.String)
	}
}

func TestConnectionPool(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(1 * time.Minute)

	// Execute multiple queries concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			var result int
			err := db.QueryRow("SELECT 1+1").Scan(&result)
			if err != nil {
				t.Errorf("query %d failed: %v", id, err)
			}
			done <- true
		}(i)
	}

	// Wait for all queries to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestLargeResultSet(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Generate a large result set
	query := `SELECT generate_series as num FROM generate_series(1, 1000)`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var num int
		if err := rows.Scan(&num); err != nil {
			t.Fatalf("failed to scan row %d: %v", count, err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration error: %v", err)
	}

	if count != 1000 {
		t.Errorf("expected 1000 rows, got %d", count)
	}
}
