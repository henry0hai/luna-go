[![main](https://github.com/luna-hq/luna-go/actions/workflows/main.yml/badge.svg)](https://github.com/luna-hq/luna-go/actions/workflows/main.yml)

# Luna Go SQL Driver

A Go `database/sql` driver for [Luna](https://github.com/luna-hq/luna) - a high-performance, in-memory columnar SQL server built on DuckDB and Apache Arrow.

## Features

- ✅ Standard Go `database/sql` interface
- ✅ Apache Arrow IPC response parsing
- ✅ Luna RESP protocol support
- ✅ Query execution (SELECT statements)
- ✅ Command execution (DDL/DML statements)
- ⚠️ Transaction support (API available but Luna doesn't persist state)
- ✅ Optional password authentication
- ✅ Connection pooling (via `database/sql`)
- ✅ Context support for cancellation and timeouts

## Installation

```bash
go get github.com/flowerinthenight/luna-go
```

## Quick Start

### Basic Usage

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/flowerinthenight/luna-go"
)

func main() {
    // Connect to Luna server
    db, err := sql.Open("luna", "localhost:7688")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Test connection
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }

    // Query data
    var result int
    err = db.QueryRow("SELECT 1+1").Scan(&result)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Result: %d\n", result) // Output: Result: 2
}
```

### DSN Format

The Data Source Name (DSN) supports several formats:

```go
// Simple host:port
db, _ := sql.Open("luna", "localhost:7688")

// With scheme
db, _ := sql.Open("luna", "luna://localhost:7688")

// With authentication
db, _ := sql.Open("luna", "luna://user:password@localhost:7688")

// Alternative authentication format
db, _ := sql.Open("luna", "user:password@localhost:7688")
```

### Querying Data

```go
// Query single row
var id int
var name string
err := db.QueryRow("SELECT id, name FROM users WHERE id = 1").Scan(&id, &name)

// Query multiple rows
rows, err := db.Query("SELECT id, name, email FROM users")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var name, email string
    if err := rows.Scan(&id, &name, &email); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%d: %s <%s>\n", id, name, email)
}

if err := rows.Err(); err != nil {
    log.Fatal(err)
}
```

### Executing Commands

```go
// Create table
_, err := db.Exec("CREATE TABLE users (id INT, name TEXT, email TEXT)")
if err != nil {
    log.Fatal(err)
}

// Insert data
_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
if err != nil {
    log.Fatal(err)
}

// Update data
result, err := db.Exec("UPDATE users SET email = 'new@example.com' WHERE id = 1")
if err != nil {
    log.Fatal(err)
}
```

### Transactions

⚠️ **Note**: Luna server doesn't maintain session state between commands, so traditional transactions don't work as expected. Each command is executed independently.

```go
// ❌ This won't work - Luna doesn't persist transaction state
tx, err := db.Begin()
_, err = tx.Exec("CREATE TEMP TABLE users (id INT, name TEXT)")
_, err = tx.Exec("INSERT INTO users VALUES (1, 'Alice')")
tx.Commit()
// The table and data won't persist!

// ✅ Instead, use single self-contained queries
_, err := db.Exec(`
    CREATE TABLE users AS
    SELECT * FROM (
        VALUES (1, 'Alice'), (2, 'Bob')
    ) AS t(id, name)
`)

// ✅ Or use CTEs (Common Table Expressions)
_, err := db.Exec(`
    WITH new_users AS (
        SELECT 1 as id, 'Alice' as name
        UNION ALL
        SELECT 2 as id, 'Bob' as name
    )
    INSERT INTO users SELECT * FROM new_users
`)
```

### Context Support

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Query with context
rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Execute with context
_, err = db.ExecContext(ctx, "CREATE TABLE temp AS SELECT * FROM source")
if err != nil {
    log.Fatal(err)
}
```

### Working with Cloud Storage

Luna supports querying data directly from cloud storage:

```go
// Query from S3
rows, err := db.Query(`
    SELECT customer_id, SUM(amount) as total
    FROM read_parquet('s3://my-bucket/data/*.parquet')
    GROUP BY customer_id
`)

// Query from GCS
rows, err := db.Query(`
    SELECT * FROM read_csv('gs://my-bucket/data.csv')
    WHERE date >= '2024-01-01'
`)

// Query from local files
rows, err := db.Query(`
    SELECT * FROM read_json('file:///path/to/data.json')
`)
```

### Prepared Statements

```go
// Prepare statement
stmt, err := db.Prepare("SELECT * FROM users WHERE id = ?")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute prepared statement (Note: Luna may not support parameterized queries yet)
rows, err := stmt.Query(1)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()
```

### Connection Pooling

The driver supports connection pooling through the standard `database/sql` package:

```go
db, err := sql.Open("luna", "localhost:7688")
if err != nil {
    log.Fatal(err)
}

// Configure pool
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(5)
db.SetConnMaxLifetime(5 * time.Minute)
```

## Advanced Examples

### ETL Pipeline

```go
func runETL(db *sql.DB) error {
    // Luna works best with single self-contained queries
    // Extract, transform, and load in one query
    _, err := db.Exec(`
        CREATE TABLE processed_data AS
        SELECT 
            id,
            UPPER(name) as name,
            email,
            DATE_TRUNC('day', created_at) as created_date
        FROM read_csv('s3://source-bucket/raw-data.csv', header=true)
        WHERE status = 'active'
    `)
    if err != nil {
        return err
    }

    // Or use CTEs for complex transformations
    _, err = db.Exec(`
        WITH raw_data AS (
            SELECT * FROM read_csv('s3://source-bucket/raw-data.csv', header=true)
        ),
        cleaned_data AS (
            SELECT 
                id,
                UPPER(TRIM(name)) as name,
                LOWER(TRIM(email)) as email,
                DATE_TRUNC('day', created_at) as created_date
            FROM raw_data
            WHERE status = 'active'
            AND email IS NOT NULL
        )
        INSERT INTO processed_data 
        SELECT * FROM cleaned_data
    `)
    
    return err
}
```

### Analytics Dashboard

```go
type DashboardMetrics struct {
    TotalUsers    int
    ActiveUsers   int
    Revenue       float64
    AvgOrderValue float64
}

func getDashboardMetrics(db *sql.DB) (*DashboardMetrics, error) {
    metrics := &DashboardMetrics{}
    
    query := `
        SELECT 
            COUNT(DISTINCT user_id) as total_users,
            COUNT(DISTINCT CASE WHEN last_active >= NOW() - INTERVAL 7 DAYS 
                  THEN user_id END) as active_users,
            SUM(order_amount) as revenue,
            AVG(order_amount) as avg_order_value
        FROM analytics_data
        WHERE order_date >= NOW() - INTERVAL 30 DAYS
    `
    
    err := db.QueryRow(query).Scan(
        &metrics.TotalUsers,
        &metrics.ActiveUsers,
        &metrics.Revenue,
        &metrics.AvgOrderValue,
    )
    
    return metrics, err
}
```

## Protocol Details

Luna uses:
- **Command Protocol**: Redis RESP (REdis Serialization Protocol) bulk strings
- **Response Format**: Apache Arrow IPC (Inter-Process Communication)

### Commands

- `q:<sql>` - Execute query (SELECT)
- `x:<sql>` - Execute statement (DDL/DML)

### Message Format

```
$<length>\r\n<data>\r\n
```

Example:
```
$13\r\nq:SELECT 1+1\r\n
```

## Limitations

### Luna Server Limitations

- **Session State**: Luna doesn't maintain state between commands
  - ❌ Temporary tables don't persist across queries
  - ❌ Transactions don't work as expected (each command is independent)
  - ✅ Use CTEs or subqueries instead of temp tables
  - ✅ Use single self-contained queries instead of multi-step transactions

### Driver Limitations

- **Parameterized Queries**: Not yet fully supported by Luna server
- **Last Insert ID**: Not supported (returns `driver.ErrSkip`)
- **Multiple Result Sets**: Not currently supported
- **Streaming Large Results**: All results loaded into memory

### Workarounds

```go
// ❌ Don't use temp tables
db.Exec("CREATE TEMP TABLE tmp AS SELECT * FROM users")
db.Query("SELECT * FROM tmp") // Won't work!

// ✅ Use CTEs instead
db.Query(`
    WITH tmp AS (SELECT * FROM users)
    SELECT * FROM tmp
`)

// ❌ Don't use multi-step transactions
tx.Begin()
tx.Exec("INSERT INTO users VALUES (1, 'Alice')")
tx.Exec("INSERT INTO orders VALUES (1, 100)")
tx.Commit() // Won't maintain state!

// ✅ Use single compound queries
db.Exec(`
    INSERT INTO users VALUES (1, 'Alice');
    INSERT INTO orders VALUES (1, 100);
`)
```

## Error Handling

```go
rows, err := db.Query("SELECT * FROM non_existent_table")
if err != nil {
    // Handle Luna errors
    if strings.Contains(err.Error(), "luna error:") {
        log.Printf("Luna server error: %v", err)
    } else if err == driver.ErrBadConn {
        log.Printf("Connection error, retrying...")
    }
    return err
}
```

## Development

### Running Tests

```bash
# Start Luna server
docker run -p 7688:7688 luna:latest

# Using bash script to run all tests
./run_tests.sh

# Run integration tests
go test -v

# Run specific test
go test -v -run TestSimpleQuery
```

### Building

```bash
go build
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

See [LICENSE](LICENSE) file for details.

## See Also

- [Luna Server](https://github.com/luna-hq/luna) - The Luna server implementation
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory data format
- [DuckDB](https://duckdb.org/) - Analytical SQL engine

## Support

For issues and questions:
- GitHub Issues: https://github.com/flowerinthenight/luna-go/issues
- Luna Server Issues: https://github.com/luna-hq/luna/issues
