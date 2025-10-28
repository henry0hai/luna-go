package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/flowerinthenight/luna-go"
)

func main() {
	// Example 1: Basic Connection
	fmt.Println("=== Example 1: Basic Connection ===")
	basicExample()

	// Example 2: Querying Data
	fmt.Println("\n=== Example 2: Querying Data ===")
	queryExample()

	// Example 3: Creating and Inserting Data
	fmt.Println("\n=== Example 3: Creating and Inserting Data ===")
	insertExample()

	// Example 4: Transactions
	fmt.Println("\n=== Example 4: Transactions ===")
	transactionExample()

	// Example 5: Cloud Storage Query
	fmt.Println("\n=== Example 5: Cloud Storage Query ===")
	cloudStorageExample()

	// Example 6: Context with Timeout
	fmt.Println("\n=== Example 6: Context with Timeout ===")
	contextExample()
}

func basicExample() {
	// Connect to Luna server
	db, err := sql.Open("luna", "localhost:7688")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Printf("Luna server not available: %v", err)
		return
	}

	fmt.Println("Successfully connected to Luna server!")

	// Simple query
	var result int
	err = db.QueryRow("SELECT 1+1").Scan(&result)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}
	fmt.Printf("Query result: 1+1 = %d\n", result)
}

func queryExample() {
	db, err := sql.Open("luna", "localhost:7688")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query with multiple columns
	query := `
		SELECT 
			1 as id,
			'Henry' as name,
			'henry@example.com' as email
		UNION ALL
		SELECT 
			2 as id,
			'Ton' as name,
			'ton@example.com' as email
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Users:")
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			log.Printf("Scan error: %v", err)
			return
		}
		fmt.Printf("  %d: %s <%s>\n", id, name, email)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Rows error: %v", err)
	}
}

func insertExample() {
	db, err := sql.Open("luna", "localhost:7688")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a temporary table
	_, err = db.Exec(`
		CREATE TEMP TABLE products (
			id INTEGER,
			name VARCHAR,
			price DECIMAL(10,2),
			category VARCHAR
		)
	`)
	if err != nil {
		log.Printf("Create table error: %v", err)
		return
	}
	fmt.Println("Table created successfully")

	// Insert data
	_, err = db.Exec(`
		INSERT INTO products VALUES 
			(1, 'Laptop', 999.99, 'Electronics'),
			(2, 'Mouse', 29.99, 'Electronics'),
			(3, 'Desk', 299.99, 'Furniture')
	`)
	if err != nil {
		log.Printf("Insert error: %v", err)
		return
	}
	fmt.Println("Data inserted successfully")

	// Query the data
	rows, err := db.Query("SELECT * FROM products ORDER BY price DESC")
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Products:")
	for rows.Next() {
		var id int
		var name, category string
		var price float64
		if err := rows.Scan(&id, &name, &price, &category); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		fmt.Printf("  %d: %s - $%.2f (%s)\n", id, name, price, category)
	}
}

func transactionExample() {
	db, err := sql.Open("luna", "localhost:7688")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Begin transaction error: %v", err)
		return
	}

	// Create table in transaction
	_, err = tx.Exec(`
		CREATE TEMP TABLE accounts (
			id INTEGER,
			name VARCHAR,
			balance DECIMAL(10,2)
		)
	`)
	if err != nil {
		tx.Rollback()
		log.Printf("Create table error: %v", err)
		return
	}

	// Insert data in transaction
	_, err = tx.Exec(`
		INSERT INTO accounts VALUES 
			(1, 'Alice', 1000.00),
			(2, 'Bob', 500.00)
	`)
	if err != nil {
		tx.Rollback()
		log.Printf("Insert error: %v", err)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Commit error: %v", err)
		return
	}

	fmt.Println("Transaction committed successfully")

	// Verify data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM accounts").Scan(&count)
	if err != nil {
		log.Printf("Count error: %v", err)
		return
	}
	fmt.Printf("Number of accounts: %d\n", count)
}

func cloudStorageExample() {
	db, err := sql.Open("luna", "localhost:7688")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Example: Reading from a local CSV file
	// In production, this could be S3, GCS, etc.
	query := `
		SELECT 
			COUNT(*) as total_records,
			COUNT(DISTINCT customer_id) as unique_customers
		FROM read_csv('tests/customers-1000.csv', header=true)
	`

	var totalRecords, uniqueCustomers int
	err = db.QueryRow(query).Scan(&totalRecords, &uniqueCustomers)
	if err != nil {
		log.Printf("Query error: %v (Make sure the CSV file exists)", err)
		return
	}

	fmt.Printf("Total records: %d\n", totalRecords)
	fmt.Printf("Unique customers: %d\n", uniqueCustomers)
}

func contextExample() {
	db, err := sql.Open("luna", "localhost:7688")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute query with context
	query := `
		SELECT 
			generate_series as num,
			num * num as square
		FROM generate_series(1, 10)
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Numbers and their squares:")
	for rows.Next() {
		var num, square int
		if err := rows.Scan(&num, &square); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		fmt.Printf("  %d² = %d\n", num, square)
	}
}
