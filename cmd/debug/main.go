package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:7688")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to Luna server at localhost:7688")
	fmt.Println("Note: Luna doesn't send anything on connection, it waits for commands")

	// Don't try to read initial response - Luna doesn't send one!
	// Instead, send a command first

	fmt.Println("\nSending command: q:SELECT 1+1")
	cmd := "q:SELECT 1+1"
	message := fmt.Sprintf("$%d\r\n%s\r\n", len(cmd), cmd)
	fmt.Printf("Sending: %q\n", message)
	fmt.Printf("Hex: % X\n", []byte(message))

	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write: %v\n", err)
		os.Exit(1)
	}

	// NOW read the response
	reader := bufio.NewReader(conn)

	// Read response
	fmt.Println("\nReading response...")
	buf := make([]byte, 1000)
	n, err := reader.Read(buf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Received %d bytes:\n", n)
	fmt.Printf("Hex (first 100): % X\n", buf[:min(n, 100)])
	fmt.Printf("String (first 100): %q\n", buf[:min(n, 100)])

	// Try to read more if available
	totalRead := n
	for reader.Buffered() > 0 && totalRead < len(buf) {
		n, err = reader.Read(buf[totalRead:])
		if err != nil && err != io.EOF {
			break
		}
		totalRead += n
	}

	if totalRead > n {
		fmt.Printf("\nTotal received: %d bytes\n", totalRead)
		fmt.Printf("Hex (all): % X\n", buf[:totalRead])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
