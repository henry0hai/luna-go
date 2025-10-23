package luna

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// Protocol constants
const (
	cmdQuery   = "q:" // Query command (SELECT)
	cmdExecute = "x:" // Execute command (DDL/DML)
)

// sendCommand sends a command to Luna using RESP bulk string format
// Format: $<length>\r\n<data>\r\n
func sendCommand(conn net.Conn, cmd string, sql string) error {
	message := cmd + sql
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)

	_, err := conn.Write([]byte(resp))
	return err
}

// readResponse reads and parses the response from Luna
// Returns the response type and data
// Uses the provided buffered reader to maintain read position across calls
func readResponse(reader *bufio.Reader) (string, []byte, error) {
	// Read the first byte to determine response type
	firstByte, err := reader.ReadByte()
	if err != nil {
		return "", nil, err
	}

	switch firstByte {
	case 0xFF: // Arrow IPC continuation marker (0xFFFFFFFF)
		// Luna sends Arrow IPC format directly
		// Read the remaining 3 bytes of the continuation marker
		marker := make([]byte, 3)
		_, err := io.ReadFull(reader, marker)
		if err != nil {
			return "", nil, fmt.Errorf("failed to read continuation marker: %w", err)
		}

		// Verify it's all 0xFF
		if marker[0] != 0xFF || marker[1] != 0xFF || marker[2] != 0xFF {
			return "", nil, fmt.Errorf("invalid continuation marker: %X %X %X", marker[0], marker[1], marker[2])
		}

		// For Arrow IPC, we return a special marker
		// The actual parsing will be done by passing the reader to parseArrowIPCFromReader
		// Store the continuation marker to prepend it later
		return "arrow-stream", []byte{0xFF, 0xFF, 0xFF, 0xFF}, nil

	case '$': // Bulk string (RESP format - error messages might use this)
		// Read length
		lengthStr, err := reader.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		lengthStr = strings.TrimSpace(lengthStr)
		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			return "", nil, fmt.Errorf("invalid length: %s", lengthStr)
		}

		if length == -1 {
			return "null", nil, nil
		}

		// Read data
		data := make([]byte, length)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return "", nil, err
		}

		// Read trailing \r\n
		reader.ReadByte() // \r
		reader.ReadByte() // \n

		return "bulk", data, nil

	case '+': // Simple string (OK response)
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		return "ok", []byte(strings.TrimSpace(line)), nil

	case '-': // Error
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		return "error", []byte(strings.TrimSpace(line)), nil

	case ':': // Integer
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		return "int", []byte(strings.TrimSpace(line)), nil

	default:
		return "", nil, fmt.Errorf("unknown response type: %c", firstByte)
	}
}

// parseArrowIPC parses Arrow IPC format data and returns records
func parseArrowIPC(data []byte) ([]arrow.Record, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Use default allocator instead of nil
	reader, err := ipc.NewReader(
		&bytesReader{data: data},
		ipc.WithAllocator(memory.NewGoAllocator()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain() // Keep the record alive
		records = append(records, rec)
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("error reading IPC records: %w", err)
	}

	return records, nil
}

// bytesReader wraps a byte slice to implement io.Reader
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// parseArrowIPCFromConn reads Arrow IPC data directly from a buffered reader
func parseArrowIPCFromReader(reader *bufio.Reader) ([]arrow.Record, error) {
	// The reader is positioned right after the continuation marker
	// We need to prepend the marker for the Arrow IPC reader

	// Use io.MultiReader to prepend the continuation marker
	continuationReader := &bytesReader{data: []byte{0xFF, 0xFF, 0xFF, 0xFF}}
	combinedReader := io.MultiReader(continuationReader, reader)

	// Use Arrow IPC library to read directly from the stream
	ipcReader, err := ipc.NewReader(combinedReader, ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer ipcReader.Release()

	// Read all records from the stream
	var records []arrow.Record
	for ipcReader.Next() {
		rec := ipcReader.Record()
		rec.Retain() // Keep the record alive after reader is released
		records = append(records, rec)
	}

	if err := ipcReader.Err(); err != nil {
		return nil, fmt.Errorf("error reading IPC records: %w", err)
	}

	return records, nil
}
