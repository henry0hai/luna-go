package luna

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// authenticate performs Luna's challenge-response authentication
func authenticate(conn net.Conn, password string) error {
	if password == "" {
		// No authentication required
		return nil
	}
	
	reader := bufio.NewReader(conn)
	
	// Read challenge from server
	// Expected format: "+<challenge>\r\n"
	firstByte, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read auth challenge: %w", err)
	}
	
	if firstByte != '+' {
		// No authentication required or error
		if firstByte == '-' {
			errMsg, _ := reader.ReadString('\n')
			return fmt.Errorf("auth error: %s", strings.TrimSpace(errMsg))
		}
		// Put the byte back and continue without auth
		return nil
	}
	
	challenge, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read challenge: %w", err)
	}
	challenge = strings.TrimSpace(challenge)
	
	// Hash the password with bcrypt
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}
	
	// Send authentication response
	authResp := fmt.Sprintf("$%d\r\n%s\r\n", len(hashedPassword), string(hashedPassword))
	_, err = conn.Write([]byte(authResp))
	if err != nil {
		return fmt.Errorf("failed to send auth response: %w", err)
	}
	
	// Read auth result
	resultByte, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read auth result: %w", err)
	}
	
	switch resultByte {
	case '+': // Success
		_, _ = reader.ReadString('\n')
		return nil
	case '-': // Error
		errMsg, _ := reader.ReadString('\n')
		return fmt.Errorf("authentication failed: %s", strings.TrimSpace(errMsg))
	default:
		return fmt.Errorf("unexpected auth response: %c", resultByte)
	}
}
