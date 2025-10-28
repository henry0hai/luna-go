#!/bin/bash

# Test runner script for Luna Go driver

set -e

echo "=========================================="
echo "Luna Go Driver - Test Runner"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if Luna server is running
echo "📡 Checking Luna server..."
if nc -z localhost 7688 2>/dev/null || timeout 1 bash -c 'cat < /dev/null > /dev/tcp/localhost/7688' 2>/dev/null; then
    echo -e "${GREEN}✓ Luna server is running${NC}"
    LUNA_RUNNING=true
else
    echo -e "${YELLOW}⚠ Luna server not detected on localhost:7688${NC}"
    echo "  Integration tests will be skipped."
    echo "  To run full tests, start Luna server:"
    echo "  ./luna --api-host-port 0.0.0.0:7688"
    LUNA_RUNNING=false
fi
echo ""

# Run unit tests (no server required)
echo "🧪 Running unit tests (no server required)..."
echo "=========================================="
go test -v -run "^TestDriver|^TestConnector|^TestResult|^TestArgs" 2>&1 | grep -E "^(===|---|\s+driver_test)" || true
echo ""

if [ "$LUNA_RUNNING" = true ]; then
    # Run integration tests
    echo "🚀 Running integration tests..."
    echo "=========================================="
    go test -v -count=1 ./... 2>&1
    echo ""
    
    echo -e "${GREEN}✓ All tests completed!${NC}"
else
    echo -e "${YELLOW}[i] Integration tests skipped (Luna server not running)${NC}"
    echo ""
    echo "To run full test suite:"
    echo "1. Start Luna server: ./luna --api-host-port 0.0.0.0:7688"
    echo "2. Run: go test -v -count=1 ./..."
fi

echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo "Unit tests: ✓ Completed"
if [ "$LUNA_RUNNING" = true ]; then
    echo "Integration tests: ✓ Completed"
else
    echo "Integration tests: ⊘ Skipped"
fi
echo ""
