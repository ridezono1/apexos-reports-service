#!/bin/bash

# Test runner script for weather-reports-service

set -e

echo "=================================="
echo "Weather Reports Service Test Suite"
echo "=================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}Error: pytest is not installed${NC}"
    echo "Install with: pip install pytest pytest-asyncio httpx"
    exit 1
fi

# Parse command line arguments
TEST_PATH="tests/"
VERBOSE=""
MARKERS=""
SHOW_OUTPUT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -s|--show-output)
            SHOW_OUTPUT="-s"
            shift
            ;;
        -m|--marker)
            MARKERS="-m $2"
            shift 2
            ;;
        -f|--file)
            TEST_PATH="tests/$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: ./run_tests.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose          Verbose output"
            echo "  -s, --show-output      Show print statements"
            echo "  -m, --marker MARKER    Run tests with specific marker"
            echo "  -f, --file FILE        Run specific test file"
            echo "  -h, --help             Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run_tests.sh                          # Run all tests"
            echo "  ./run_tests.sh -v                       # Verbose output"
            echo "  ./run_tests.sh -m api                   # Run only API tests"
            echo "  ./run_tests.sh -f test_geocoding_service.py  # Run specific file"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Display test configuration
echo -e "${YELLOW}Test Configuration:${NC}"
echo "Path: $TEST_PATH"
if [ -n "$MARKERS" ]; then
    echo "Markers: $MARKERS"
fi
echo ""

# Run tests
echo -e "${GREEN}Running tests...${NC}"
echo ""

pytest $TEST_PATH $VERBOSE $SHOW_OUTPUT $MARKERS

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ All tests passed!${NC}"
else
    echo ""
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi
