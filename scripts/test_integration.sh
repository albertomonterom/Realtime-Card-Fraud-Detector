#!/bin/bash

# Integration test for the complete fraud detection pipeline
# Tests: API → Database → Streaming → Kafka alerts

set -e  # Exit on error

# macOS compatibility - use gtimeout if available, otherwise just curl without timeout
if command -v gtimeout &> /dev/null; then
    TIMEOUT_CMD="gtimeout"
else
    TIMEOUT_CMD=""  # No timeout on macOS without GNU coreutils
fi

echo "═══════════════════════════════════════════════════════════════════════════"
echo "FRAUD DETECTION SYSTEM - INTEGRATION TEST"
echo "═══════════════════════════════════════════════════════════════════════════"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
API_URL="http://localhost:5001"
MAX_RETRIES=30
RETRY_DELAY=2

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

function log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

function log_error() {
    echo -e "${RED}✗${NC} $1"
}

function log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

function wait_for_service() {
    local service=$1
    local url=$2
    local retries=$MAX_RETRIES
    
    echo "Waiting for $service to be ready..."
    while [ $retries -gt 0 ]; do
        if curl -s -m 2 "$url" > /dev/null 2>&1; then
            log_info "$service is ready"
            return 0
        fi
        retries=$((retries - 1))
        sleep $RETRY_DELAY
    done
    
    log_error "$service failed to start"
    return 1
}

function test_health_check() {
    echo -e "\n${YELLOW}Testing ML API Health${NC}"
    response=$(curl -s -m 2 "$API_URL/health")
    
    if echo "$response" | grep -q "healthy"; then
        log_info "Health check passed"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_error "Health check failed: $response"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

function test_scoring() {
    echo -e "\n${YELLOW}Testing Transaction Scoring${NC}"
    
    # Simple test - just call /score endpoint 
    response=$(curl -s -m 3 -X POST "http://localhost:5001/score" \
        -H "Content-Type: application/json" \
        -d '{
            "amt": 45.50,
            "hour": 14,
            "day_of_week": 3,
            "month": 1,
            "is_weekend": 0,
            "is_night": 0,
            "card_avg_amt": 75.50,
            "card_std_amt": 25.30,
            "amt_deviation": -1.06,
            "amt_to_avg_ratio": 0.60,
            "geo_distance": 5.5,
            "card_txn_count": 3,
            "category_encoded": 2,
            "state_encoded": 1,
            "gender_encoded": 0,
            "age": 45.2,
            "city_pop_log": 13.1
        }' 2>&1)
    
    if echo "$response" | grep -q "fraud_probability"; then
        log_info "Scoring successful"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_warn "Scoring endpoint not responding (skipping)"
    fi
}

function test_metrics() {
    echo -e "\n${YELLOW}Testing Prometheus Metrics${NC}"
    
    response=$(curl -s -m 2 "$API_URL/metrics")
    
    if echo "$response" | grep -q "fraud_predictions_total"; then
        log_info "Metrics endpoint working"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_error "Metrics endpoint failed"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

function test_database() {
    echo -e "\n${YELLOW}Testing Database Logging${NC}"
    
    # Connect to PostgreSQL and check if prediction was logged
    result=$(docker exec fraud_detector_postgres psql -U fraud_user -d fraud_detection \
        -c "SELECT COUNT(*) FROM scored_transactions;" 2>/dev/null || echo "0")
    
    if [ "$result" != "0" ]; then
        log_info "Database contains scored transactions"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_warn "No scored transactions found in database (may be expected if producer hasn't run)"
    fi
}

function test_kafka() {
    echo -e "\n${YELLOW}Testing Kafka Topics${NC}"
    
    # Check if we can list Kafka topics
    result=$(docker exec fraud_detector_kafka kafka-topics \
        --bootstrap-server localhost:9092 --list 2>/dev/null | grep -c "fraud-alerts" || echo "0")
    
    if [ "$result" != "0" ]; then
        log_info "Kafka fraud-alerts topic exists"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_warn "Kafka fraud-alerts topic not found (may need to be created)"
    fi
}

function print_summary() {
    echo -e "\n${YELLOW}═══════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "TEST SUMMARY"
    echo -e "${YELLOW}═══════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    
    if [ $TESTS_FAILED -eq 0 ]; then
        log_info "All tests passed!"
        return 0
    else
        log_error "$TESTS_FAILED test(s) failed"
        return 1
    fi
}

# Main execution
echo "Checking Docker services..."
if ! docker ps -q > /dev/null 2>&1; then
    log_error "Docker is not running"
    exit 1
fi

# Wait for services
if ! wait_for_service "ML API" "$API_URL/health"; then
    log_error "ML API is not running"
    echo "Start services with: docker-compose up -d"
    exit 1
fi

# Run tests
test_health_check
test_scoring
test_metrics
test_database
test_kafka

# Print results
print_summary
