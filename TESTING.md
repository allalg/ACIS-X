# ACIS-X Testing Guide

This document explains how to run tests locally and in CI/CD pipelines.

---

## Quick Start

###  Run All Unit Tests (No Kafka Required)

```bash
# Install test dependencies
pip install -r requirements.txt

# Run unit tests only (no Kafka broker needed)
python -m pytest tests/ -m unit -v
```

### Run Integration Tests (Kafka Required)

```bash
# Start Kafka broker (docker-compose)
docker-compose up -d kafka

# Run integration tests
python -m pytest tests/ -m integration -v

# Stop Kafka
docker-compose down
```

### Run Everything

```bash
python -m pytest tests/ -v
```

---

## Test Organization

```
tests/
├── __init__.py
├── test_unit_schema.py          # Unit: Database schema & queries
├── test_unit_architecture_fixes.py  # Unit: Phase 1 & 2 fixes
└── test_overload.py (integration)   # Integration: Requires Kafka

conftest.py                      # Pytest fixtures & configuration
pytest.ini                       # Pytest settings
```

---

## Test Categories

### Unit Tests (Mark: `@pytest.mark.unit`)

**What**: Tests that don't require a running Kafka broker or external services
**Where**: `tests/test_unit_*.py`
**Run**: `pytest -m unit`
**Speed**: Fast (< 1 second each)
**Use For**: Local development, CI fast feedback loop

**Examples**:
- Database schema validation
- Data contract verification
- Architecture fix verification
- Mocked agent logic

### Integration Tests (Mark: `@pytest.mark.integration`)

**What**: Tests that require Kafka broker, external APIs, or full system
**Where**: `tests/test_*integration*.py` and `tests/test_overload.py`
**Run**: `pytest -m integration` (requires Kafka running)
**Speed**: Slow (5+ seconds each)
**Use For**: Pre-deployment validation, nightly CI runs

**Examples**:
- Consumer group distribution
- Kafka offset tracking
- End-to-end event flow
- Agent overload detection

---

## Running Specific Tests

### By Category

```bash
# Unit tests only
pytest -m unit

# Integration tests only
pytest -m integration

# Schema tests
pytest -m schema

# Slow tests
pytest -m slow
```

### By File

```bash
# Single test file
pytest tests/test_unit_schema.py

# Single test function
pytest tests/test_unit_schema.py::test_schema_creation

# Pattern matching
pytest tests/ -k "enrichment"
```

### With Coverage

```bash
# Generate coverage report
pytest --cov=agents --cov=runtime --cov=schemas tests/

# HTML coverage report
pytest --cov=agents --cov=runtime --cov=schemas \
       --cov-report=html tests/
# Open htmlcov/index.html
```

---

## Setting Up Local Testing

### 1. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate  # Windows
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Verify pytest Works

```bash
pytest --version
pytest tests/ --collect-only  # List all tests
```

### 4. Run Unit Tests (No Kafka Setup)

```bash
pytest tests/ -m unit -v
```

---

## Kafka for Integration Tests

### Option 1: Docker Compose

```bash
# Start Kafka (requires docker)
docker-compose up -d kafka zookeeper

# Run integration tests
pytest tests/ -m integration -v

# Clean up
docker-compose down
```

### Option 2: Local Kafka

```bash
# Install Kafka locally from https://kafka.apache.org/
# Start broker on default port 9092
bin/kafka-server-start.sh config/server.properties

# Run tests
pytest tests/ -m integration -v
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: pytest tests/ -m unit -v

  integration-tests:
    runs-on: ubuntu-latest
    services:
      kafka:
        image: confluentinc/cp-kafka:7.5.0
        env:
          KAFKA_BROKER_ID: 1
          ...
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
      - run: pip install -r requirements.txt
      - run: pytest tests/ -m integration -v
```

---

## Fixtures

Common fixtures provided in `conftest.py`:

```python
# Mocked Kafka client (no broker needed)
def test_something(mock_kafka_client):
    mock_kafka_client.publish(...)

# Temporary database for testing
def test_db(db_agent, query_agent):
    query_agent.get_customer("id")

# Sample events
def test_event(sample_customer_event):
    assert event["event_type"] == "customer.profile.created"
```

---

## Writing Tests

### Unit Test Example

```python
import pytest

@pytest.mark.unit
def test_query_agent_returns_name(query_agent):
    """Test that QueryAgent includes name field."""
    customer = query_agent.get_customer("cust_001")
    assert "name" in customer, "QueryAgent should return company name"
    assert customer["name"] == "ACME Corp"
```

### Integration Test Example

```python
import pytest

@pytest.mark.integration
def test_kafka_consumer_group_distribution(mock_kafka_client):
    """Test that Kafka distributes partitions among replicas."""
    # Test with live Kafka
    client = KafkaClient(...)  # Real connection
    client.subscribe(["acis.metrics"], "canonical-group-id")
    # Verify partition distribution
```

---

## Troubleshooting

### ImportError: No module named 'pytest'

```bash
pip install pytest pytest-mock
```

### Kafka connection refused (integration tests)

```bash
# Start Kafka or run with -m unit only
docker-compose up -d kafka
pytest tests/ -m integration
```

### Tests hang or timeout

```bash
# Run with timeout
pytest tests/ --timeout=10 -v
```

###  Database locked error

```bash
# Close other connections to acis.db
rm acis.db acis.db-wal acis.db-shm
pytest tests/
```

---

## Test Development Workflow

1. **Write test in `tests/test_unit_*.py`**
   - Use fixture `mock_kafka_client` for Kafka mocks
   - Use fixture `db_agent`, `query_agent` for database tests
   - Mark with `@pytest.mark.unit`

2. **Run test locally**
   ```bash
   pytest tests/test_unit_yourtest.py::test_name -v
   ```

3. **Add to suite**
   - Tests auto-discovered by pytest
   - Markers auto-applied based on module name

4. **Verify it passes**
   ```bash
   pytest tests/ -m unit -v
   ```

---

## Success Criteria

System is ready when:

- [x] `pytest -m unit` passes (all unit tests)
- [x] `pytest --collect-only` finds all tests
- [x] No import errors
- [x] No hardcoded Kafka connections in unit tests
- [x] Coverage > 70% for critical paths (agents, runtime)

---

## Next Steps

1. Add integration test suite
2. Add CI/CD pipeline
3. Achieve 80% code coverage
4. Add performance benchmarks

