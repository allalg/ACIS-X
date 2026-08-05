# ACIS-X: Intelligent Collections System

**Status**: Production Ready ✅ | **Latest**: Phase 1 & 2 Complete

---

## Overview

ACIS-X monitors payments, enriches customer data with external signals (financial + litigation risk), predicts defaults, and routes to collections.

**Key Fixes (Sessions 11-15)**:
- ✅ Real company names in all agents (not IDs)
- ✅ Horizontal scaling: 3x throughput (canonical consumer groups)
- ✅ 95% less startup overhead (lazy producer init)
- ✅ Lock contention eliminated
- ✅ Complete test coverage (pytest, no Kafka needed)

**Phase 4 (Research & ML)**:
- ✅ Dynamic Discovery Protocol for automated agent registration
- ✅ SHAP-enabled RandomForest model for payment prediction
- ✅ Complete academic research paper generation

---

## Quick Start

```bash
# 1. Setup
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env       # Configure your API keys in .env

# 2. Fresh start (clean database)
rm -f .acis_consumer_groups_initialized acis.db acis.db-wal acis.db-shm

# 3. Start system
python run_acis.py

# 4. Run tests (no Kafka needed)
python -m pytest tests/ -m unit -v
```

---

## Running Locally

ACIS-X uses Kafka as its event bus. The quickest way to get a local broker
is via Docker Desktop and the bundled `docker-compose.yml`.

### 1. Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and **running**
- Python 3.9+ with a virtual environment activated
- All dependencies: `pip install -r requirements.txt`

### 2. Start Kafka or the Full Application Stack

You can run the system in two ways:

#### Option A: Run Full Application inside Docker Containers (Recommended)
This starts all components (Kafka cluster, Python agent engine, BFF gateway, and Frontend dashboard) in isolated, production-ready containers:
```bash
# Launch everything in the background
docker-compose up --build -d

# To view live logs of the engine:
docker-compose logs -f acis-engine

# To shut down everything:
docker-compose down
```

#### Option B: Run Kafka in Docker + Python Agents on Host
Useful for interactive debugging of agents in your local terminal:
```bash
# 1. Start only the Kafka cluster in the background
docker-compose up -d kafka1 kafka2 kafka3 kafka-ui

# 2. Run ACIS-X Agent Engine
python run_acis.py

# 3. Stop Kafka later
docker-compose down
```

### 3. Controlling the Scenario Generator (Simulation)

You can pause and resume the live simulation generator without stopping any of the other agents or services:

```bash
# Pause the Scenario Generator spawning cycles
python control_scenario.py pause

# Resume the Scenario Generator spawning cycles
python control_scenario.py resume
```

### 4. Resetting to a Clean State

To wipe all data, reset Kafka partition offsets, and start fresh:

```bash
# 1. Shut down any running containers
docker-compose down

# 2. Re-create pristine topics and wipe database/logs
python reset_acis.py

# 3. Start fresh
docker-compose up --build -d
```

### 5. Run the test suite

Tests are fully offline — no Kafka or running system required:

```bash
# All unit tests
python -m pytest tests/ -m unit -v

# With coverage
python -m pytest tests/ -m unit --cov=agents --cov=runtime -v

# Single module
python -m pytest tests/test_unit_architecture_fixes.py -v
```

### 6. Diagnostic tools

Read-only DB inspection scripts live in `scripts/diagnostics/`.
**Only run these when the system is stopped** to avoid stale reads.

```bash
python scripts/acis_control.py stop
python scripts/diagnostics/analyze_db.py
```

See [`scripts/diagnostics/README.md`](scripts/diagnostics/README.md) for details.

---

## Commands

```bash
# Start in background
python scripts/acis_control.py start

# Stop gracefully
python scripts/acis_control.py stop

# Status
python scripts/acis_control.py status

# Fresh restart (clean database)
python scripts/FINAL_CLEANUP_AND_START.py
```

---

## Documentation

- **[README.md](README.md)** - This file (overview)
- **[CHANGELOG.md](CHANGELOG.md)** - What was fixed (Sessions 11-15)
- **[TESTING.md](TESTING.md)** - How to write and run tests
- **[DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)** - Production deployment

---

## Testing

```bash
# Unit tests (no broker)
python -m pytest tests/ -m unit -v

# With coverage
python -m pytest tests/ -m unit --cov=agents --cov=runtime -v

# Specific test
python -m pytest tests/test_unit_architecture_fixes.py::test_customer_identity_contract -v
```

See [TESTING.md](TESTING.md) for full guide.

---

## System Architecture

```
Data Sources
  ├─ ScenarioGeneratorAgent (test data)
  ├─ CustomerStateAgent (metrics)
  └─ DBAgent (persist)
    ↓
Enrichment (uses REAL company names!)
  ├─ ExternalDataAgent (financial risk from screener.in)
  ├─ ExternalScrapingAgent (litigation risk from Google News)
  └─ PaymentPredictionAgent (default risk from ML)
    ↓
Risk Scoring
  └─ RiskScoringAgent (final score)
    ↓
Actions
  ├─ CollectionsAgent
  └─ RegistryService
```

---

## Key Files

```
ACIS-X/
├── agents/              # Agent implementations (base, storage, intelligence, etc.)
├── runtime/             # Kafka, placement, orchestration
├── tests/              # Pytest tests (unit only, no broker)
│   ├── test_unit_architecture_fixes.py  # Validates Phase 1 & 2
│   └── test_unit_schema.py              # Database tests
├── scripts/            # Operations (control, cleanup)
├── run_acis.py        # Entry point
├── requirements.txt    # Dependencies
├── pytest.ini         # Test config
├── conftest.py        # Test fixtures
├── README.md          # This file
├── CHANGELOG.md       # Session history
├── TESTING.md         # Test guide
└── DEPLOYMENT_GUIDE.md # Deploy guide
```

---

## Troubleshooting

### Consumer group errors
```bash
rm -f .acis_consumer_groups_initialized
python run_acis.py
```

### Database locked
```bash
rm -f acis.db-wal acis.db-shm
python run_acis.py
```

See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for more.

---

## Performance

| Metric | Value |
|--------|-------|
| Startup | < 30s |
| Throughput | 1000+ events/s |
| Memory | < 500MB @ 10k events |
| Connections | ~2 (was ~20) |

---

## Status

- ✅ Phase 1 & 2: Architecture + Performance fixes
- ✅ Phase 3: Testing framework + cleanup
- ✅ Phase 4: Dynamic Discovery + ML (SHAP/RandomForest) + Paper
- ✅ Production ready

See [CHANGELOG.md](CHANGELOG.md) for detailed session history.

---

**Last Updated**: 2026-04-10
