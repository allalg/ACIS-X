# ACIS-X Fresh Start Deployment Guide

**Last Updated**: 2026-04-10
**Status**: Ready for Production Deployment

---

## Pre-Deployment Checklist

Before starting the system:

- [ ] Kafka broker running on `localhost:9092`
- [ ] SQLite (local) or PostgreSQL (configured in agents/)
- [ ] Python 3.9+ with venv activated
- [ ] All dependencies installed: `pip install -r requirements.txt`

---

## Fresh Start Procedure

### Step 1: Clean Kafka Consumer Groups

Consumer group offsets are stored in Kafka. When deploying a new version with architectural changes, reset them:

```bash
# Delete the marker file that tracks consumer group initialization
rm -f .acis_consumer_groups_initialized

# Optional: If using external Kafka management tool
# $ kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group <group-name>
```

**Why**: Old system used unique group IDs per replica (wrong). New system uses canonical group IDs (correct). Starting fresh avoids Kafka rebalancing issues.

### Step 2: Clean Local Database

```bash
# Delete existing database (SQLite)
rm -f acis.db acis.db-wal acis.db-shm

# Or: Clear PostgreSQL database if configured
# $ psql -U acis_user -d acis_db -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

**Why**: Database schema improvements and data model fixes require clean data.

### Step 3: Verify Configuration

Check `config.yaml` or environment variables:

```bash
# Standard config (localhost Kafka, SQLite)
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export DB_BACKEND="sqlite"
export DB_PATH="./acis.db"

# Or custom config
export KAFKA_CLIENT_ID="acis-runtime"
export LOG_LEVEL="INFO"
```

### Step 4: Start the System

```bash
# Activate virtual environment
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate  # Windows

# Start ACIS-X
python run_acis.py
```

**Expected Output**:
```
[INFO] ACIS Runtime: Initializing 18 agents
[INFO] KafkaClient initialized (backend: confluent, client_id: acis-runtime, lazy init enabled)
[INFO] BaseAgent: Subscribing to topics [...] with consumer group: customer-state-group (instance: agent_...)
[INFO] Started 18 agents successfully
[INFO] TimeTickAgent: Starting time tick generator loop
[INFO] All agents online, waiting for messages...
```

---

## Post-Deployment Validation

### 1. Verify Customer Event Processing (First 100 Events)

Monitor logs for:

```
✓ customer.profile.created events flow to DBAgent
✓ Customers appear in database with real company names
✓ customer.metrics.updated events include company_name + credit_limit
✓ No "customer_id" being used as fallback (or rarely)
```

**Check logs**:
```bash
grep "Resolved company_name from DB\|customer_id" acis.log | head -20
```

### 2. Verify Consumer Group Distribution

All replicas of same agent should share one consumer group:

```bash
# List active Kafka consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Expected groups:
# - customer-state-group (not customer-state-group-agent_00001, etc.)
# - db-agent-group
# - external-data-group
# - external-scrapping-group
# - payment-prediction-group
# - risk-scoring-group
# - etc.

# Check group members and lag
kafka-consumer-groups --bootstrap-server localhost:9092 --group customer-state-group --describe
```

**Expected Output**:
```
GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  MEMBER_ID  HOST
customer-state-group acis.metrics    0          100             150             50   member-1   ...
customer-state-group acis.metrics    1          120             150             30   member-2   ...
customer-state-group acis.metrics    2          110             150             40   member-3   ...
```

### 3. Verify Agent Lifecycle (Registry)

All agents should be registered with heartbeats:

```bash
# Check registry (if exposed via API)
curl http://localhost:5000/api/agents

# Expected: 18 agents all with status="RUNNING"
# Including: TimeTickAgent, ScenarioGeneratorAgent
```

### 4. Performance Baseline

Compare against previous version:

```
Metric                          Target          Notes
- Startup time                  < 30s           Startup should be similar (lazy init doesn't break performance)
- Event throughput              > 1000 evt/s    3x improvement with proper consumer groups
- Kafka Connection count        ~1              Shared producer (was ~20)
- Lock contention (CustomerStateAgent)  < 5%  Reduced after lock optimization
- Memory usage @ 10k events     < 500MB         Should be stable
```

---

## Monitoring (Continuous)

### Key Metrics to Track

1. **Consumer Lag** - Should decrease over time
   ```bash
   kafka-consumer-groups ... --describe | grep LAG
   ```

2. **Error Logs** - Should be minimal
   ```bash
   grep "ERROR\|CRITICAL" acis.log | wc -l
   ```

3. **External Agent Fallbacks** - Should decrease as cache warms
   ```bash
   grep "Failed to lookup customer\|using customer_id" acis.log | tail -20
   ```

4. **Registry Stability** - All agents should maintain heartbeat
   ```bash
   curl http://localhost:5000/api/agents | jq '.[] | select(.status != "RUNNING")'
   ```

---

## Common Issues & Fixes

### Issue: "MemberIdRequiredError" or "UnknownMemberIdError"

**Symptom**: Agent crashes with Kafka consumer group error

**Fix**:
```bash
rm -f .acis_consumer_groups_initialized
# Restart agents
```

**Root Cause**: Leftover consumer group state from old version (unique group_ids)

---

### Issue: Consumer Lag Growing / Not Advancing

**Symptom**: `kafka-consumer-groups --describe` shows LAG > 0 and growing

**Fix**:
```bash
# Check if agent is hung
ps aux | grep "python run_acis"

# Kill and restart
pkill -f "python run_acis"
python run_acis.py
```

**Root Cause**: Polling timeout (10s), external API slowness

---

### Issue: Database Corruption / NULL Values

**Symptom**: `company_name` is NULL in customer table

**Fix**:
```bash
rm -f acis.db acis.db-wal acis.db-shm
# Restart - system will repopulate from events
```

**Root Cause**: Old data with broken enrichment

---

### Issue: Memory Growing Unbounded

**Symptom**: Memory usage increases over time, doesn't stabilize

**Check**: Cache cleanup is running
```bash
grep "Cleaned.*cache\|TTL cleanup" acis.log
```

**Fix**: If not appearing, may need to update cache cleanup logic

---

## Data Validation Queries

### Verify Company Names Populated

```sql
-- SQLite
SELECT COUNT(*),
       COUNT(CASE WHEN name IS NULL THEN 1 END) as null_names,
       COUNT(CASE WHEN name LIKE 'cust_%' THEN 1 END) as fallback_names
FROM customers;

-- Expected: null_names and fallback_names should be small (< 5%)
```

### Verify Metrics Enrichment

```sql
SELECT COUNT(*),
       COUNT(CASE WHEN company_name IS NOT NULL THEN 1 END) as enriched,
       COUNT(CASE WHEN credit_limit > 0 THEN 1 END) as credit_set
FROM metrics;

-- Expected: enriched and credit_set > 95% of count
```

### Check Consumer Group Health

```sql
SELECT agent_name,
       COUNT(DISTINCT group_id) as unique_groups,
       COUNT(DISTINCT instance_id) as replica_count
FROM agent_cards
GROUP BY agent_name
ORDER BY agent_name;

-- Expected: Each agent has exactly 1 unique group_id, but N instance_ids
-- Example: customer-state-group with 3 instances
```

---

## Rollback Procedure (If Needed)

If issues occur:

```bash
# Stop current system
pkill -f "python run_acis"

# Revert code
git checkout HEAD~1  # or specific commit

# Clean state
rm -f .acis_consumer_groups_initialized acis.db*

# Restart
python run_acis.py
```

---

## Success Criteria

System is ready for production when:

1. ✅ All 18 agents start without errors
2. ✅ Customer events flow through complete pipeline
3. ✅ Company names populated (not customer_id defaults)
4. ✅ Consumer lag stable or decreasing
5. ✅ No "Kafka.*Error" in logs
6. ✅ Register shows all agents with status=RUNNING
7. ✅ First 100 events processed in < 30 seconds
8. ✅ Memory usage stable (< 500MB for local run)

---

## Contact & Support

For issues contact the platform team. Reference:
- Log files: `acis.log`
- Agent status: Registry API
- Database: `acis.db` (SQLite) or PostgreSQL

---

**Document Version**: 1.0
**Last Deployment**: 2026-04-10
**Architecture Version**: Phase 1 & 2 (Canonical Group IDs + Enriched Metrics)
