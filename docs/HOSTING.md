# ACIS-X Hosting Guide

**Stack:** Vercel (frontend) + Render (BFF) + Confluent Cloud (Kafka) + Supabase (Postgres)  
**Engine host (switchable):** laptop **or** Oracle Always Free ARM

Supabase is the **standard database** in all hosted/demo modes. Laptop vs Oracle only changes where agents run — not where data lives.

## Architecture

```
Browser → Vercel (SPA)
       → Render BFF (:8000)
            ├─ Confluent Cloud Kafka
            └─ Supabase Postgres
Agent engine (laptop OR Oracle)
            ├─ Confluent Cloud Kafka
            └─ same Supabase Postgres
```

## Mode matrix

| Mode | Engine | Kafka | DB | BFF | UI |
|------|--------|-------|----|-----|-----|
| Offline/dev | `docker compose up` / `python run_acis.py` | Docker Kafka | SQLite | local | Vite |
| Standard laptop | laptop engine | Confluent | **Supabase** | Render | Vercel |
| Showcase | Oracle ARM | Confluent | **same Supabase** | Render | Vercel |

## 1. Supabase

1. Create a project → copy the Postgres URI (`DATABASE_URL`, prefer pooler + `?sslmode=require`).
2. Apply schema once:

```bash
psql "$DATABASE_URL" -f deploy/supabase/schema.sql
```

Or paste [`deploy/supabase/schema.sql`](../deploy/supabase/schema.sql) into the Supabase SQL editor.  
Starting the engine with `DATABASE_URL` set also runs `CREATE TABLE IF NOT EXISTS` on boot.

## 2. Confluent Cloud

1. Create a Basic cluster; create an API key with produce/consume (and create topics if allowed).
2. Set in `.env.cloud`:

```env
ACIS_KAFKA_BOOTSTRAP_SERVERS=pkc-....confluent.cloud:9092
ACIS_KAFKA_SECURITY_PROTOCOL=SASL_SSL
ACIS_KAFKA_SASL_MECHANISM=PLAIN
ACIS_KAFKA_SASL_USERNAME=<API_KEY>
ACIS_KAFKA_SASL_PASSWORD=<API_SECRET>
```

3. Topics: engine tries to create `acis.*` on startup. If ACLs block create, pre-create topics from `TopicAdmin.create_all_acis_topics` (see `runtime/topic_manager.py`).

## 3. Render (BFF)

1. Connect the repo; use [`render.yaml`](../render.yaml) or create a Python Web Service with root `acis-api-bff`.
2. Set env vars (same Confluent + Supabase as the engine) plus:

```env
ACIS_ENV=production
ACIS_API_KEY=<32+ chars>
ACIS_ALLOWED_ORIGINS=https://your-app.vercel.app,http://localhost:5173
ACIS_DATABASE_URL=<same as DATABASE_URL>
```

3. Health check: `GET /api/v1/health`

## 4. Vercel (frontend)

1. Project root: `acis-x-frontend`
2. Build env:

```env
VITE_API_BASE_URL=https://<your-bff>.onrender.com
VITE_API_KEY=<same as ACIS_API_KEY>
```

3. After deploy, confirm the UI loads agents/customers from Render.

## 5. Engine — laptop

```bash
cp .env.cloud.example .env.cloud
# fill Confluent + Supabase + API key

# Option A: Docker
docker compose -f docker-compose.cloud-engine.yml --env-file .env.cloud up -d --build

# Option B: host Python
set -a && source .env.cloud && set +a   # bash
python run_acis.py
```

## 6. Engine — Oracle Always Free (ARM)

1. Create an **Ampere A1** VM with the full free memory (do not use the 1 GB AMD shapes for the engine).
2. Install Docker; clone the repo; copy `.env.cloud` onto the VM.
3. Run:

```bash
docker compose -f deploy/oracle/docker-compose.yml --env-file .env up -d --build
```

4. Open **outbound** HTTPS/Kafka only — no inbound ports required for the engine.

## Switching laptop ↔ Oracle

1. Stop the engine on the host you are leaving (`docker compose down` or Ctrl+C).
2. Start the engine on the other host with the **same** `.env.cloud` (same Confluent + Supabase).
3. Do **not** redeploy Vercel/Render — they already point at the shared data plane.

Only one engine should write at a time for a clean demo (avoid two ScenarioGenerators).

## Demo reset

```bash
# Truncate Supabase business tables (SQL editor), then:
# reset Confluent consumer groups / recreate topics if needed
python reset_acis.py   # when pointed at the same brokers (careful in shared cloud)
```

## Env checklist

| Variable | Engine | BFF | Frontend |
|----------|--------|-----|----------|
| `DATABASE_URL` / `ACIS_DATABASE_URL` | yes | yes | — |
| Confluent bootstrap + SASL | yes | yes | — |
| `ACIS_API_KEY` | — | yes | build `VITE_API_KEY` |
| `VITE_API_BASE_URL` | — | — | yes (Render URL) |
| `ACIS_ALLOWED_ORIGINS` | — | yes (Vercel URLs) | — |
| `GROQ_API_KEY` | yes (scraping) | — | — |
