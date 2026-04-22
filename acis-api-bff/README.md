# ACIS API BFF

FastAPI backend-for-frontend service for ACIS-X dashboard.

## Run

```bash
cd acis-api-bff
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Auth

All endpoints require `X-API-Key` header.
For SSE (`/api/v1/events/stream`), `api_key` query param is also accepted because native EventSource cannot set custom headers.
