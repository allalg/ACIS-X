# ACIS-X Frontend

## Run locally (development)

```bash
cd acis-x-frontend
npm install
cp .env.example .env.local
npm run dev
```

Default URL: `http://localhost:5173`

## Deploy locally (production preview)

```bash
cd acis-x-frontend
npm install
cp .env.example .env.local
npm run deploy:local
```

This builds the app and serves the production bundle on `0.0.0.0:4173`
(for example `http://localhost:4173` and your local network IP).

## Environment variables

Set these in `.env.local`:

- `VITE_API_BASE_URL` (default: `http://localhost:8000`)
- `VITE_STREAM_URL` (default: `http://localhost:8000/api/v1/events/stream`)
- `VITE_API_KEY`
- `VITE_USE_STUBS` (`true` to run frontend without backend data)
