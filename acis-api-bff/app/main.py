from __future__ import annotations

import asyncio
import json
import random
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .config import load_settings
from .db import (
    get_customer_by_id,
    get_customer_metrics,
    get_customers,
    get_dashboard_summary,
    get_invoices,
    get_payments,
    get_risk_profiles,
)
from .security import require_api_key

settings = load_settings()
app = FastAPI(title='acis-api-bff', version='0.1.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=['GET', 'POST', 'OPTIONS'],
    allow_headers=['X-API-Key', 'Content-Type'],
)

METRICS_JOBS: dict[str, dict[str, Any]] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


@app.get('/api/v1/health')
def health(_: str = Depends(require_api_key)):
    return {
        'status': 'ok',
        'service': 'acis-api-bff',
        'version': '0.1.0',
        'timestamp': now_iso(),
    }


@app.get('/api/v1/dashboard/summary')
def dashboard_summary(_: str = Depends(require_api_key)):
    return get_dashboard_summary()


@app.get('/api/v1/customers')
def customers(
    search: str | None = Query(default=None),
    _: str = Depends(require_api_key),
):
    rows = get_customers(search)
    return {'customers': rows, 'total': len(rows)}


@app.get('/api/v1/customers/{customer_id}')
def customer_detail(customer_id: str, _: str = Depends(require_api_key)):
    row = get_customer_by_id(customer_id)
    if row is None:
        raise HTTPException(status_code=404, detail='Customer not found')
    return row


@app.get('/api/v1/invoices')
def invoices(
    customer_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=500),
    _: str = Depends(require_api_key),
):
    rows, total = get_invoices(customer_id=customer_id, status=status, page=page, limit=limit)
    return {'invoices': rows, 'total': total}


@app.get('/api/v1/payments')
def payments(
    customer_id: str | None = Query(default=None),
    invoice_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=500),
    _: str = Depends(require_api_key),
):
    rows, total = get_payments(customer_id=customer_id, invoice_id=invoice_id, page=page, limit=limit)
    return {'payments': rows, 'total': total}


@app.get('/api/v1/agents/status')
def agents_status(_: str = Depends(require_api_key)):
    names = [
        'ScenarioGeneratorAgent',
        'CustomerStateAgent',
        'AggregatorAgent',
        'CustomerProfileAgent',
        'RiskScoringAgent',
        'PaymentPredictionAgent',
        'CollectionsAgent',
        'OverdueDetectionAgent',
        'CreditPolicyAgent',
        'ExternalDataAgent',
        'ExternalScrapingAgent',
        'DBAgent',
        'MemoryAgent',
        'QueryAgent',
        'TimeTickAgent',
        'MonitoringAgent',
        'SelfHealingAgent',
        'RuntimeManager',
        'PlacementEngine',
        'RegistryService',
    ]
    return {
        'agents': [
            {
                'agent_id': f'agent-{index:03d}',
                'agent_name': name,
                'agent_type': 'business' if name.endswith('Agent') else 'operational',
                'status': random.choice(['healthy', 'healthy', 'degraded']),
                'registered_at': now_iso(),
                'last_heartbeat': now_iso(),
                'topics': {
                    'consumes': ['acis.invoices', 'acis.payments'],
                    'produces': ['acis.risk'],
                },
                'capabilities': ['stream-processing'],
                'version': '1.0.0',
            }
            for index, name in enumerate(names, start=1)
        ]
    }


@app.post('/api/v1/metrics/compute')
def compute_metrics(_: str = Depends(require_api_key)):
    job_id = str(uuid.uuid4())
    METRICS_JOBS[job_id] = {
        'status': 'computing',
        'started_at': now_iso(),
    }
    return {
        'job_id': job_id,
        'status': 'computing',
        'started_at': METRICS_JOBS[job_id]['started_at'],
    }


@app.get('/api/v1/metrics/result/{job_id}')
def metrics_result(job_id: str, _: str = Depends(require_api_key)):
    job = METRICS_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')

    # Prototype behavior: first request after compute returns ready immediately.
    job['status'] = 'ready'

    return {
        'job_id': job_id,
        'status': 'ready',
        'computed_at': now_iso(),
        'data': {
            'risk_profiles': get_risk_profiles(),
            'customer_metrics': get_customer_metrics(),
            'summary': get_dashboard_summary(),
        },
    }


async def stream_generator():
    while True:
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': random.choice(
                [
                    'invoice.created',
                    'payment.received',
                    'customer.metrics.updated',
                    'risk.profile.updated',
                    'agent.health',
                    'self.healing.triggered',
                    'time.tick',
                ]
            ),
            'event_source': random.choice(
                [
                    'ScenarioGeneratorAgent',
                    'CustomerStateAgent',
                    'AggregatorAgent',
                    'RiskScoringAgent',
                    'MonitoringAgent',
                    'SelfHealingAgent',
                ]
            ),
            'event_time': now_iso(),
            'correlation_id': None,
            'entity_id': f'CUST-{random.randint(1, 20):04d}',
            'schema_version': '1.0',
            'payload': {'reason': 'prototype-stream'},
            'metadata': {},
        }

        yield ': heartbeat\n\n'
        yield f"event: acis_event\ndata: {json.dumps(event)}\n\n"
        await asyncio.sleep(15)


@app.get('/api/v1/events/stream')
async def events_stream(_: str = Depends(require_api_key)):
    return StreamingResponse(stream_generator(), media_type='text/event-stream')
