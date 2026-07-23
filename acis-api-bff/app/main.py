from __future__ import annotations

import asyncio
import json
import uuid
import logging
import random
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer
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
AGENTS_STATE: dict[str, dict[str, Any]] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat().replace('+00:00', 'Z')


async def consume_agent_status():
    consumer = AIOKafkaConsumer(
        'acis.registry', 'acis.agent.health',
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"bff-status-group-{uuid.uuid4()}",
        auto_offset_reset='latest'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            if msg.value:
                event = json.loads(msg.value.decode('utf-8'))
                payload = event.get('payload', {})
                agent_id = payload.get('agent_id')
                if not agent_id:
                    continue
                
                if event['event_type'] == 'registry.agent.deregistered':
                    AGENTS_STATE.pop(agent_id, None)
                else:
                    if agent_id not in AGENTS_STATE:
                        AGENTS_STATE[agent_id] = {
                            'agent_id': agent_id,
                            'agent_name': payload.get('agent_name', agent_id),
                            'agent_type': payload.get('agent_type', 'unknown'),
                            'status': payload.get('status', 'healthy'),
                            'registered_at': now_iso(),
                            'last_heartbeat': now_iso(),
                            'topics': payload.get('topics', {}),
                            'capabilities': payload.get('capabilities', []),
                            'version': payload.get('version', '1.0.0')
                        }
                    else:
                        if 'status' in payload:
                            AGENTS_STATE[agent_id]['status'] = payload['status']
                        AGENTS_STATE[agent_id]['last_heartbeat'] = now_iso()
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_agent_status())


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
    return {
        'agents': list(AGENTS_STATE.values())
    }


@app.post('/api/v1/metrics/compute')
def compute_metrics(_: str = Depends(require_api_key)):
    job_id = str(uuid.uuid4())
    return {
        'job_id': job_id,
        'status': 'ready',
        'started_at': now_iso(),
    }


@app.get('/api/v1/metrics/result/{job_id}')
def metrics_result(job_id: str, _: str = Depends(require_api_key)):
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


logger = logging.getLogger(__name__)

async def stream_generator():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"bff-sse-group-{uuid.uuid4()}",
        auto_offset_reset='latest',
        enable_auto_commit=False,
    )
    
    # Subscribe to all topics starting with acis.
    consumer.subscribe(pattern=r'^acis\..*')
    
    await consumer.start()
    try:
        while True:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=15.0)
                if msg.value:
                    event_data = msg.value.decode('utf-8')
                    yield f"event: acis_event\ndata: {event_data}\n\n"
            except asyncio.TimeoutError:
                # Keep connection alive
                yield ': heartbeat\n\n'
    except asyncio.CancelledError:
        logger.info("SSE client disconnected, stopping consumer")
        raise
    finally:
        await consumer.stop()


@app.get('/api/v1/events/stream')
async def events_stream(_: str = Depends(require_api_key)):
    return StreamingResponse(stream_generator(), media_type='text/event-stream')
