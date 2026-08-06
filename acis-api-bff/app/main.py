from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Set

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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
    get_customer_collections,
    get_customer_risk_explanation,
    get_customer_external_intelligence,
    get_table_rows,
)
from .security import require_api_key

settings = load_settings()
app = FastAPI(title='acis-api-bff', version='0.1.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

AGENTS_STATE: dict[str, dict[str, Any]] = {}

# ── SSE broadcast infrastructure ──────────────────────────────────────────
# One shared Kafka consumer pushes events into a set of asyncio.Queue objects,
# one per connected SSE client. This prevents unbounded consumer group
# proliferation (previous impl created a new group per browser tab).
_sse_clients: Set[asyncio.Queue] = set()
_sse_consumer_started = False


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat().replace('+00:00', 'Z')


async def _consume_agent_status() -> None:
    """Background task: track agent registration & heartbeat events."""
    consumer = AIOKafkaConsumer(
        'acis.registry', 'acis.agent.health',
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id='bff-status-group',
        auto_offset_reset='latest',
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
                    metrics_data = payload.get('metrics', {})
                    restart_cnt = payload.get('restart_count', metrics_data.get('restart_count', 0))

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
                            'version': payload.get('version', '1.0.0'),
                            'restart_count': restart_cnt,
                            'metrics': metrics_data,
                        }
                    else:
                        if 'status' in payload:
                            AGENTS_STATE[agent_id]['status'] = payload['status']
                        AGENTS_STATE[agent_id]['last_heartbeat'] = now_iso()
                        if metrics_data:
                            AGENTS_STATE[agent_id]['metrics'] = metrics_data
                        if restart_cnt:
                            AGENTS_STATE[agent_id]['restart_count'] = restart_cnt
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


async def _consume_sse_events() -> None:
    """Single shared Kafka consumer that broadcasts events to all SSE clients.

    Instead of creating a new consumer group per browser tab (which caused
    unbounded group proliferation), this runs one consumer and fans out
    messages to all connected client queues.
    """
    global _sse_consumer_started
    _sse_consumer_started = True
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id='bff-sse-shared',
        auto_offset_reset='latest',
        enable_auto_commit=False,
    )
    consumer.subscribe(pattern=r'^acis\..*')
    await consumer.start()
    logger.info('Shared SSE consumer started (group=bff-sse-shared)')
    try:
        async for msg in consumer:
            if msg.value:
                event_data = msg.value.decode('utf-8')
                sse_line = f"event: acis_event\ndata: {event_data}\n\n"
                # Fan out to all connected clients
                dead_queues = []
                for q in _sse_clients:
                    try:
                        q.put_nowait(sse_line)
                    except asyncio.QueueFull:
                        dead_queues.append(q)
                # Remove clients whose queues are full (disconnected/slow)
                for q in dead_queues:
                    _sse_clients.discard(q)
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        _sse_consumer_started = False


@app.on_event('startup')
async def startup_event():
    asyncio.create_task(_consume_agent_status())
    asyncio.create_task(_consume_sse_events())


@app.on_event('shutdown')
async def shutdown_event():
    global _producer
    if _producer is not None:
        await _producer.stop()


@app.get('/api/v1/health')
def health():
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


_producer: AIOKafkaProducer | None = None

async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await _producer.start()
    return _producer


@app.get('/api/v1/customers/{customer_id}/collections')
def customer_collections(customer_id: str, _: str = Depends(require_api_key)):
    return get_customer_collections(customer_id)


@app.get('/api/v1/customers/{customer_id}/risk-explanation')
def customer_risk_explanation(customer_id: str, _: str = Depends(require_api_key)):
    row = get_customer_risk_explanation(customer_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Risk explanation not found")
    return row


@app.get('/api/v1/customers/{customer_id}/external-intelligence')
def customer_external_intelligence(customer_id: str, _: str = Depends(require_api_key)):
    row = get_customer_external_intelligence(customer_id)
    if row is None:
        raise HTTPException(status_code=404, detail="External intelligence not found")
    return row


@app.get('/api/v1/database/tables/{table_name}')
def database_table_data(table_name: str, limit: int = Query(default=50, ge=1, le=200), _: str = Depends(require_api_key)):
    data = get_table_rows(table_name, limit)
    if "error" in data:
        raise HTTPException(status_code=400, detail=data["error"])
    return data


@app.get('/api/v1/system/logs/stream')
async def stream_system_logs(_: str = Depends(require_api_key)):
    async def log_generator():
        import os
        import asyncio
        yield "data: [BFF] Log stream connected\n\n"
        log_path = "/app/acis.log"
        if not os.path.exists(log_path):
            yield "data: [BFF] Log file not found\n\n"
            return
        
        with open(log_path, 'r', encoding='utf-8') as f:
            f.seek(0, os.SEEK_END)
            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(0.5)
                    continue
                yield f"data: {line.strip()}\n\n"
                
    return StreamingResponse(
        log_generator(),
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache, no-transform',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        },
    )


def _broadcast_sse_event(event_dict: dict) -> None:
    event_str = json.dumps(event_dict)
    sse_line = f"event: acis_event\ndata: {event_str}\n\n"
    for queue in list(_sse_clients):
        try:
            queue.put_nowait(sse_line)
        except asyncio.QueueFull:
            pass


SIMULATION_STATE = {"is_paused": False, "mode": "running"}


@app.get('/api/v1/simulation/status')
async def get_simulation_status(_: str = Depends(require_api_key)):
    return SIMULATION_STATE


@app.post('/api/v1/simulation/control')
async def simulation_control(payload: dict, _: str = Depends(require_api_key)):
    action = payload.get("action")
    valid_actions = ("pause", "pause_scenario", "freeze_all", "resume")
    if action not in valid_actions:
        raise HTTPException(status_code=400, detail=f"Invalid action. Must be one of {valid_actions}.")

    import uuid
    producer = await get_producer()

    if action in ("pause_scenario", "pause"):
        SIMULATION_STATE["is_paused"] = True
        SIMULATION_STATE["mode"] = "scenario_only"
        events_to_send = [("scenario.pause", "scenario_generator")]
    elif action == "freeze_all":
        SIMULATION_STATE["is_paused"] = True
        SIMULATION_STATE["mode"] = "freeze_all"
        events_to_send = [("scenario.pause", "scenario_generator"), ("time.pause", "time_tick")]
    else:  # resume
        SIMULATION_STATE["is_paused"] = False
        SIMULATION_STATE["mode"] = "running"
        events_to_send = [("scenario.resume", "scenario_generator"), ("time.resume", "time_tick")]

    for etype, entity in events_to_send:
        event = {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "event_type": etype,
            "event_source": "bff_control_api",
            "event_time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
            "correlation_id": f"ctrl_{uuid.uuid4().hex}",
            "entity_id": entity,
            "schema_version": "1.1",
            "payload": {"action": action, "mode": SIMULATION_STATE["mode"]},
            "metadata": {}
        }
        await producer.send_and_wait("acis.control", json.dumps(event).encode("utf-8"), key=entity.encode("utf-8"))
        _broadcast_sse_event(event)

    return {"status": "ok", "message": f"Simulation {action} published", "simulation_state": SIMULATION_STATE}


@app.post('/api/v1/simulation/fault')
async def simulation_fault(payload: dict, _: str = Depends(require_api_key)):
    instance_id = payload.get("instance_id")
    if not instance_id:
        raise HTTPException(status_code=400, detail="Missing instance_id.")
        
    import uuid
    agent_name = "UnknownAgent"
    parts = instance_id.split("_")
    if len(parts) >= 2:
        raw_name = parts[1]
        if "dbagent" in raw_name:
            agent_name = "DBAgent"
        elif "queryagent" in raw_name:
            agent_name = "QueryAgent"
        elif "memoryagent" in raw_name:
            agent_name = "MemoryAgent"
        else:
            agent_name = raw_name

    # Immediately mark agent state as restarting in memory
    for aid, ainfo in list(AGENTS_STATE.items()):
        if aid == instance_id or ainfo.get("agent_name") == agent_name:
            ainfo["status"] = "restarting"
            
    event = {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "event_type": "agent.restart.requested",
        "event_source": "bff_fault_api",
        "event_time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
        "correlation_id": f"ctrl_{uuid.uuid4().hex}",
        "entity_id": agent_name,
        "schema_version": "1.1",
        "payload": {
            "agent_id": instance_id,
            "agent_name": agent_name,
            "instance_id": instance_id,
            "reason": "Manual fault injection from BFF dashboard",
            "graceful": True,
            "timeout_seconds": 30,
            "restart_count": 0,
            "max_restarts": 3,
            "decision_rule": "manual_trigger",
            "decision_score": 1.0
        },
        "metadata": {}
    }
    producer = await get_producer()
    await producer.send_and_wait("acis.system", json.dumps(event).encode("utf-8"), key=agent_name.encode("utf-8"))
    _broadcast_sse_event(event)
    return {"status": "ok", "message": f"Restart request for {instance_id} published"}


@app.get('/api/v1/metrics')
def get_metrics(_: str = Depends(require_api_key)):
    """Return precomputed metrics directly from the database."""
    return {
        'status': 'ready',
        'computed_at': now_iso(),
        'data': {
            'risk_profiles': get_risk_profiles(),
            'customer_metrics': get_customer_metrics(),
            'summary': get_dashboard_summary(),
        },
    }


# Backward-compatible aliases for the old two-step compute→poll pattern.
# The frontend may still call these until it's updated.
@app.post('/api/v1/metrics/compute')
def compute_metrics(_: str = Depends(require_api_key)):
    return {'job_id': 'precomputed', 'status': 'ready', 'started_at': now_iso()}


@app.get('/api/v1/metrics/result/{job_id}')
def metrics_result(job_id: str, _: str = Depends(require_api_key)):
    return get_metrics(_)


logger = logging.getLogger(__name__)


# ── SSE Event Stream ──────────────────────────────────────────────────────

async def _client_stream_generator():
    """Per-client SSE generator that reads from the shared broadcast queue."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    _sse_clients.add(queue)
    try:
        yield ': ping\n\n'
        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=15.0)
                yield data
            except asyncio.TimeoutError:
                yield ': heartbeat\n\n'
    except asyncio.CancelledError:
        logger.info('SSE client disconnected')
        raise
    finally:
        _sse_clients.discard(queue)


@app.get('/api/v1/events/stream')
async def events_stream(_: str = Depends(require_api_key)):
    return StreamingResponse(
        _client_stream_generator(),
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache, no-transform',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        },
    )
