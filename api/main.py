import asyncio
import json
import logging
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from runtime.kafka_client import KafkaClient
from schemas.event_schema import Event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("acis_api")

app = FastAPI(title="ACIS-X Real-Time Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"Client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        
        # Convert message to json once
        text_data = json.dumps(message)
        
        # Broadcast to all connected clients
        for connection in list(self.active_connections):
            try:
                await connection.send_text(text_data)
            except Exception as e:
                logger.error(f"Error sending message to client: {e}")
                self.disconnect(connection)

manager = ConnectionManager()

# Background Kafka Consumer Task
async def consume_kafka_events():
    logger.info("Initializing Kafka Consumer for Dashboard...")
    try:
        # Using confluent backend as default
        client = KafkaClient(backend="confluent")
        
        # We need a unique group ID for the UI so it sees all events and doesn't load balance with other consumers
        client.subscribe(["acis.system", "acis.registry", "acis.agent.health", "acis.risk"], group_id="acis_dashboard_api_group")
        
        logger.info("Subscribed to dashboard topics. Starting poll loop...")
        
        while True:
            # Poll Kafka (non-blocking in async context by using a small timeout and asyncio.sleep)
            messages = client.poll(timeout_ms=100)
            
            for msg in messages:
                try:
                    # Message value is bytes, decode it to string, then parse JSON
                    msg_str = msg.value.decode('utf-8')
                    msg_dict = json.loads(msg_str)
                    
                    # We broadcast the raw dictionary to WebSockets
                    # Wrap it to include the topic it came from
                    payload = {
                        "topic": msg.topic,
                        "event": msg_dict
                    }
                    await manager.broadcast(payload)
                except Exception as e:
                    logger.error(f"Error processing message from {msg.topic}: {e}")
            
            # Yield control back to the event loop
            await asyncio.sleep(0.01)
            
    except Exception as e:
        logger.error(f"Fatal error in Kafka consumer loop: {e}")

@app.on_event("startup")
async def startup_event():
    # Start the Kafka consumer as a background task
    asyncio.create_task(consume_kafka_events())

@app.get("/health")
async def health_check():
    return {"status": "ok", "clients_connected": len(manager.active_connections)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # We don't expect the client to send much, maybe ping/pong
            data = await websocket.receive_text()
            # Could handle client messages here if needed (e.g. filtering requests)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
