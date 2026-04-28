import logging
import multiprocessing
import time
from dataclasses import dataclass
from typing import Dict, Literal, List, Any

logger = logging.getLogger(__name__)

@dataclass
class AgentEntry:
    process: multiprocessing.Process
    agent_class: type
    kwargs: dict
    restart_count: int
    last_restart_time: float
    status: Literal["running", "failed", "permanently_failed"]

class SupervisorClient:
    def __init__(self, shared_dict):
        self.shared_dict = shared_dict

    def restart_agent(self, agent_name: str) -> bool:
        requests = self.shared_dict.get("restart_requests", [])
        requests.append(agent_name)
        self.shared_dict["restart_requests"] = requests
        return True

class AgentSupervisor:
    def __init__(self, kafka_client=None):
        self._registry: Dict[str, AgentEntry] = {}
        self.kafka_client = kafka_client

    def register(self, agent_name: str, process: multiprocessing.Process, agent_class: type, kwargs: dict) -> None:
        self._registry[agent_name] = AgentEntry(
            process=process,
            agent_class=agent_class,
            kwargs=kwargs,
            restart_count=0,
            last_restart_time=time.time(),
            status="running"
        )
        logger.info(f"Supervisor registered {agent_name}")

    def restart_agent(self, agent_name: str) -> bool:
        if agent_name not in self._registry:
            logger.error(f"Cannot restart unknown agent: {agent_name}")
            return False
            
        entry = self._registry[agent_name]
        
        if entry.status == "permanently_failed":
            logger.critical(f"Agent {agent_name} is permanently failed. Not restarting.")
            return False
            
        now = time.time()
        
        # Terminate existing process
        if entry.process and entry.process.is_alive():
            logger.info(f"Terminating existing process for {agent_name}")
            entry.process.terminate()
            
            # Wait up to 5s
            for _ in range(50):
                if not entry.process.is_alive():
                    break
                time.sleep(0.1)
                
            if entry.process.is_alive():
                logger.warning(f"Process {agent_name} did not terminate gracefully, killing it.")
                entry.process.kill()
                entry.process.join(timeout=1)
                
        # Check restart limits
        if entry.restart_count >= 5 and (now - entry.last_restart_time) < 120.0:
            entry.status = "permanently_failed"
            logger.critical(f"Agent {agent_name} permanently failed after {entry.restart_count} restarts in {now - entry.last_restart_time:.1f}s.")
            
            if self.kafka_client:
                from datetime import datetime
                import uuid
                alert_payload = {
                    "agent_name": agent_name,
                    "reason": "Max restart limit reached",
                    "restart_count": entry.restart_count,
                    "timestamp": datetime.utcnow().isoformat(),
                    "severity": "critical"
                }
                event = {
                    "event_id": f"evt_{uuid.uuid4()}",
                    "event_type": "agent.critical.failure",
                    "event_source": "AgentSupervisor",
                    "event_time": datetime.utcnow().isoformat(),
                    "entity_id": agent_name,
                    "schema_version": "1.1",
                    "payload": alert_payload,
                    "metadata": {}
                }
                try:
                    self.kafka_client.publish("acis.alerts", event)
                except Exception as e:
                    logger.error(f"Failed to publish alert to acis.alerts: {e}")
            return False
            
        # Reset if it's been running fine for a while
        if (now - entry.last_restart_time) > 120.0:
            entry.restart_count = 0
            
        entry.restart_count += 1
        entry.last_restart_time = now
        
        logger.info(f"Spawning fresh Process for {agent_name} (attempt {entry.restart_count})")
        from run_acis import launch_agent  # Import here to avoid circular imports
        
        new_process = multiprocessing.Process(
            target=launch_agent,
            args=(entry.agent_class, entry.kwargs),
            name=agent_name
        )
        new_process.start()
        
        entry.process = new_process
        entry.status = "running"
        return True

    def get_status(self) -> Dict[str, dict]:
        status_dict = {}
        for name, entry in self._registry.items():
            status_dict[name] = {
                "status": entry.status,
                "restart_count": entry.restart_count,
                "last_restart_time": entry.last_restart_time,
                "is_alive": entry.process.is_alive() if entry.process else False,
                "pid": entry.process.pid if entry.process else None
            }
        return status_dict

    def all_processes(self) -> List[multiprocessing.Process]:
        return [entry.process for entry in self._registry.values() if entry.process]
