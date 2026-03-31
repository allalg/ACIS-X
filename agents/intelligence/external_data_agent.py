import random
import logging
from typing import List, Any
from datetime import datetime

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class ExternalDataAgent(BaseAgent):
    """
    External Data Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (customer.metrics.updated)

    Enriches customer events with simulated external intelligence signals:
    - financial_score (company strength)
    - external_risk (external environment risk)
    - litigation_flag (risk indicator)

    Produces:
    - acis.external (ExternalDataEnriched)
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_OUTPUT = "acis.external"

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="ExternalDataAgent",
            agent_version="1.0.0",
            group_id="external-data-group",
            subscribed_topics=[self.TOPIC_INPUT],
            capabilities=["external_data_enrichment"],
            kafka_client=kafka_client,
            agent_type="ExternalDataAgent",
        )

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)

    def handle_event(self, event: Event) -> None:
        """Handle customer.metrics.updated event and enrich with external data."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in metrics event")
            return

        # Step 2: Generate external signals
        financial_score = round(random.uniform(0.4, 0.9), 2)
        external_risk = round(random.uniform(0.2, 0.8), 2)
        litigation_flag = random.random() < 0.2

        # Normalize generated values to [0, 1]
        financial_score = min(max(financial_score, 0), 1)
        external_risk = min(max(external_risk, 0), 1)

        # Step 3: Create payload
        external_payload = {
            "customer_id": customer_id,
            "financial_score": financial_score,
            "external_risk": external_risk,
            "litigation_flag": litigation_flag,
            "generated_at": datetime.utcnow().isoformat()
        }

        # Step 4: Publish event
        self.publish_event(
            topic=self.TOPIC_OUTPUT,
            event_type="ExternalDataEnriched",
            entity_id=customer_id,
            payload=external_payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[ExternalDataAgent] Enriched data for {customer_id}: "
            f"financial_score={financial_score}, external_risk={external_risk}, "
            f"litigation_flag={litigation_flag}"
        )
