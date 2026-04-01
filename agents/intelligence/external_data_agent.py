import logging
import requests
import time
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
        self._cache = {}

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)

    def _fetch_gdelt_risk(self, company_name: str) -> float:
        url = "https://api.gdeltproject.org/api/v2/doc/doc"

        params = {
            "query": company_name,
            "mode": "ArtList",
            "maxrecords": 10,
            "format": "json",
            "timespan": "1d",
        }

        try:
            logger.debug(f"GDELT query for: {company_name}")
            time.sleep(0.5)
            response = requests.get(url, params=params, timeout=5)

            if response.status_code != 200:
                logger.warning(f"GDELT non-200 response for {company_name}")
                return 0.0

            try:
                data = response.json()
            except Exception:
                logger.warning(f"GDELT invalid JSON for {company_name}")
                return 0.0

            articles = data.get("articles", [])
            if not articles:
                return 0.0

            tones = [
                max(min(a.get("tone", 0), 100), -100)
                for a in articles if "tone" in a
            ]
            if not tones:
                return 0.0

            avg_tone = sum(tones) / len(tones)

            risk = (-avg_tone) / 100
            return max(0.0, min(1.0, risk))

        except Exception as e:
            logger.error(f"GDELT fetch failed for {company_name}: {e}")
            return 0.0

    def handle_event(self, event: Event) -> None:
        """Handle customer.metrics.updated event and enrich with external data."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in metrics event")
            return

        company_name = data.get("company_name") or customer_id
        company_name = company_name.strip()[:100]

        # Step 2: Fetch external risk with caching
        cache_key = company_name.lower()
        cache_entry = self._cache.get(cache_key)

        if cache_entry:
            age = (datetime.utcnow() - cache_entry["timestamp"]).total_seconds()
            if age < 86400:
                external_risk = cache_entry["risk"]
                logger.info(f"[ExternalDataAgent] Cache hit for {customer_id}")
            else:
                external_risk = self._fetch_gdelt_risk(company_name)
                self._cache[cache_key] = {
                    "risk": external_risk,
                    "timestamp": datetime.utcnow()
                }
                logger.info(f"[ExternalDataAgent] Cache expired, fetched new data for {customer_id}")
        else:
            external_risk = self._fetch_gdelt_risk(company_name)
            self._cache[cache_key] = {
                "risk": external_risk,
                "timestamp": datetime.utcnow()
            }
            logger.info(f"[ExternalDataAgent] Cache miss, fetched new data for {customer_id}")

        # Step 3: Set placeholders
        financial_score = None
        litigation_flag = None

        # Step 4: Create payload
        external_payload = {
            "customer_id": customer_id,
            "financial_score": financial_score,
            "external_risk": round(external_risk, 4),
            "litigation_flag": litigation_flag,
            "source": "gdelt_api",
            "generated_at": datetime.utcnow().isoformat()
        }

        # Step 5: Publish event
        self.publish_event(
            topic=self.TOPIC_OUTPUT,
            event_type="ExternalDataEnriched",
            entity_id=customer_id,
            payload=external_payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[ExternalDataAgent] customer={customer_id}, external_risk={external_risk:.4f}"
        )
