"""
External Scraping Agent for ACIS-X.

Generates multiple risk signals using a scraping-ready architecture:
- Litigation risk
- Reputation risk
- News risk

Currently uses mock data, designed for future integration with:
- eCourts India
- Indian Kanoon
- News websites
"""

import logging
import random
import time
from typing import List, Any, Tuple
from datetime import datetime

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class ExternalScrapingAgent(BaseAgent):
    """
    External Scraping Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (customer.metrics.updated)

    Produces:
    - acis.external (LitigationRiskUpdated)

    Responsibility:
    Generate multi-signal external risk from scraping sources.
    Designed for future scraping integration (bs4).
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_OUTPUT = "acis.external"

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="ExternalScrapingAgent",
            agent_version="1.0.0",
            group_id="litigation-agent-group",
            subscribed_topics=[self.TOPIC_INPUT],
            capabilities=[
                "litigation_risk_detection",
                "legal_signal_extraction",
                "reputation_risk_detection",
                "news_signal_extraction"
            ],
            kafka_client=kafka_client,
            agent_type="ExternalScrapingAgent",
        )
        self._cache = {}

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)

    def _fetch_litigation_signal(self, company_name: str) -> Tuple[float, int]:
        """
        Fetch litigation signal for a company.

        PHASE 1: Mock implementation with random data.
        FUTURE: Replace with real scraping using requests + BeautifulSoup (bs4).

        Target sources (not implemented yet):
        - eCourts India
        - Indian Kanoon

        Args:
            company_name: Normalized company name

        Returns:
            Tuple of (litigation_risk, case_count)
        """
        try:
            case_count = random.randint(0, 5)

            severity_bonus = 0.0

            if case_count >= 3:
                severity_bonus += 0.2

            if case_count >= 5:
                severity_bonus += 0.2

            litigation_risk = case_count * 0.12 + severity_bonus

            litigation_risk = max(0.0, min(1.0, litigation_risk))

            return litigation_risk, case_count

        except Exception as e:
            logger.error(f"[ExternalScrapingAgent] Litigation fetch failed for {company_name}: {e}")
            return 0.0, 0

    def _fetch_reputation_signal(self, company_name: str) -> float:
        """
        Fetch reputation signal for a company.

        PHASE 1: Mock implementation with random data.
        FUTURE: Replace with real scraping using requests + BeautifulSoup (bs4).

        Target sources (not implemented yet):
        - Review websites
        - Consumer complaint forums

        Args:
            company_name: Normalized company name

        Returns:
            reputation_risk score (0.0 to 1.0)
        """
        try:
            reputation_keywords = ["fraud", "scam", "default"]

            risk = random.uniform(0.0, 0.5)

            return risk

        except Exception as e:
            logger.error(f"[ExternalScrapingAgent] Reputation fetch failed for {company_name}: {e}")
            return 0.0

    def _fetch_news_signal(self, company_name: str) -> float:
        """
        Fetch news signal for a company.

        PHASE 1: Mock implementation with random data.
        FUTURE: Replace with real scraping using requests + BeautifulSoup (bs4).

        Target sources (not implemented yet):
        - News websites
        - Financial news aggregators

        Args:
            company_name: Normalized company name

        Returns:
            news_risk score (0.0 to 1.0)
        """
        try:
            news_risk = random.uniform(0.0, 0.6)

            return news_risk

        except Exception as e:
            logger.error(f"[ExternalScrapingAgent] News fetch failed for {company_name}: {e}")
            return 0.0

    def handle_event(self, event: Event) -> None:
        """Handle customer.metrics.updated event and generate multi-signal risk."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in metrics event")
            return

        company_name = data.get("company_name") or customer_id
        company_name = company_name.strip().lower()[:100]

        # Step 2: Check cache
        cache_key = company_name
        cache_entry = self._cache.get(cache_key)

        if cache_entry:
            age = (datetime.utcnow() - cache_entry["timestamp"]).total_seconds()
            if age < 86400:
                litigation_risk = cache_entry["litigation_risk"]
                news_risk = cache_entry["news_risk"]
                reputation_risk = cache_entry["reputation_risk"]
                external_scraped_risk = cache_entry["combined"]
                case_count = cache_entry["case_count"]
                logger.info(f"[ExternalScrapingAgent] Cache hit for {customer_id}")
            else:
                # Rate limiting - max 1 request per second
                time.sleep(1)

                litigation_risk, case_count = self._fetch_litigation_signal(company_name)
                reputation_risk = self._fetch_reputation_signal(company_name)
                news_risk = self._fetch_news_signal(company_name)

                external_scraped_risk = (
                    0.5 * litigation_risk +
                    0.3 * news_risk +
                    0.2 * reputation_risk
                )
                external_scraped_risk = max(0.0, min(1.0, external_scraped_risk))

                self._cache[cache_key] = {
                    "litigation_risk": litigation_risk,
                    "news_risk": news_risk,
                    "reputation_risk": reputation_risk,
                    "combined": external_scraped_risk,
                    "case_count": case_count,
                    "timestamp": datetime.utcnow()
                }
                logger.info(f"[ExternalScrapingAgent] Cache expired, fetched new data for {customer_id}")
        else:
            # Rate limiting - max 1 request per second
            time.sleep(1)

            litigation_risk, case_count = self._fetch_litigation_signal(company_name)
            reputation_risk = self._fetch_reputation_signal(company_name)
            news_risk = self._fetch_news_signal(company_name)

            external_scraped_risk = (
                0.5 * litigation_risk +
                0.3 * news_risk +
                0.2 * reputation_risk
            )
            external_scraped_risk = max(0.0, min(1.0, external_scraped_risk))

            self._cache[cache_key] = {
                "litigation_risk": litigation_risk,
                "news_risk": news_risk,
                "reputation_risk": reputation_risk,
                "combined": external_scraped_risk,
                "case_count": case_count,
                "timestamp": datetime.utcnow()
            }
            logger.info(f"[ExternalScrapingAgent] Cache miss, fetched new data for {customer_id}")

        # Step 3: Create payload
        payload = {
            "customer_id": customer_id,

            "litigation_risk": round(litigation_risk, 4),
            "news_risk": round(news_risk, 4),
            "reputation_risk": round(reputation_risk, 4),

            "external_scraped_risk": round(external_scraped_risk, 4),

            "case_count": case_count,
            "source": "scraping_multi_signal",
            "confidence": 0.6,
            "generated_at": datetime.utcnow().isoformat()
        }

        # Step 4: Publish event
        self.publish_event(
            topic=self.TOPIC_OUTPUT,
            event_type="LitigationRiskUpdated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[ExternalScrapingAgent] company={company_name} "
            f"litigation={litigation_risk:.4f} news={news_risk:.4f} "
            f"reputation={reputation_risk:.4f} combined={external_scraped_risk:.4f}"
        )
