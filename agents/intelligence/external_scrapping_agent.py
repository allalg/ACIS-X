"""
External Scraping Agent for ACIS-X.

Generates litigation risk signals from Google News RSS:
- Fraud, lawsuit, investigation detection
- Risk analysis (keyword + inference based)
- Litigation inference (direction detection)

Subscribes to:
- acis.metrics (customer events)

Produces:
- acis.metrics (external.litigation.updated)

Self-contained agent with NO external module dependencies.
"""

import logging
import re
import time
import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
from typing import List, Any, Dict, Optional
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# RISK KEYWORD WEIGHTS
# ─────────────────────────────────────────────────────────────────────────────
NEWS_RISK_KEYWORDS = {
    "fraud": 1.0,
    "scam": 1.0,
    "bankruptcy": 1.0,
    "investigation": 0.8,
    "lawsuit": 0.7,
    "sued": 0.7,
    "penalty": 0.6,
    "default": 0.6,
    "violation": 0.5,
    "probe": 0.5,
    "indictment": 0.9,
    "embezzlement": 1.0,
    "misconduct": 0.7,
    "scandal": 0.8,
    "settlement": 0.5,
}

# ─────────────────────────────────────────────────────────────────────────────
# CASE TYPE CLASSIFICATION KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────
CASE_TYPE_KEYWORDS = {
    "fraud": ["fraud", "cheating", "ipc 420", "420 ipc", "dishonest", "misappropriation", "scam", "embezzlement"],
    "insolvency": ["insolvency", "ibc", "bankruptcy", "liquidation", "nclt", "corporate insolvency"],
    "regulatory": ["sebi", "violation", "compliance", "rbi", "regulatory", "investigation", "probe", "penalty"],
    "lawsuit": ["lawsuit", "sued", "sues", "litigation", "legal action", "court"],
    "financial_default": ["default", "loan", "repayment", "recovery", "npa", "debt recovery"],
    "tax_dispute": ["tax", "gst", "income tax", "tax evasion", "tax authority"],
    "contract_dispute": ["contract", "breach", "agreement", "specific performance"],
    "criminal": ["criminal", "offence", "fir", "cognizable", "prosecution", "indictment"],
}


class ExternalScrapingAgent(BaseAgent):
    """
    External Scraping Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (customer.metrics.updated, customer.profile.updated)

    Produces:
    - acis.metrics (external.litigation.updated)

    Responsibility:
    Generate litigation risk from Google News RSS (self-contained).
    All news fetching, risk analysis, and litigation inference is inline.
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_OUTPUT = "acis.metrics"
    GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"

    def __init__(self, kafka_client: Any, query_agent: Optional[Any] = None):
        super().__init__(
            agent_name="ExternalScrapingAgent",
            agent_version="3.0.0",
            group_id="litigation-agent-group",
            subscribed_topics=[self.TOPIC_INPUT],
            capabilities=[
                "litigation_risk_detection",
                "legal_signal_extraction",
                "google_news_integration",
                "news_risk_analysis",
            ],
            kafka_client=kafka_client,
            agent_type="ExternalScrapingAgent",
        )
        self._query_agent = query_agent
        self._cache = {}

        # FIX: Configure session with connection pooling and retry logic for Google News
        self._session = requests.Session()

        # Retry strategy: exponential backoff on connection failures
        retry_strategy = Retry(
            total=3,  # Max retries
            backoff_factor=1,  # 1s, 2s, 4s wait between retries
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
            allowed_methods=["GET"]  # Only retry GET requests
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        # Headers with User-Agent for better compatibility
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0"
        })
        # FIX 9: Track published litigation_risk values to prevent duplicate events
        self._last_published_risk: Dict[str, float] = {}

        logger.info("[ExternalScrapingAgent] Initialized with Google News RSS integration (retries enabled)")

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type in ("customer.metrics.updated", "customer.profile.updated"):
            self.handle_event(event)

    # ─────────────────────────────────────────────────────────────────────────
    # GOOGLE NEWS RSS FUNCTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _fetch_news(self, company_name: str) -> List[Dict[str, str]]:
        """
        Fetch news articles from Google News RSS.

        Args:
            company_name: Company name to search

        Returns:
            List of article dicts with title and pubDate (max 15, last 3 days only)
        """
        try:
            search_query = f'"{company_name}" lawsuit OR sued OR fraud OR investigation OR penalty OR default'
            encoded_query = quote_plus(search_query)
            url = f"{self.GOOGLE_NEWS_RSS_URL}?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

            logger.info(f"[ExternalScrapingAgent] Fetching Google News for: {company_name}")

            # FIX: Increased timeout from 10 to 25 seconds (Google News can be slow)
            # Retry strategy is configured in __init__ via HTTPAdapter
            response = self._session.get(url, timeout=25)

            if response.status_code != 200:
                logger.warning(f"[ExternalScrapingAgent] Google News returned {response.status_code}")
                return []

            root = ET.fromstring(response.content)
            articles = []
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=3)

            for item in root.findall(".//item")[:15]:
                title_elem = item.find("title")
                pub_date_elem = item.find("pubDate")

                title = title_elem.text if title_elem is not None else ""
                pub_date_str = pub_date_elem.text if pub_date_elem is not None else ""

                if not title:
                    continue

                # Filter by date (last 3 days only)
                if pub_date_str:
                    try:
                        pub_date = parsedate_to_datetime(pub_date_str)
                        if pub_date < cutoff_date:
                            continue
                    except (ValueError, TypeError):
                        pass  # Include articles with unparseable dates

                articles.append({
                    "title": title.strip(),
                    "pubDate": pub_date_str.strip() if pub_date_str else "",
                })

            if not articles:
                logger.info(f"[ExternalScrapingAgent] No recent news articles found for {company_name}")
            else:
                logger.info(f"[ExternalScrapingAgent] Found {len(articles)} news articles for {company_name}")
            
            return articles

        except ET.ParseError as e:
            logger.error(f"[ExternalScrapingAgent] XML parse error: {e}")
            return []
        except requests.RequestException as e:
            logger.error(f"[ExternalScrapingAgent] Google News fetch error: {e}")
            return []
        except Exception as e:
            logger.error(f"[ExternalScrapingAgent] Unexpected error fetching news: {e}")
            return []

    def _classify_case(self, text: str) -> str:
        """
        Classify case/article type based on keywords.

        Args:
            text: Headline or case title

        Returns:
            Case type: fraud, insolvency, regulatory, lawsuit, financial_default, general
        """
        text_lower = text.lower()

        for case_type, keywords in CASE_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return case_type

        return "general"

    def _detect_direction(self, text: str, company_name: str) -> str:
        """
        Detect litigation direction from headline.

        Patterns detected:
        - "X sues Y" → check if company is X (plaintiff) or Y (defendant)
        - "X sued by Y" → check positions

        Args:
            text: Headline text
            company_name: Company name to check

        Returns:
            "filed_by_company", "filed_against_company", or "unknown"
        """
        text_lower = text.lower()
        company_lower = company_name.lower()

        # Build match patterns: full name + individual significant words
        company_patterns = [company_lower]
        company_words = [w for w in company_lower.split() if len(w) > 2]
        company_patterns.extend(company_words)

        def matches_company(segment: str) -> bool:
            """Check if segment contains company name or significant words."""
            segment = segment.lower()
            for pattern in company_patterns:
                # Match as word boundary or partial match for longer patterns
                if len(pattern) >= 4:
                    if pattern in segment:
                        return True
                else:
                    # For short patterns, require word boundary
                    if re.search(rf'\b{re.escape(pattern)}\b', segment):
                        return True
            return False

        # Pattern: "X sues Y" or "X suing Y"
        sues_pattern = r"(\b\w+(?:\s+\w+){0,5})\s+(?:sues|suing|files\s+(?:suit|lawsuit)\s+against)\s+(\b\w+(?:\s+\w+){0,5})"
        match = re.search(sues_pattern, text_lower)
        if match:
            plaintiff = match.group(1)
            defendant = match.group(2)
            if matches_company(plaintiff):
                return "filed_by_company"
            if matches_company(defendant):
                return "filed_against_company"

        # Pattern: "X sued by Y"
        sued_by_pattern = r"(\b\w+(?:\s+\w+){0,5})\s+(?:sued\s+by|facing\s+(?:lawsuit|suit)\s+from)\s+(\b\w+(?:\s+\w+){0,5})"
        match = re.search(sued_by_pattern, text_lower)
        if match:
            defendant = match.group(1)
            plaintiff = match.group(2)
            if matches_company(defendant):
                return "filed_against_company"
            if matches_company(plaintiff):
                return "filed_by_company"

        # Pattern: "A vs B"
        vs_pattern = r"(\b\w+(?:\s+\w+){0,5})\s+(?:vs\.?|v\.|versus)\s+(\b\w+(?:\s+\w+){0,5})"
        match = re.search(vs_pattern, text_lower)
        if match:
            plaintiff_side = match.group(1)
            defendant_side = match.group(2)
            if matches_company(plaintiff_side):
                return "filed_by_company"
            if matches_company(defendant_side):
                return "filed_against_company"

        return "unknown"

    def _analyze_news(self, articles: List[Dict[str, str]], company_name: str) -> Dict[str, Any]:
        """
        Analyze news articles for litigation risk.

        For each article:
        - Detect risk keywords with weights
        - Classify case type
        - Detect litigation direction

        Args:
            articles: List of article dicts with title, pubDate
            company_name: Company name for direction detection

        Returns:
            Dict with risk_score, severity, case_count, cases, case_types
        """
        if not articles:
            logger.info(f"[ExternalScrapingAgent] No articles to analyze for {company_name}")
            return {
                "risk_score": 0.0,
                "severity": "low",
                "case_count": 0,
                "cases": [],
                "case_types": [],
                "litigation_flag": False,
            }

        cases = []
        case_types = set()
        total_weight = 0.0
        match_count = 0

        for article in articles:
            title = article.get("title", "")
            title_lower = title.lower()

            article_weight = 0.0
            matched_keywords = []

            for keyword, weight in NEWS_RISK_KEYWORDS.items():
                if keyword in title_lower:
                    article_weight = max(article_weight, weight)
                    matched_keywords.append(keyword)

            if matched_keywords:
                case_type = self._classify_case(title)
                direction = self._detect_direction(title, company_name)

                case_types.add(case_type)
                total_weight += article_weight
                match_count += 1

                cases.append({
                    "headline": title[:200],
                    "type": case_type,
                    "direction": direction,
                })

        # Compute risk score based on total articles
        risk_score = min(1.0, (total_weight / max(1, len(articles))) * 2)

        # Determine severity
        if risk_score >= 0.5:
            severity = "high"
        elif risk_score >= 0.25:
            severity = "medium"
        else:
            severity = "low"

        return {
            "risk_score": round(risk_score, 4),
            "severity": severity,
            "case_count": match_count,
            "cases": cases[:10],
            "case_types": list(case_types),
            "litigation_flag": match_count > 0,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # EVENT HANDLER
    # ─────────────────────────────────────────────────────────────────────────

    def handle_event(self, event: Event) -> None:
        """Handle customer events and generate litigation risk."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in event")
            return

        # Resolve company name with improved fallback chain
        # Priority: payload -> QueryAgent lookup -> customer_id
        company_name = data.get("company_name")
        if not company_name and self._query_agent:
            try:
                customer = self._query_agent.get_customer(customer_id)
                if customer:
                    company_name = customer.get("name")
                    if company_name:
                        logger.debug(f"[ExternalScrapingAgent] Resolved company name from DB: {company_name}")
            except Exception as e:
                logger.debug(f"[ExternalScrapingAgent] Failed to lookup customer from QueryAgent: {e}")

        # Final fallback: use customer_id
        company_name = company_name or customer_id
        company_name = company_name.strip()[:100]

        # Check cache (TTL = 24h)
        cache_key = company_name.lower()
        cache_entry = self._cache.get(cache_key)

        if cache_entry:
            age = (datetime.utcnow() - cache_entry["timestamp"]).total_seconds()
            if age < 86400:
                news_data = cache_entry["news_data"]
                logger.info(f"[ExternalScrapingAgent] Cache hit for {customer_id}")
            else:
                articles = self._fetch_news(company_name)
                news_data = self._analyze_news(articles, company_name)
                self._cache[cache_key] = {
                    "news_data": news_data,
                    "timestamp": datetime.utcnow()
                }
                logger.info(f"[ExternalScrapingAgent] Cache expired, fetched new data for {customer_id}")
        else:
            articles = self._fetch_news(company_name)
            news_data = self._analyze_news(articles, company_name)
            self._cache[cache_key] = {
                "news_data": news_data,
                "timestamp": datetime.utcnow()
            }
            logger.info(f"[ExternalScrapingAgent] Cache miss, fetched new data for {customer_id}")

        # Build payload
        payload = {
            "customer_id": customer_id,
            "company_name": company_name,

            "litigation_flag": news_data["litigation_flag"],
            "litigation_risk": news_data["risk_score"],
            "severity": news_data["severity"],

            "case_count": news_data["case_count"],
            "cases": news_data["cases"],
            "case_types": news_data["case_types"],

            "source": "google_news",
            "confidence": 0.7,
            "generated_at": datetime.utcnow().isoformat()
        }

        # FIX 9: DEDUPLICATION - Skip publishing if litigation_risk unchanged
        last_risk = self._last_published_risk.get(customer_id)
        litigation_risk = round(news_data["risk_score"], 4)
        if last_risk is not None and last_risk == litigation_risk:
            logger.debug(
                f"[ExternalScrapingAgent] Skipping publish for {customer_id}: "
                f"litigation_risk={litigation_risk:.4f} (unchanged)"
            )
            return

        # Publish event (standardized naming)
        self.publish_event(
            topic=self.TOPIC_OUTPUT,
            event_type="external.litigation.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        # FIX 9: Track published risk to prevent duplicate events
        self._last_published_risk[customer_id] = litigation_risk

        logger.info(
            f"[ExternalScrapingAgent] company={company_name} "
            f"litigation_risk={news_data['risk_score']:.4f} "
            f"cases={news_data['case_count']} severity={news_data['severity']}"
        )
