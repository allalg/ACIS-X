import logging
import re
import sqlite3
import time
import requests
from typing import List, Any, Optional, Dict
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class ExternalDataAgent(BaseAgent):
    """
    External Data Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (customer.metrics.updated)

    Enriches customer events with Screener.in financial data:
    - PE ratio, ROE, ROCE, Debt, Market Cap
    - Sales growth, Profit growth, Operating margin, Interest coverage
    - Computed external_risk based on financial signals

    Produces:
    - acis.external (ExternalDataEnriched)
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_OUTPUT = "acis.external"
    DB_PATH = "acis.db"
    CACHE_TTL_HOURS = 24

    def __init__(
        self,
        kafka_client: Any,
        db_path: Optional[str] = None,
    ):
        super().__init__(
            agent_name="ExternalDataAgent",
            agent_version="2.0.0",
            group_id="external-data-group",
            subscribed_topics=[self.TOPIC_INPUT],
            capabilities=["external_data_enrichment", "screener_scraping"],
            kafka_client=kafka_client,
            agent_type="ExternalDataAgent",
        )
        self._db_path = db_path or self.DB_PATH
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)

    # ─────────────────────────────────────────────────────────────────────────
    # DATABASE FUNCTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _get_cached_financials(self, slug: str) -> Optional[Dict]:
        """
        Query external_financials table for cached data.
        Returns data if exists and updated_at < 24 hours, else None.
        Uses slug as cache key for consistency.
        """
        try:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM external_financials WHERE company_name = ?",
                (slug,)
            )
            row = cursor.fetchone()
            conn.close()

            if row:
                updated_at = datetime.fromisoformat(row["updated_at"])
                if datetime.utcnow() - updated_at < timedelta(hours=self.CACHE_TTL_HOURS):
                    logger.info(f"[ExternalDataAgent] Cache hit for {slug}")
                    return dict(row)
                else:
                    logger.info(f"[ExternalDataAgent] Cache expired for {slug}")
            return None
        except Exception as e:
            logger.warning(f"[ExternalDataAgent] DB read error: {e}")
            return None

    def _store_financials(self, data: Dict) -> None:
        """Insert or update financial data into external_financials table."""
        try:
            conn = sqlite3.connect(self._db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO external_financials (
                    company_name, pe, roe, roce, debt, market_cap,
                    sales_growth, profit_growth, operating_margin,
                    interest_coverage, risk, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_name) DO UPDATE SET
                    pe=excluded.pe,
                    roe=excluded.roe,
                    roce=excluded.roce,
                    debt=excluded.debt,
                    market_cap=excluded.market_cap,
                    sales_growth=excluded.sales_growth,
                    profit_growth=excluded.profit_growth,
                    operating_margin=excluded.operating_margin,
                    interest_coverage=excluded.interest_coverage,
                    risk=excluded.risk,
                    updated_at=excluded.updated_at
            """, (
                data["company_name"],
                data.get("pe"),
                data.get("roe"),
                data.get("roce"),
                data.get("debt"),
                data.get("market_cap"),
                data.get("sales_growth"),
                data.get("profit_growth"),
                data.get("operating_margin"),
                data.get("interest_coverage"),
                data.get("risk"),
                data.get("updated_at"),
            ))
            conn.commit()
            conn.close()
            logger.info(f"[ExternalDataAgent] Stored financials for {data['company_name']}")
        except Exception as e:
            logger.error(f"[ExternalDataAgent] DB write error: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # SCRAPER FUNCTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _normalize_company_name(self, name: str) -> str:
        """
        Normalize company name to Screener slug format.
        E.g., 'RELIANCE INDUSTRIES LIMITED' → 'RELIANCE'
        """
        name = name.upper().strip()
        # Remove common suffixes
        suffixes = [
            "LIMITED", "LTD", "PRIVATE", "PVT", "CORPORATION", "CORP",
            "INDUSTRIES", "IND", "INDIA", "COMPANY", "CO", "INC"
        ]
        for suffix in suffixes:
            name = re.sub(rf"\b{suffix}\b\.?", "", name)
        # Clean up
        name = re.sub(r"[^A-Z0-9]", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        # Take first word as slug
        slug = name.split()[0] if name else "UNKNOWN"
        return slug

    def _resolve_slug(self, company_name: str) -> str:
        """
        Resolve company name to valid Screener slug.
        1. Try normalized slug first
        2. If 404, fallback to search page
        Returns validated slug or normalized fallback.
        """
        normalized = self._normalize_company_name(company_name)
        url = f"https://www.screener.in/company/{normalized}/"

        try:
            response = self._session.get(url, timeout=10, allow_redirects=True)
            if response.status_code == 200:
                logger.info(f"[ExternalDataAgent] Slug resolved directly: {normalized}")
                return normalized
        except requests.RequestException:
            pass

        # Fallback: search page
        try:
            search_url = f"https://www.screener.in/api/company/search/?q={company_name}"
            response = self._session.get(search_url, timeout=10)
            if response.status_code == 200:
                results = response.json()
                if results and len(results) > 0:
                    # API returns list of dicts with 'url' like '/company/RELIANCE/'
                    first_url = results[0].get("url", "")
                    match = re.search(r"/company/([^/]+)/", first_url)
                    if match:
                        slug = match.group(1)
                        logger.info(f"[ExternalDataAgent] Slug resolved via search: {slug}")
                        return slug
        except Exception as e:
            logger.warning(f"[ExternalDataAgent] Search fallback failed: {e}")

        # Last resort: return normalized
        logger.warning(f"[ExternalDataAgent] Using normalized slug fallback: {normalized}")
        return normalized

    def _parse_numeric(self, text: str) -> Optional[float]:
        """
        Parse numeric value from text, handling Cr, Lakh, %, commas.
        - Cr → multiply by 1e7 (1 crore = 10 million)
        - Lakh → multiply by 1e5 (1 lakh = 100 thousand)
        """
        if not text:
            return None
        text = text.strip().replace(",", "")

        multiplier = 1.0
        if "Cr" in text:
            multiplier = 1e7
            text = text.replace("Cr", "")
        elif "Lakh" in text or "Lac" in text:
            multiplier = 1e5
            text = re.sub(r"Lakh|Lac", "", text, flags=re.IGNORECASE)

        text = text.replace("%", "").strip()

        try:
            return float(text) * multiplier
        except ValueError:
            return None

    def _get_value_by_label(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        """
        Find value by label text using robust traversal.
        Searches for label text (case-insensitive), then extracts adjacent value.
        """
        label_lower = label.lower()

        # Strategy 1: Find span/div containing label, get sibling value
        for el in soup.find_all(["span", "div", "td", "li"]):
            text = el.get_text(strip=True).lower()
            if label_lower in text and len(text) < 50:  # Avoid matching long paragraphs
                # Check next sibling
                next_sib = el.find_next_sibling()
                if next_sib:
                    val = next_sib.get_text(strip=True)
                    if val and re.search(r"[\d.]", val):
                        return val
                # Check parent's children
                parent = el.parent
                if parent:
                    children = parent.find_all(recursive=False)
                    for i, child in enumerate(children):
                        if child == el and i + 1 < len(children):
                            val = children[i + 1].get_text(strip=True)
                            if val and re.search(r"[\d.]", val):
                                return val

        # Strategy 2: Look for name/value pattern in list items
        for li in soup.select("li"):
            name_el = li.select_one(".name")
            value_el = li.select_one(".value, .number")
            if name_el and value_el:
                if label_lower in name_el.get_text(strip=True).lower():
                    return value_el.get_text(strip=True)

        return None

    def _fetch_screener_data(self, company_name: str, slug: str) -> Dict:
        """
        Scrape financial data from Screener.in for the given company.
        Returns dict with all financial metrics and computed risk.
        Includes retry logic (max 2 attempts).
        """
        url = f"https://www.screener.in/company/{slug}/"

        data = {
            "company_name": slug,
            "pe": None,
            "roe": None,
            "roce": None,
            "debt": None,
            "market_cap": None,
            "sales_growth": None,
            "profit_growth": None,
            "operating_margin": None,
            "interest_coverage": None,
            "risk": 0.3,  # default risk
            "updated_at": datetime.utcnow().isoformat(),
        }

        # Retry logic: max 2 attempts
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                logger.info(f"[ExternalDataAgent] Scraping Screener for {slug}: {url} (attempt {attempt + 1})")
                response = self._session.get(url, timeout=10)

                if response.status_code != 200:
                    logger.warning(f"[ExternalDataAgent] Screener returned {response.status_code} for {slug}")
                    if attempt < max_attempts - 1:
                        time.sleep(1)
                        continue
                    return data

                soup = BeautifulSoup(response.text, "lxml")

                # Extract using label-based approach
                data["market_cap"] = self._parse_numeric(
                    self._get_value_by_label(soup, "Market Cap")
                )
                data["pe"] = self._parse_numeric(
                    self._get_value_by_label(soup, "Stock P/E") or
                    self._get_value_by_label(soup, "P/E")
                )
                data["roe"] = self._parse_numeric(
                    self._get_value_by_label(soup, "ROE")
                )
                data["roce"] = self._parse_numeric(
                    self._get_value_by_label(soup, "ROCE")
                )
                data["debt"] = self._parse_numeric(
                    self._get_value_by_label(soup, "Debt") or
                    self._get_value_by_label(soup, "Debt to equity")
                )
                data["sales_growth"] = self._parse_numeric(
                    self._get_value_by_label(soup, "Sales growth") or
                    self._get_value_by_label(soup, "Revenue growth")
                )
                data["profit_growth"] = self._parse_numeric(
                    self._get_value_by_label(soup, "Profit growth") or
                    self._get_value_by_label(soup, "Net profit growth")
                )
                data["operating_margin"] = self._parse_numeric(
                    self._get_value_by_label(soup, "OPM") or
                    self._get_value_by_label(soup, "Operating margin")
                )
                data["interest_coverage"] = self._parse_numeric(
                    self._get_value_by_label(soup, "Interest coverage") or
                    self._get_value_by_label(soup, "Int Coverage")
                )

                # Compute risk score
                data["risk"] = self._compute_risk(data)

                logger.info(
                    f"[ExternalDataAgent] Scraped Screener data for {slug}: "
                    f"PE={data['pe']}, ROE={data['roe']}, ROCE={data['roce']}, "
                    f"Debt={data['debt']}, Risk={data['risk']:.2f}"
                )
                return data

            except requests.RequestException as e:
                logger.error(f"[ExternalDataAgent] Screener fetch failed for {slug}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(1)
            except Exception as e:
                logger.error(f"[ExternalDataAgent] Screener parse error for {slug}: {e}")
                break

        return data

    def _compute_risk(self, data: Dict) -> float:
        """
        Compute risk score based on multiple financial signals.
        Returns value between 0.0 and 1.0.
        """
        risk = 0.0

        # High debt increases risk
        debt = data.get("debt")
        if debt is not None and debt > 1.0:  # Debt-to-equity > 1
            risk += 0.2

        # Low ROE increases risk
        roe = data.get("roe")
        if roe is not None and roe < 10:
            risk += 0.2

        # Negative profit growth increases risk
        profit_growth = data.get("profit_growth")
        if profit_growth is not None and profit_growth < 0:
            risk += 0.2

        # Low interest coverage increases risk
        interest_coverage = data.get("interest_coverage")
        if interest_coverage is not None and interest_coverage < 2:
            risk += 0.2

        # Low operating margin increases risk
        operating_margin = data.get("operating_margin")
        if operating_margin is not None and operating_margin < 10:
            risk += 0.2

        return min(risk, 1.0)

    # ─────────────────────────────────────────────────────────────────────────
    # EVENT HANDLER
    # ─────────────────────────────────────────────────────────────────────────

    def handle_event(self, event: Event) -> None:
        """Handle customer.metrics.updated event and enrich with external data."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data
        payload = event.payload or {}
        customer_id = payload.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in metrics event")
            return

        company_name = payload.get("company_name") or customer_id
        company_name = company_name.strip()[:100]

        # Step 2: Resolve slug FIRST (consistent cache key)
        slug = self._resolve_slug(company_name)

        # Step 3: Check DB cache using slug
        cached = self._get_cached_financials(slug)

        if cached:
            # Use cached data
            external_risk = cached["risk"]
            financial_data = cached
        else:
            # Scrape fresh data from Screener
            financial_data = self._fetch_screener_data(company_name, slug)
            self._store_financials(financial_data)
            external_risk = financial_data["risk"]

        # Step 4: Create enriched payload with all financial signals
        external_payload = {
            "customer_id": customer_id,
            "company_name": company_name,
            "pe": financial_data.get("pe"),
            "roe": financial_data.get("roe"),
            "roce": financial_data.get("roce"),
            "debt": financial_data.get("debt"),
            "market_cap": financial_data.get("market_cap"),
            "sales_growth": financial_data.get("sales_growth"),
            "profit_growth": financial_data.get("profit_growth"),
            "operating_margin": financial_data.get("operating_margin"),
            "interest_coverage": financial_data.get("interest_coverage"),
            "external_risk": round(external_risk, 4),
            "source": "screener.in",
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
