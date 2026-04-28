import logging
import re
import sqlite3
import threading
import time
import requests
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any, Optional, Dict
from datetime import datetime, timedelta
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient
from utils.circuit_breaker import CircuitBreaker, CircuitOpenError

_screener_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)

logger = logging.getLogger(__name__)


class ExternalDataAgent(BaseAgent):
    """
    External Data Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (customer.metrics.updated)

    Enriches customer events with financial data:
    - PE ratio, ROE, ROCE, Debt, Market Cap  (NSE/BSE primary)
    - Screener.in fallback for missing fields
    - Tofler.in for private companies (primary)
    - LLM-based estimation for private companies with no structured data
    - Computed external_risk with tiered confidence model

    Produces:
    - acis.metrics (ExternalDataEnriched)  — ONLY when real data is fetched.
      Throttled events do NOT publish (no-data events violate event contract).
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_OUTPUT = "acis.metrics"
    DB_PATH = "acis.db"
    CACHE_TTL_HOURS = 24
    THROTTLE_MIN_HOURS = 24

    # Confidence weights for tiered private-company enrichment
    CONFIDENCE_STRUCTURED = 0.85   # NSE/BSE/Screener structured data
    CONFIDENCE_TOFLER     = 0.75   # Tofler.in semi-structured
    CONFIDENCE_LLM        = 0.20   # LLM news-based estimation (weak signal)

    def __init__(
        self,
        kafka_client: Any,
        db_path: Optional[str] = None,
    ):
        super().__init__(
            agent_name="ExternalDataAgent",
            agent_version="3.0.0",  # Bumped: no-publish-when-throttled + Tofler + LLM fallback
            group_id="external-data-group",
            subscribed_topics=[self.TOPIC_INPUT],
            capabilities=["external_data_enrichment", "screener_scraping", "tofler_enrichment", "llm_estimation"],
            kafka_client=kafka_client,
            agent_type="ExternalDataAgent",
        )
        self._db_path = db_path or self.DB_PATH

        # FIX: Configure session with connection pooling and retry logic
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

        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        # FIX 8: In-memory throttle tracking (per company_id)
        self._last_fetch_time: Dict[str, float] = {}
        # FIX 9: Track published external_risk values to prevent duplicate events
        # Use bounded OrderedDict (max 5000 entries) to prevent memory growth
        from collections import OrderedDict
        self._last_published_risk: OrderedDict = OrderedDict()  # customer_id -> last risk
        self._last_published_signature: OrderedDict = OrderedDict()  # customer_id -> signature
        self._MAX_RISK_TRACK = 5000  # evict oldest when exceeded
        _screener_breaker.on_state_change = self._on_screener_breaker_change

        # ThreadPoolExecutor for non-blocking enrichment.
        # handle_event() submits work here and returns immediately so the Kafka
        # consumer thread is never blocked by network I/O.
        self._executor = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="ext-data",
        )
        self._in_flight: set[str] = set()
        self._in_flight_lock = threading.Lock()

    def _on_screener_breaker_change(self, old_state: str, new_state: str) -> None:
        if new_state == "OPEN":
            logger.warning("screener.in circuit OPEN — returning null enrichment")
        self.publish_event(
            topic="acis.monitoring",
            event_type="circuit_breaker.state_change",
            entity_id=self.agent_name,
            payload={
                "service": "screener.in",
                "new_state": new_state,
                "agent_name": self.agent_name
            }
        )

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def stop(self) -> None:
        """Stop agent and shut down the enrichment thread pool."""
        super().stop()
        self._executor.shutdown(wait=True, cancel_futures=True)

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)

    # ─────────────────────────────────────────────────────────────────────────
    # DATABASE FUNCTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _get_cached_financials(self, company_name: str, slug: str) -> Optional[Dict]:
        """
        Query external_financials by real company_name first, then by ticker (slug).
        Returns data if exists and updated_at < 24 hours, else None.
        """
        try:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # Try real company name first
            cursor.execute(
                "SELECT * FROM external_financials WHERE company_name = ?",
                (company_name,)
            )
            row = cursor.fetchone()
            # Fallback: lookup by ticker/slug
            if not row:
                try:
                    cursor.execute(
                        "SELECT * FROM external_financials WHERE ticker = ?",
                        (slug,)
                    )
                    row = cursor.fetchone()
                except Exception:
                    pass
            conn.close()

            if row:
                updated_at = datetime.fromisoformat(row["updated_at"])
                if datetime.utcnow() - updated_at < timedelta(hours=self.CACHE_TTL_HOURS):
                    logger.info(f"[ExternalDataAgent] Cache hit for {company_name}")
                    return dict(row)
                else:
                    logger.info(f"[ExternalDataAgent] Cache expired for {company_name}")
            return None
        except Exception as e:
            logger.warning(f"[ExternalDataAgent] DB read error: {e}")
            return None

    def _store_financials(self, data: Dict) -> None:
        """Insert or update financial data into external_financials table.

        BUG 3 FIX: store real company_name (e.g. 'Kotak Mahindra Bank Ltd'),
        not the Screener slug ('KOTAKBANK').  The slug is stored in the
        'ticker' column as a secondary lookup key.
        Only non-NULL incoming values overwrite existing columns (COALESCE),
        so partial updates never erase previously stored fields.
        """
        real_name = data.get("real_company_name") or data.get("company_name")
        ticker    = data.get("ticker") or data.get("company_name")  # slug used as ticker
        try:
            conn = sqlite3.connect(self._db_path)
            cursor = conn.cursor()

            # Ensure ticker column exists (migration-safe)
            try:
                cursor.execute("ALTER TABLE external_financials ADD COLUMN ticker TEXT")
                conn.commit()
            except Exception:
                pass  # column already exists

            cursor.execute("""
                INSERT INTO external_financials (
                    company_name, ticker, pe, roe, roce, debt, market_cap,
                    sales_growth, profit_growth, operating_margin,
                    interest_coverage, risk, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_name) DO UPDATE SET
                    ticker           = COALESCE(excluded.ticker,            external_financials.ticker),
                    pe               = COALESCE(excluded.pe,               external_financials.pe),
                    roe              = COALESCE(excluded.roe,              external_financials.roe),
                    roce             = COALESCE(excluded.roce,             external_financials.roce),
                    debt             = COALESCE(excluded.debt,             external_financials.debt),
                    market_cap       = COALESCE(excluded.market_cap,       external_financials.market_cap),
                    sales_growth     = COALESCE(excluded.sales_growth,     external_financials.sales_growth),
                    profit_growth    = COALESCE(excluded.profit_growth,    external_financials.profit_growth),
                    operating_margin = COALESCE(excluded.operating_margin, external_financials.operating_margin),
                    interest_coverage= COALESCE(excluded.interest_coverage,external_financials.interest_coverage),
                    risk             = COALESCE(excluded.risk,             external_financials.risk),
                    updated_at       = excluded.updated_at
            """, (
                real_name,
                ticker,
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
            logger.info(f"[ExternalDataAgent] Stored financials for {real_name} (ticker={ticker})")
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
            # FIX: Increased timeout from 10 to 20 seconds
            response = _screener_breaker.call(self._session.get, url, timeout=20, allow_redirects=True)
            if response.status_code == 200:
                logger.info(f"[ExternalDataAgent] Slug resolved directly: {normalized}")
                return normalized
        except CircuitOpenError:
            pass
        except requests.RequestException:
            pass

        # Fallback: search page
        try:
            search_url = f"https://www.screener.in/api/company/search/?q={company_name}"
            # FIX: Increased timeout from 10 to 20 seconds
            response = _screener_breaker.call(self._session.get, search_url, timeout=20)
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
        except CircuitOpenError:
            pass
        except Exception as e:
            logger.warning(f"[ExternalDataAgent] Search fallback failed: {e}")

        # Last resort: return normalized
        logger.warning(f"[ExternalDataAgent] Using normalized slug fallback: {normalized}")
        return normalized

    def _parse_numeric(self, text: str) -> Optional[float]:
        """
        Parse numeric value from text, handling Cr, Lakh, %, commas, and currency symbols.
        - Cr → multiply by 1e7 (1 crore = 10 million)
        - Lakh → multiply by 1e5 (1 lakh = 100 thousand)
        """
        if not text:
            return None
        text = str(text).strip().replace(",", "")

        multiplier = 1.0
        if "Cr" in text:
            multiplier = 1e7
        elif "Lakh" in text or "Lac" in text:
            multiplier = 1e5

        import re
        match = re.search(r"[-+]?\d*\.?\d+", text)
        if not match:
            return None

        try:
            return float(match.group()) * multiplier
        except ValueError:
            return None

    def _get_value_by_label(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        """
        Find value by label text using robust traversal.
        """
        label_lower = label.lower()

        # Strategy 1: Look for name/value pattern in list items FIRST
        for li in soup.select("li"):
            name_el = li.select_one(".name")
            value_el = li.select_one(".value, .number")
            if name_el and value_el:
                if label_lower in name_el.get_text(strip=True).lower():
                    return value_el.get_text(strip=True)

        # Strategy 2: Find span/div containing label, get sibling value
        for el in soup.find_all(["span", "div", "td"]):
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
            "source": "screener.in",
            "updated_at": datetime.utcnow().isoformat(),
        }

        # Retry logic: max 2 attempts
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                logger.info(f"[ExternalDataAgent] Scraping Screener for {slug}: {url} (attempt {attempt + 1})")
                response = _screener_breaker.call(self._session.get, url, timeout=10)

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

            except CircuitOpenError:
                return {"source": "circuit_open", "financial_risk": None, "revenue": None}
            except requests.RequestException as e:
                logger.error(f"[ExternalDataAgent] Screener fetch failed for {slug}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(1)
            except Exception as e:
                logger.error(f"[ExternalDataAgent] Screener parse error for {slug}: {e}")
                break

        return data

    def _safe_float(self, value: Any) -> Optional[float]:
        """Best-effort conversion to float."""
        if value is None:
            return None
        try:
            return float(str(value).replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    def _safe_div(self, numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
        """Safe division helper."""
        if numerator is None or denominator in (None, 0):
            return None
        return numerator / denominator

    def _fetch_nse_data(self, company_name: str, slug: str) -> Dict[str, Any]:
        """
        Fetch quote-level fallbacks from NSE public endpoints.
        This is best-effort and used only to fill missing Screener fields.
        """
        fallback = {}
        try:
            # Warm up session for NSE cookies.
            self._session.get("https://www.nseindia.com", timeout=15)
        except Exception:
            pass

        symbol = slug
        try:
            search_url = f"https://www.nseindia.com/api/search/autocomplete?q={quote_plus(company_name)}"
            resp = self._session.get(search_url, timeout=15, headers={"Referer": "https://www.nseindia.com"})
            if resp.status_code == 200:
                results = resp.json().get("symbols", [])
                if results:
                    symbol = results[0].get("symbol") or symbol
        except Exception:
            pass

        try:
            quote_url = f"https://www.nseindia.com/api/quote-equity?symbol={quote_plus(symbol)}"
            resp = self._session.get(quote_url, timeout=15, headers={"Referer": "https://www.nseindia.com"})
            if resp.status_code != 200:
                return fallback
            payload = resp.json()

            price = self._safe_float(payload.get("priceInfo", {}).get("lastPrice"))
            eps = self._safe_float(payload.get("metadata", {}).get("eps"))
            issued_cap = self._safe_float(payload.get("securityInfo", {}).get("issuedCap"))
            market_cap = None
            if issued_cap and price:
                market_cap = issued_cap * price

            # Prefer EPS-derived company PE (accurate) over sector PE (benchmark)
            pe = None
            if eps and price:
                pe = self._safe_div(price, eps)
            if pe is None:
                # pdSectorPe is sector-level P/E, not company-specific — use only as fallback
                sector_pe = self._safe_float(payload.get("metadata", {}).get("pdSectorPe"))
                if sector_pe is not None:
                    pe = sector_pe
                    logger.debug(
                        f"[ExternalDataAgent] Using sector PE ({sector_pe}) as fallback for {slug} "
                        f"(no EPS/price available for company-specific PE)"
                    )

            fallback = {
                "market_cap": market_cap,
                "pe": pe,
            }
        except Exception as e:
            logger.debug(f"[ExternalDataAgent] NSE fallback failed for {slug}: {e}")
        return fallback

    def _fetch_bse_data(self, company_name: str) -> Dict[str, Any]:
        """
        Fetch additional quote ratios from BSE public endpoints.
        Best-effort fallback only.
        """
        fallback: Dict[str, Any] = {}
        try:
            search_url = f"https://api.bseindia.com/BseIndiaAPI/api/SmartSearch/w?text={quote_plus(company_name)}"
            search_resp = self._session.get(search_url, timeout=15, headers={"Referer": "https://www.bseindia.com"})
            if search_resp.status_code != 200:
                return fallback
            results = search_resp.json()
            if not results:
                return fallback
            scrip_code = results[0].get("ScripCode")
            if not scrip_code:
                return fallback

            quote_url = f"https://api.bseindia.com/BseIndiaAPI/api/GetStkCurrMain/w?quotetype=EQ&scripcode={scrip_code}&flag=0"
            quote_resp = self._session.get(quote_url, timeout=15, headers={"Referer": "https://www.bseindia.com"})
            if quote_resp.status_code != 200:
                return fallback
            q = quote_resp.json()

            price = self._safe_float(q.get("LTP"))
            eps = self._safe_float(q.get("EPS"))
            book_value = self._safe_float(q.get("BookValue"))
            pe = self._safe_float(q.get("PE"))
            if pe is None and price and eps:
                pe = self._safe_div(price, eps)

            roe = None
            if eps is not None and book_value not in (None, 0):
                roe = (eps / book_value) * 100.0

            fallback = {
                "pe": pe,
                "roe": roe,
            }
        except Exception as e:
            logger.debug(f"[ExternalDataAgent] BSE fallback failed for {company_name}: {e}")

        return fallback

    def _fetch_primary_exchange_data(self, company_name: str, slug: str) -> Dict[str, Any]:
        """Fetch basic ratios primarily from NSE and BSE."""
        data = {
            "company_name": company_name,
            "source": "nse/bse",
            "pe": None,
            "roe": None,
            "roce": None,
            "debt": None,
            "market_cap": None,
            "sales_growth": None,
            "profit_growth": None,
            "operating_margin": None,
            "interest_coverage": None,
        }
        
        nse = self._fetch_nse_data(company_name, slug)
        bse = self._fetch_bse_data(company_name)
        
        for key in data.keys():
            if key in ("company_name", "source"):
                continue
            val_nse = nse.get(key)
            val_bse = bse.get(key)
            data[key] = val_nse if val_nse is not None else val_bse
            
        # FIX: Always include updated_at timestamp so cache TTL logic works
        data["updated_at"] = datetime.utcnow().isoformat()
            
        return data

    def _enrich_with_screener_fallback(self, data: Dict[str, Any], company_name: str, slug: str) -> Dict[str, Any]:
        """Fill missing NSE/BSE values using Screener.in (historical/advanced ratios)."""
        essential_keys = ["pe", "roe", "roce", "debt", "market_cap", "sales_growth", "profit_growth"]
        needs_fallback = any(data.get(k) is None for k in essential_keys)

        if needs_fallback:
            screener_data = self._fetch_screener_data(company_name, slug)
            for k in essential_keys + ["operating_margin", "interest_coverage"]:
                if data.get(k) is None:
                    data[k] = screener_data.get(k)
            data["source"] = "nse/bse+screener.in"

        # If still missing essential data, try Tofler (private companies)
        still_missing = any(data.get(k) is None for k in essential_keys)
        if still_missing:
            tofler_data = self._fetch_tofler_data(company_name)
            if tofler_data:
                for k in essential_keys + ["operating_margin", "interest_coverage"]:
                    if data.get(k) is None:
                        data[k] = tofler_data.get(k)
                data["source"] = data.get("source", "") + "+tofler"
                data["confidence"] = self.CONFIDENCE_TOFLER
                logger.info(f"[ExternalDataAgent] Tofler enrichment applied for {company_name}")

        # Last resort: LLM estimation — applied at low confidence weight
        still_missing_after_tofler = all(data.get(k) is None for k in essential_keys)
        if still_missing_after_tofler:
            llm_risk = self._estimate_risk_from_llm(company_name)
            if llm_risk is not None:
                # LLM risk is a weak signal — scale by confidence weight before storing
                data["llm_estimated_risk"] = round(llm_risk * self.CONFIDENCE_LLM, 4)
                data["source"] = data.get("source", "") + "+llm_estimation"
                data["confidence"] = self.CONFIDENCE_LLM
                logger.info(
                    f"[ExternalDataAgent] LLM estimation applied for {company_name}: "
                    f"raw={llm_risk:.4f}, weighted={data['llm_estimated_risk']:.4f}"
                )

        data["risk"] = self._compute_risk(data)
        return data

    def _fetch_tofler_data(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch basic financial signals from Tofler.in public search.
        Tofler provides MCA/ROC registration data including authorised capital,
        paid-up capital, and director count — useful for private companies.
        Returns a partial dict with any fields found, or None on failure.
        """
        try:
            search_url = f"https://www.tofler.in/search?company_name={quote_plus(company_name)}"
            resp = self._session.get(search_url, timeout=15)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "lxml")
            result = {}

            # Try to extract paid-up capital (proxy for company size / debt capacity)
            for el in soup.find_all(text=True):
                text = str(el).strip()
                if "paid up capital" in text.lower():
                    # Look for a nearby numeric value
                    parent = el.parent
                    if parent:
                        sib = parent.find_next_sibling()
                        if sib:
                            val = self._parse_numeric(sib.get_text(strip=True))
                            if val:
                                result["market_cap"] = val  # best proxy available
                                break

            return result if result else None
        except Exception as e:
            logger.debug(f"[ExternalDataAgent] Tofler fetch failed for {company_name}: {e}")
            return None

    def _estimate_risk_from_llm(self, company_name: str) -> Optional[float]:
        """
        Last-resort: call Groq LLM to estimate a financial risk score
        for private companies with no structured data.

        Returns a raw float in [0, 1] representing estimated financial risk.
        This is a WEAK SIGNAL — callers MUST multiply by CONFIDENCE_LLM (0.20)
        before storing or comparing against structured scores.
        Returns None if LLM is unavailable or response is malformed.
        """
        import os, json
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            logger.debug("[ExternalDataAgent] GROQ_API_KEY not set — skipping LLM estimation")
            return None

        prompt = (
            f"You are a financial risk analyst. Based solely on public knowledge about "
            f"'{company_name}', estimate a financial risk score between 0.0 (very low risk) "
            f"and 1.0 (very high risk). Consider: profitability, leverage, funding stage, "
            f"growth trajectory, regulatory environment. "
            f"Respond ONLY with a JSON object: {{\"financial_risk\": <float>}}"
        )
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            body = {
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "response_format": {"type": "json_object"},
            }
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers, json=body, timeout=20,
            )
            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"]
                parsed = json.loads(content)
                raw = float(parsed.get("financial_risk", 0.5))
                return max(0.0, min(1.0, raw))
        except Exception as e:
            logger.debug(f"[ExternalDataAgent] LLM estimation failed for {company_name}: {e}")
        return None

    def _compute_risk(self, data: Dict) -> Optional[float]:
        """
        Weighted financial risk model.

        Combines:
        - leverage risk
        - profitability risk
        - growth risk
        - coverage risk
        - efficiency risk

        Returns score in [0.0, 1.0] or None if no signals available.
        Uses llm_estimated_risk (already confidence-weighted) as fallback.
        """

        def normalize(val, low, high):
            if val is None:
                return None
            return max(0.0, min(1.0, (val - low) / (high - low)))

        def inverse_normalize(val, low, high):
            if val is None:
                return None
            return 1.0 - normalize(val, low, high)

        scores = []

        # ----------------------------
        # 1. LEVERAGE RISK (Debt)
        # ----------------------------
        debt = data.get("debt")
        if debt is not None:
            debt_score = normalize(debt, 0, 2)   # >2 = high risk
            scores.append(("debt", debt_score, 0.25))

        # ----------------------------
        # 2. PROFITABILITY (ROE)
        # ----------------------------
        roe = data.get("roe")
        if roe is not None:
            roe_score = inverse_normalize(roe, 10, 25)  # low ROE = high risk
            scores.append(("roe", roe_score, 0.20))

        # ----------------------------
        # 3. GROWTH (Profit Growth)
        # ----------------------------
        profit_growth = data.get("profit_growth")
        if profit_growth is not None:
            growth_score = inverse_normalize(profit_growth, 0, 20)
            scores.append(("growth", growth_score, 0.20))

        # ----------------------------
        # 4. INTEREST COVERAGE
        # ----------------------------
        coverage = data.get("interest_coverage")
        if coverage is not None:
            coverage_score = inverse_normalize(coverage, 2, 10)
            scores.append(("coverage", coverage_score, 0.15))

        # ----------------------------
        # 5. OPERATING MARGIN
        # ----------------------------
        margin = data.get("operating_margin")
        if margin is not None:
            margin_score = inverse_normalize(margin, 10, 30)
            scores.append(("margin", margin_score, 0.10))

        # ----------------------------
        # 6. PE RATIO (Overvaluation Risk)
        # ----------------------------
        pe = data.get("pe")
        if pe is not None:
            pe_score = normalize(pe, 10, 40)
            scores.append(("pe", pe_score, 0.10))

        # ----------------------------
        # AGGREGATION
        # ----------------------------
        total_weight = sum(w for _, _, w in scores)
        if total_weight == 0:
            # No structured signals — use LLM-estimated risk if available (already confidence-weighted)
            llm_fallback = data.get("llm_estimated_risk")
            if llm_fallback is not None:
                return round(max(0.0, min(1.0, llm_fallback)), 4)
            return None  # truly no data

        weighted_sum = sum(score * weight for _, score, weight in scores)

        risk = weighted_sum / total_weight

        logger.debug(f"[ExternalRisk] computed_risk={risk:.4f}, signals={scores}")

        return round(max(0.0, min(1.0, risk)), 4)

    # ─────────────────────────────────────────────────────────────────────────
    # EVENT HANDLER
    # ─────────────────────────────────────────────────────────────────────────

    def handle_event(self, event: Event) -> None:
        """Submit enrichment work to the thread pool and return immediately.

        The Kafka consumer thread is freed as soon as the task is queued.
        All blocking I/O (NSE/BSE/Screener fetches) runs inside
        _enrich_and_publish() on an executor thread.
        """
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        payload = event.payload or {}
        customer_id = payload.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in metrics event")
            return

        company_name = payload.get("company_name")

        # Submit — returns immediately
        with self._in_flight_lock:
            if customer_id in self._in_flight:
                logger.debug(
                    "[ExternalDataAgent] Skipping %s - enrichment already in flight",
                    customer_id,
                )
                return
            self._in_flight.add(customer_id)

        try:
            self._executor.submit(
                self._enrich_and_publish,
                customer_id,
                company_name,
                event.correlation_id,
            )
        except RuntimeError:
            with self._in_flight_lock:
                self._in_flight.discard(customer_id)
            logger.debug("[ExternalDataAgent] Executor is stopped; dropped enrichment for %s", customer_id)
            raise

    def _enrich_and_publish(self, customer_id: str, company_name: Optional[str], correlation_id: Optional[str]) -> None:
        """Full enrichment pipeline executed on an executor thread.

        Previously the body of handle_event().  Resolves company name, fetches
        exchange/Screener data, stores to DB, and publishes the enriched event.
        """
        try:
            # Resolve company name if not provided in payload
            if not company_name:
                try:
                    customer = QueryClient.query("get_customer", {"customer_id": customer_id})
                    if customer:
                        company_name = customer.get("name")
                except Exception as e:
                    logger.debug(f"[ExternalDataAgent] Failed to lookup customer: {e}")

            if not company_name:
                logger.debug(
                    f"[ExternalDataAgent] Skipping enrichment for {customer_id}: "
                    f"no company name resolved"
                )
                return

            company_name = company_name.strip()[:100]

            # Resolve slug (ticker)
            slug = self._resolve_slug(company_name)

            current_time = time.time()
            last_fetch = self._last_fetch_time.get(slug, 0)
            time_since_fetch = (current_time - last_fetch) / 3600

            # Check DB cache (keyed by real company_name, fallback by slug/ticker)
            cached = self._get_cached_financials(company_name, slug)

            if cached:
                logger.info(
                    f"[ExternalDataAgent] Using cached data for {company_name} "
                    f"(fetched {time_since_fetch:.1f}h ago)"
                )
                external_risk = cached.get("risk")
                financial_data = cached
                try:
                    updated_at = cached.get("updated_at")
                    if updated_at:
                        db_ts = datetime.fromisoformat(updated_at).timestamp()
                        self._last_fetch_time[slug] = max(self._last_fetch_time.get(slug, 0), db_ts)
                except Exception:
                    self._last_fetch_time[slug] = current_time

            elif last_fetch > 0 and time_since_fetch < self.THROTTLE_MIN_HOURS:
                logger.info(
                    f"[ExternalDataAgent] Throttled for {slug}: "
                    f"last fetch was {time_since_fetch:.1f}h ago. "
                    f"Skipping publish to preserve downstream cache integrity."
                )
                return

            else:
                logger.info(f"[ExternalDataAgent] Fetching fresh data for {slug} (Primary: NSE/BSE)")
                financial_data = self._fetch_primary_exchange_data(company_name, slug)
                financial_data = self._enrich_with_screener_fallback(financial_data, company_name, slug)

                financial_data["real_company_name"] = company_name
                financial_data["ticker"] = slug

                self._store_financials(financial_data)
                external_risk = financial_data.get("risk")
                self._last_fetch_time[slug] = current_time

            source = financial_data.get("source", "screener.in")
            confidence = financial_data.get("confidence", self.CONFIDENCE_STRUCTURED)

            llm_risk = financial_data.get("llm_estimated_risk")
            if external_risk is None and llm_risk is not None:
                external_risk = llm_risk

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
                "interest_coverage": financial_data.get("interest_coverage"),
                "external_risk": round(external_risk, 4) if external_risk is not None else None,
                "source": source,
                "confidence": round(confidence, 4),
                "generated_at": datetime.utcnow().isoformat(),
            }

            last_risk = self._last_published_risk.get(customer_id)
            current_risk_str = f"{round(external_risk, 4)}" if external_risk is not None else "None"
            current_signature = (
                f"{current_risk_str}|{financial_data.get('pe')}|{financial_data.get('roe')}|"
                f"{financial_data.get('roce')}|{financial_data.get('debt')}|{financial_data.get('market_cap')}"
            )
            safe_external_risk = round(external_risk, 4) if external_risk is not None else None

            if (
                last_risk == safe_external_risk
                and self._last_published_signature.get(customer_id) == current_signature
            ):
                logger.debug(
                    f"[ExternalDataAgent] Skipping publish for {customer_id}: "
                    f"external_risk={current_risk_str} (unchanged)"
                )
                return

            self.publish_event(
                topic=self.TOPIC_OUTPUT,
                event_type="external.data.enriched",
                entity_id=customer_id,
                payload=external_payload,
                correlation_id=correlation_id,
            )

            if len(self._last_published_risk) >= self._MAX_RISK_TRACK:
                self._last_published_risk.popitem(last=False)
            if len(self._last_published_signature) >= self._MAX_RISK_TRACK:
                self._last_published_signature.popitem(last=False)
            self._last_published_risk[customer_id] = safe_external_risk
            self._last_published_signature[customer_id] = current_signature

            logger.info(
                f"[ExternalDataAgent] customer={customer_id}, "
                f"external_risk={external_risk if external_risk is not None else 'None'}"
            )

        except Exception as exc:
            logger.error(
                "[ExternalDataAgent] _enrich_and_publish failed for %s: %s",
                customer_id, exc, exc_info=True,
            )
        finally:
            with self._in_flight_lock:
                self._in_flight.discard(customer_id)

