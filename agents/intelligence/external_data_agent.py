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
                    d = dict(row)
                    # Remap from DB column names to internal dict keys
                    if "market_cap (₹ Cr.)" in d:
                        d["market_cap"] = d.pop("market_cap (₹ Cr.)")
                    if "debt (₹ Cr.)" in d:
                        d["debt"] = d.pop("debt (₹ Cr.)")
                    return d
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
        source    = data.get("source", "")
        try:
            conn = sqlite3.connect(self._db_path)
            cursor = conn.cursor()

            # Ensure ticker + source columns exist (migration-safe)
            for col, col_type in [("ticker", "TEXT"), ("source", "TEXT"), ("\"debt (₹ Cr.)\"", "REAL"), ("\"market_cap (₹ Cr.)\"", "REAL")]:
                try:
                    cursor.execute(f"ALTER TABLE external_financials ADD COLUMN {col} {col_type}")
                    conn.commit()
                except Exception:
                    pass  # column already exists

            cursor.execute("""
                INSERT INTO external_financials (
                    company_name, ticker, pe, roe, roce, "debt (₹ Cr.)", "market_cap (₹ Cr.)",
                    sales_growth, profit_growth, operating_margin,
                    interest_coverage, risk, source, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_name) DO UPDATE SET
                    ticker           = COALESCE(excluded.ticker,            external_financials.ticker),
                    pe               = COALESCE(excluded.pe,               external_financials.pe),
                    roe              = COALESCE(excluded.roe,              external_financials.roe),
                    roce             = COALESCE(excluded.roce,             external_financials.roce),
                    "debt (₹ Cr.)"   = COALESCE(excluded."debt (₹ Cr.)",   external_financials."debt (₹ Cr.)"),
                    "market_cap (₹ Cr.)" = COALESCE(excluded."market_cap (₹ Cr.)", external_financials."market_cap (₹ Cr.)"),
                    sales_growth     = COALESCE(excluded.sales_growth,     external_financials.sales_growth),
                    profit_growth    = COALESCE(excluded.profit_growth,    external_financials.profit_growth),
                    operating_margin = COALESCE(excluded.operating_margin, external_financials.operating_margin),
                    interest_coverage= COALESCE(excluded.interest_coverage,external_financials.interest_coverage),
                    risk             = COALESCE(excluded.risk,             external_financials.risk),
                    source           = excluded.source,
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
                source,
                data.get("updated_at"),
            ))
            conn.commit()
            conn.close()
            logger.info(f"[ExternalDataAgent] Stored financials for {real_name} (ticker={ticker}, source={source})")
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

    def _strip_pvt(self, company_name: str) -> str:
        """
        Convert a private-company name to its listed-entity equivalent.
        e.g. 'Ola Electric Mobility Pvt Ltd' → 'Ola Electric Mobility Ltd'
        Also tries 'Limited' variant since some registrations use the full word.
        Useful for companies that were private when onboarded but have since IPO'd.
        """
        # First strip Pvt from 'Pvt Ltd' or 'Pvt Limited'
        name = re.sub(
            r'\bPvt\.?\s+(Ltd|Limited)\.?\b', r'\1',
            company_name, flags=re.IGNORECASE
        ).strip()
        return name

    def _name_variants(self, company_name: str) -> list:
        """
        Generate name variants for Screener search:
        - Original name
        - Pvt stripped version
        - 'Ltd' <-> 'Limited' swapped version
        """
        variants = [company_name]
        stripped = self._strip_pvt(company_name)
        if stripped != company_name:
            variants.append(stripped)
        # Swap Ltd <-> Limited
        if re.search(r'\bLtd\.?\b', company_name, re.IGNORECASE):
            variants.append(re.sub(r'\bLtd\.?\b', 'Limited', company_name, flags=re.IGNORECASE))
        elif re.search(r'\bLimited\b', company_name, re.IGNORECASE):
            variants.append(re.sub(r'\bLimited\b', 'Ltd', company_name, flags=re.IGNORECASE))
        # Also add bare name without any legal suffixes (handles rebranded companies)
        bare = re.sub(
            r'\b(Pvt|Private|Ltd|Limited|Inc|Corp|Corporation|LLP|LLC)\b\.?', '',
            company_name, flags=re.IGNORECASE
        ).strip().rstrip('.')
        if bare and bare != company_name and len(bare) >= 3:
            variants.append(bare)
        # Deduplicate
        seen = set()
        return [v for v in variants if v and not (v in seen or seen.add(v))]

    def _slug_candidates(self, company_name: str) -> list:
        """
        Generate multiple slug candidates to maximise Screener hit rate.
        Tries:
          1. Slug from raw company name          (e.g. 'OLA')
          2. Slug from Pvt→Ltd transformed name  (e.g. 'OLA' — same here, but different for multi-word)
          3. Full cleaned name without suffixes   (e.g. 'OLAELECTRIC', 'OLAELEC')
          4. Two-word slug                        (e.g. 'OLAELECTRIC')
        """
        candidates = []

        # Candidate 1: standard first-word slug
        candidates.append(self._normalize_company_name(company_name))

        # Candidate 2: after stripping Pvt
        listed_name = self._strip_pvt(company_name)
        if listed_name != company_name:
            candidates.append(self._normalize_company_name(listed_name))

        # Candidate 3: concatenated first-two-words (handles 'OLA ELECTRIC' → 'OLAELECTRIC')
        upper = company_name.upper()
        for suffix in ["LIMITED", "LTD", "PRIVATE", "PVT", "CORPORATION", "CORP",
                        "INDUSTRIES", "IND", "INDIA", "COMPANY", "CO", "INC", "MOBILITY",
                        "SERVICES", "TECHNOLOGIES", "SOLUTIONS"]:
            upper = re.sub(rf"\b{suffix}\b\.?", "", upper)
        words = re.sub(r"[^A-Z0-9 ]", " ", upper).split()
        words = [w for w in words if len(w) > 1]
        if len(words) >= 2:
            candidates.append("".join(words[:2])[:12])  # e.g. OLAELECTRIC
            candidates.append("".join(words[:2])[:8])   # e.g. OLAELEC

        # Deduplicate preserving order
        seen = set()
        return [c for c in candidates if c and not (c in seen or seen.add(c))]

    def _resolve_slug(self, company_name: str) -> str:
        """
        Resolve company name to valid Screener slug.
        1. Try each slug candidate directly against screener.in/company/<slug>/
        2. If none hit, fall back to Screener search API with the raw name
           AND the Pvt-stripped name.
        Returns validated slug or first-word fallback.
        """
        candidates = self._slug_candidates(company_name)

        # -- Direct slug probe --
        for slug in candidates:
            try:
                url = f"https://www.screener.in/company/{slug}/"
                response = _screener_breaker.call(
                    self._session.get, url, timeout=20, allow_redirects=True
                )
                if response.status_code == 200:
                    logger.info(f"[ExternalDataAgent] Slug resolved directly: {slug} (for '{company_name}')")
                    return slug
            except CircuitOpenError:
                break   # circuit open — stop probing
            except requests.RequestException:
                continue

        # -- Screener search API fallback (try all name variants) --
        search_names = self._name_variants(company_name)

        for sname in search_names:
            try:
                search_url = f"https://www.screener.in/api/company/search/?q={quote_plus(sname)}"
                response = _screener_breaker.call(self._session.get, search_url, timeout=20)
                if response.status_code == 200:
                    results = response.json()
                    if results:
                        first_url = results[0].get("url", "")
                        match = re.search(r"/company/([^/]+)/", first_url)
                        if match:
                            slug = match.group(1)
                            logger.info(
                                f"[ExternalDataAgent] Slug resolved via search: {slug} "
                                f"(query='{sname}')"
                            )
                            return slug
            except CircuitOpenError:
                break
            except Exception as e:
                logger.warning(f"[ExternalDataAgent] Search fallback failed for '{sname}': {e}")

        # Last resort: return first-word slug
        fallback = candidates[0] if candidates else "UNKNOWN"
        logger.warning(f"[ExternalDataAgent] Using normalized slug fallback: {fallback}")
        return fallback

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

    def _parse_ranges_tables(self, soup: BeautifulSoup):
        """
        Parse Screener.in's `table.ranges-table` blocks used for growth metrics.

        On the P&L tab, Screener renders Sales Growth, Profit Growth and
        Interest Coverage as small side-by-side tables with header text like
        "Compounded Sales Growth" and rows like:
          10 Years: | 14%
          5 Years:  | 16%
          3 Years:  | 13%
          TTM:      | 9%

        NOTE: Screener does NOT use <tbody> — <tr> elements sit directly inside
        <table>. We must use table.select("tr") not table.select("tbody tr").

        We prefer the 3-Year CAGR row as a stable signal.
        Returns (sales_growth, profit_growth, interest_coverage) as floats or None.
        """
        sales_g = None
        profit_g = None
        int_cov = None

        PREFER_ROW = ("3 years", "ttm", "5 years", "10 years")  # priority order

        for table in soup.select("table.ranges-table"):
            # Header can be inside <thead> or directly as <th>
            header_el = table.select_one("thead th") or table.select_one("th")
            if not header_el:
                continue
            header = header_el.get_text(strip=True).lower()

            rows = {}  # label.lower() -> raw_value_text
            # NOTE: Screener has no <tbody>; <tr> sits directly inside <table>
            for tr in table.select("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    label = tds[0].get_text(strip=True).lower().rstrip(":")
                    value = tds[1].get_text(strip=True)
                    rows[label] = value

            # Pick the best available value following priority order
            def pick_value(rows_dict=rows):
                for pref in PREFER_ROW:
                    val = rows_dict.get(pref)
                    if val and val not in ("", "%", "--", "N/A"):
                        return self._parse_numeric(val)
                return None

            if "sales growth" in header or "compounded sales" in header:
                if sales_g is None:
                    sales_g = pick_value()
            elif "profit growth" in header or "compounded profit" in header:
                if profit_g is None:
                    profit_g = pick_value()
            elif "interest coverage" in header:
                if int_cov is None:
                    int_cov = pick_value()

        return sales_g, profit_g, int_cov

    def _compute_interest_coverage_from_pl(self, soup: BeautifulSoup) -> Optional[float]:
        """Compute Interest Coverage Ratio from Screener's P&L table.

        Screener does NOT display Interest Coverage as a standalone metric.
        It must be calculated: Operating Profit / Interest expense.
        We use the most recent year's figures (last column in the P&L table).

        Returns the ratio as a float, or None if the data is unavailable.
        """
        try:
            pl_table = soup.select_one("section#profit-loss table.data-table")
            if not pl_table:
                return None

            op_profit = None
            interest = None

            for tr in pl_table.select("tbody tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                label = tds[0].get_text(strip=True).lower()
                # Get the last (most recent) column value
                last_val = tds[-1].get_text(strip=True) if len(tds) > 1 else None

                if label.startswith("operating profit") and not label.startswith("operating profit margin"):
                    op_profit = self._parse_numeric(last_val)
                elif label == "interest":
                    interest = self._parse_numeric(last_val)

            if op_profit is not None and interest not in (None, 0):
                ratio = round(op_profit / interest, 2)
                logger.debug(f"[ExternalDataAgent] Computed Interest Coverage: {op_profit}/{interest} = {ratio}")
                return ratio
        except Exception as e:
            logger.debug(f"[ExternalDataAgent] Interest coverage computation failed: {e}")

        return None

    def _compute_debt_from_bs(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract total debt from Screener's Balance Sheet table ('Borrowings' row).
        Values in this table are ALREADY in Crores.
        """
        try:
            bs_table = soup.select_one("section#balance-sheet table.data-table")
            if not bs_table:
                return None

            for tr in bs_table.select("tbody tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                label = tds[0].get_text(strip=True).lower()
                if label == "borrowings" or label.startswith("borrowings"):
                    last_val = tds[-1].get_text(strip=True) if len(tds) > 1 else None
                    # _parse_numeric strips commas. Since there's no "Cr.", it won't multiply by 1e7.
                    # It will just return the float value in Crores directly.
                    return self._parse_numeric(last_val)
        except Exception as e:
            logger.debug(f"[ExternalDataAgent] Debt extraction from BS failed: {e}")
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
                mcap = self._parse_numeric(self._get_value_by_label(soup, "Market Cap"))
                data["market_cap"] = round(mcap / 1e7, 2) if mcap is not None else None

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

                # Fetch Debt from Balance Sheet (already in Crores)
                data["debt"] = self._compute_debt_from_bs(soup)
                data["operating_margin"] = self._parse_numeric(
                    self._get_value_by_label(soup, "OPM") or
                    self._get_value_by_label(soup, "Operating margin")
                )
                data["interest_coverage"] = self._parse_numeric(
                    self._get_value_by_label(soup, "Interest coverage") or
                    self._get_value_by_label(soup, "Int Coverage")
                )

                # FIX: sales_growth, profit_growth live inside
                # `table.ranges-table` in the P&L section — NOT in the top-ratios
                # ul.  Parse them from the table directly.
                sales_g, profit_g, int_cov = self._parse_ranges_tables(soup)
                if data["interest_coverage"] is None:
                    data["interest_coverage"] = int_cov
                if sales_g is not None:
                    data["sales_growth"] = sales_g
                if profit_g is not None:
                    data["profit_growth"] = profit_g

                # Interest Coverage: Screener doesn't display this as a
                # standalone metric.  Compute from P&L: Operating Profit / Interest.
                if data["interest_coverage"] is None:
                    data["interest_coverage"] = self._compute_interest_coverage_from_pl(soup)

                # Compute risk score
                data["risk"] = self._compute_risk(data)

                risk_val = data['risk']
                risk_str = f"{risk_val:.2f}" if risk_val is not None else "None"
                logger.info(
                    f"[ExternalDataAgent] Scraped Screener data for {slug}: "
                    f"PE={data['pe']}, ROE={data['roe']}, ROCE={data['roce']}, "
                    f"Debt={data['debt']}, SalesG={data['sales_growth']}, "
                    f"ProfitG={data['profit_growth']}, IntCov={data['interest_coverage']}, "
                    f"Risk={risk_str}"
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
        # Use clean company name variants for NSE search (strip Pvt, try Ltd/Limited)
        search_names = self._name_variants(company_name)
        for sname in search_names:
            try:
                search_url = f"https://www.nseindia.com/api/search/autocomplete?q={quote_plus(sname)}"
                resp = self._session.get(search_url, timeout=15, headers={"Referer": "https://www.nseindia.com"})
                if resp.status_code == 200:
                    results = resp.json().get("symbols", [])
                    if results:
                        symbol = results[0].get("symbol") or symbol
                        break
            except Exception:
                continue

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
                market_cap = round((issued_cap * price) / 1e7, 2)  # Convert to Crores

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
        # Use clean name variants for BSE search (strip Pvt, try Ltd/Limited)
        search_names = self._name_variants(company_name)
        for sname in search_names:
            try:
                search_url = f"https://api.bseindia.com/BseIndiaAPI/api/SmartSearch/w?text={quote_plus(sname)}"
                search_resp = self._session.get(search_url, timeout=15, headers={"Referer": "https://www.bseindia.com"})
                if search_resp.status_code != 200:
                    continue
                results = search_resp.json()
                if not results:
                    continue
                scrip_code = results[0].get("ScripCode")
                if scrip_code:
                    break
            except Exception:
                continue
        else:
            return fallback

        if not scrip_code:
            return fallback

        try:
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
        """Fill missing NSE/BSE values using Screener.in (historical/advanced ratios).

        For private companies with no public data, values are left as NULL rather
        than using unreliable Tofler scraping or LLM estimation.
        """
        essential_keys = ["pe", "roe", "roce", "debt", "market_cap", "sales_growth", "profit_growth"]
        needs_fallback = any(data.get(k) is None for k in essential_keys)

        # Build accurate source tracking
        sources_used = []
        # Check which exchange sources actually returned data
        nse_contributed = any(data.get(k) is not None for k in ["pe", "roe", "market_cap"])
        if nse_contributed:
            sources_used.append("nse/bse")

        if needs_fallback:
            screener_data = self._fetch_screener_data(company_name, slug)
            screener_contributed = False
            for k in essential_keys + ["operating_margin", "interest_coverage"]:
                if data.get(k) is None and screener_data.get(k) is not None:
                    data[k] = screener_data.get(k)
                    screener_contributed = True
            if screener_contributed:
                sources_used.append("screener.in")

        # Build source string from actually-used sources only
        data["source"] = "+".join(sources_used) if sources_used else "none"
        data["confidence"] = self.CONFIDENCE_STRUCTURED if sources_used else 0.0

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

            if not company_name or bool(re.match(r'^cust_\d+$', company_name)):
                logger.debug(
                    f"[ExternalDataAgent] Skipping enrichment for {customer_id}: "
                    f"no real company name resolved (got '{company_name}')"
                )
                return

            company_name = company_name.strip()[:100]

            # Resolve slug (ticker)
            slug = self._resolve_slug(company_name)

            current_time = time.time()
            last_fetch = self._last_fetch_time.get(company_name, 0)
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
                        self._last_fetch_time[company_name] = max(self._last_fetch_time.get(company_name, 0), db_ts)
                except Exception:
                    self._last_fetch_time[company_name] = current_time

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
                self._last_fetch_time[company_name] = current_time

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

