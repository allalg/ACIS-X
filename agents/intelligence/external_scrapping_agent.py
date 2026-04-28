"""
External Scraping Agent for ACIS-X.

Generates litigation risk signals from NCLT cases and industry-level Google News RSS.
- Queries all 16 NCLT benches for pending cases in the last 5 years
- Falls back to industry-level news for sector stress context
- Merges signals into a unified litigation risk score

Subscribes to:
- acis.metrics (customer events)

Produces:
- acis.metrics (external.litigation.updated)
"""

import logging
import json
import os
import re
import threading
import time
import requests
import xml.etree.ElementTree as ET
import concurrent.futures
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
from typing import List, Any, Dict, Optional
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import OrderedDict

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient
from utils.circuit_breaker import CircuitBreaker, CircuitOpenError

_news_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS & CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

NCLT_SESSION_URL = "https://efiling.nclt.gov.in/casehistorybeforeloginmenutrue.drt"
NCLT_QUERY_URL = "https://efiling.nclt.gov.in/caseHistoryoptional.drt"

NCLT_BENCH_IDS = {
    1: "Ahmedabad", 2: "Allahabad", 3: "Bengaluru", 4: "Chandigarh",
    5: "Chennai", 6: "Guwahati", 7: "Hyderabad", 8: "Kolkata",
    9: "Mumbai", 10: "New Delhi / Principal", 11: "Jaipur",
    12: "Amaravati", 13: "Cuttack", 14: "Kochi", 15: "Indore",
    16: "Additional Bench"
}

NCLT_CASE_TYPE_SCORES = {
    "IBA": 1.0, "CIRP": 1.0, "LQD": 1.0,
    "PIB": 0.90, "CP": 0.85, "CC": 0.75,
    "CA": 0.60, "IA": 0.30, "MA": 0.20,
}




class ExternalScrapingAgent(BaseAgent):
    """
    External Scraping Agent for ACIS-X.
    Generates litigation risk by fusing NCLT case data with industry-level news.
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_CUSTOMERS = "acis.customers"
    TOPIC_OUTPUT = "acis.metrics"
    GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
    BING_NEWS_RSS_URL = "https://www.bing.com/news/search"

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="ExternalScrapingAgent",
            agent_version="4.0.0",
            group_id="litigation-agent-group",
            subscribed_topics=[self.TOPIC_INPUT, self.TOPIC_CUSTOMERS],
            capabilities=[
                "nclt_litigation_extraction",
                "industry_risk_detection",
                "litigation_risk_fusion"
            ],
            kafka_client=kafka_client,
            agent_type="ExternalScrapingAgent",
        )
        
        # Caches
        self._nclt_cache = {}      # TTL: 12h
        self._news_cache = {}      # TTL: 6h
        self._llm_cache = {}       # LLM response cache
        
        # Memory Store
        self._risk_history: Dict[str, List[Dict]] = {}
        self._MAX_HISTORY = 20
        
        # Deduplication tracker
        self._last_published_risk: OrderedDict = OrderedDict()
        self._MAX_RISK_TRACK = 5000

        # General session for RSS
        self._session = self._create_session(timeout=25, retries=3)
        
        # Dedicated session for NCLT
        self._nclt_session = self._create_session(timeout=5, retries=1)
        self._nclt_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Origin": "https://efiling.nclt.gov.in",
            "Referer": NCLT_SESSION_URL,
            "X-Requested-With": "XMLHttpRequest"
        })

        _news_breaker.on_state_change = self._on_news_breaker_change

        # ThreadPoolExecutor for non-blocking scraping.
        # 2 workers: NCLT + LLM calls are heavy; more workers would overwhelm
        # external APIs and the Groq rate limit.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix="ext-scrape",
        )

        # Per-customer in-flight guard: prevents duplicate concurrent submissions
        # for the same customer when events arrive faster than scraping completes.
        self._in_flight: dict = {}   # customer_id -> True
        self._in_flight_lock = threading.Lock()

        logger.info("[ExternalScrapingAgent] Initialized with NCLT + Industry News fusion")

    def _on_news_breaker_change(self, old_state: str, new_state: str) -> None:
        if new_state == "OPEN":
            logger.warning("Google News circuit OPEN — skipping news fetch")
        self.publish_event(
            topic="acis.monitoring",
            event_type="circuit_breaker.state_change",
            entity_id=self.agent_name,
            payload={
                "service": "google_news",
                "new_state": new_state,
                "agent_name": self.agent_name
            }
        )

    def _create_session(self, timeout: int, retries: int) -> requests.Session:
        """Create a resilient requests session."""
        session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        return session

    def subscribe(self) -> List[str]:
        return [self.TOPIC_INPUT, self.TOPIC_CUSTOMERS]

    def stop(self) -> None:
        """Stop agent and shut down the scraping thread pool."""
        super().stop()
        self._executor.shutdown(wait=False)

    def process_event(self, event: Event) -> None:
        # Ignore our own output to prevent self-triggering loops
        if event.event_type == "external.litigation.updated":
            return
            
        if event.event_type.startswith("customer.") or event.event_type.startswith("external."):
            self.handle_event(event)

    # ─────────────────────────────────────────────────────────────────────────
    # NCLT LOGIC
    # ─────────────────────────────────────────────────────────────────────────

    def _fetch_nclt_bench(self, company_name: str, bench_id: int) -> List[Dict]:
        """Query a single NCLT bench for cases.

        BUG FIX: The NCLT API wraps results in a dict:
          { "caseHheader": ..., "mainpanellist": [...], "errormsg": ... }
        The old code checked `isinstance(data, list)` and returned [] for every
        company because the top-level response is always a dict.
        Now we unwrap mainpanellist first.
        """
        try:
            payload = {
                "wayofselection": "partyname",
                "i_bench_id": "0",
                "filing_no": "",
                "i_bench_id_case_no": "0",
                "i_case_type_caseno": "0",
                "i_case_year_caseno": "0",
                "case_no": "",
                "i_party_search": "E",
                "i_bench_id_party": str(bench_id),
                "party_type_party": "R",  # Respondent — cases filed against company
                "party_name_party": company_name,
                "i_case_year_party": "0",
                "status_party": "P",  # Pending only
                "i_adv_search": "E",
                "i_bench_id_lawyer": "0",
                "party_lawer_name": "",
                "i_case_year_lawyer": "0",
                "bar_council_advocate": ""
            }

            response = self._nclt_session.post(NCLT_QUERY_URL, json=payload, timeout=20)

            if response.status_code != 200:
                logger.debug(f"[ExternalScrapingAgent] NCLT returned {response.status_code} for bench {bench_id}")
                return []

            raw = response.json()

            # --- FIX: Unwrap the dict envelope ---
            # API always returns a dict with 'mainpanellist' containing the actual cases.
            # A bare list response is also handled for forward-compatibility.
            if isinstance(raw, list):
                items = raw
            elif isinstance(raw, dict):
                items = raw.get("mainpanellist")
                if not items or not isinstance(items, list):
                    # errormsg = "No Details" means genuinely no cases, not an error
                    err = raw.get("errormsg", "")
                    if err:
                        logger.debug(f"[ExternalScrapingAgent] NCLT bench {bench_id} ({NCLT_BENCH_IDS.get(bench_id)}): {err}")
                    return []
            else:
                return []

            cases = []
            cutoff_date = datetime.now() - timedelta(days=5 * 365)

            for item in items:
                case_no      = item.get("case_no", "")
                case_type    = item.get("case_type", "")
                filing_date_str = item.get("filing_date", "")
                status       = item.get("status", "Pending")

                if status.upper() != "PENDING":
                    continue

                if filing_date_str:
                    try:
                        parts = re.split(r'[-/]', filing_date_str)
                        if len(parts) == 3:
                            if len(parts[2]) == 4:  # dd-mm-yyyy
                                f_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                            else:                    # yyyy-mm-dd
                                f_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                            if f_date < cutoff_date:
                                continue
                    except Exception:
                        pass

                cases.append({
                    "case_no": case_no,
                    "case_type": case_type,
                    "bench": NCLT_BENCH_IDS.get(bench_id, "Unknown"),
                    "filing_date": filing_date_str,
                    "status": status,
                })

            return cases

        except Exception as e:
            logger.debug(f"[ExternalScrapingAgent] NCLT fetch failed for bench {bench_id}: {e}")
            return []

    def _fetch_nclt_all_benches(self, company_name: str) -> List[Dict]:
        """Query all NCLT benches and deduplicate results.

        BUG 4 FIX: A failed session prime no longer aborts the entire fetch.
        Many NCLT benches respond without a pre-session cookie; aborting on a
        5-second prime timeout was silently returning 0 cases for every company.
        """
        all_cases = []
        seen_case_nos = set()

        # Prime the session cookie — best-effort only, do NOT abort on failure
        prime_ok = False
        try:
            self._nclt_session.get(NCLT_SESSION_URL, timeout=10)
            prime_ok = True
        except Exception as e:
            logger.warning(
                f"[ExternalScrapingAgent] NCLT session prime failed: {e}. "
                f"Proceeding to query benches without cookie."
            )

        # Fetch from all benches concurrently (max 5 workers to be polite)
        def fetch_bench(bench_id):
            return self._fetch_nclt_bench(company_name, bench_id)

        benches_attempted = 0
        benches_succeeded = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_bench = {executor.submit(fetch_bench, bid): bid for bid in NCLT_BENCH_IDS.keys()}
            for future in concurrent.futures.as_completed(future_to_bench):
                bench_id = future_to_bench[future]
                benches_attempted += 1
                try:
                    cases = future.result()
                    benches_succeeded += 1
                    for c in cases:
                        cno = c.get("case_no")
                        if cno and cno not in seen_case_nos:
                            seen_case_nos.add(cno)
                            all_cases.append(c)
                except Exception as e:
                    logger.debug(
                        f"[ExternalScrapingAgent] Concurrent fetch failed for bench {bench_id}: {e}"
                    )

        logger.info(
            f"[ExternalScrapingAgent] NCLT fetch complete for '{company_name}': "
            f"prime_ok={prime_ok}, benches={benches_succeeded}/{benches_attempted}, "
            f"cases_found={len(all_cases)}"
        )
        return all_cases



    def _unified_llm_analysis(
        self,
        company_name: str,
        industry: str,
        nclt_cases: List[Dict],
        articles: List[Dict],
        casemine_data: Dict,
        macro_articles: List[Dict] = None,
        aliases: List[str] = None,
    ) -> Dict:
        """
        Single LLM call that sees ALL evidence and returns one risk score + explanation.
        Sources: NCLT pending cases + company news + CaseMine + macro/regulatory context.
        """
        empty = {
            "final_risk": 0.0,
            "severity": "low",
            "confidence": 0.5,
            "evidence": "No data available for analysis.",
            "analysis_status": "skipped_no_data",
            "risk_types": [],
            "nclt_case_count": len(nclt_cases),
            "nclt_cases": [c.get("case_no") for c in nclt_cases],
            "casemine_total": casemine_data.get("total_hits", 0) if casemine_data else 0,
            "casemine_high_risk_courts": casemine_data.get("high_risk_courts", []) if casemine_data else [],
        }

        has_nclt     = bool(nclt_cases)
        has_news     = bool(articles)
        has_casemine = bool(casemine_data and casemine_data.get("sample_cases"))
        has_macro    = bool(macro_articles)

        if not has_nclt and not has_news and not has_casemine and not has_macro:
            empty["evidence"] = "No NCLT cases, news, CaseMine, or macro data found."
            return empty

        # ── NCLT section ──────────────────────────────────────────────────────
        if has_nclt:
            lines = []
            for c in nclt_cases[:20]:
                lines.append(
                    f"  [{c.get('bench','?')}] {c.get('case_no','?')} "
                    f"type={c.get('case_type','?')} filed={c.get('filing_date','?')}"
                )
            nclt_section = f"NCLT PENDING CASES ({len(nclt_cases)} total):\n" + "\n".join(lines)
        else:
            nclt_section = "NCLT PENDING CASES: None found across all 16 benches."

        # ── News section (grouped by tier) ────────────────────────────────────
        tier_labels = {
            "recent_general":      "RECENT NEWS (last 7 days)",
            "legal_events":        "LEGAL & REGULATORY EVENTS (last 60 days)",
            "resolution_tracking": "RESOLUTION / OUTCOME TRACKING (last 60 days)",
        }
        if has_news:
            # Group articles by tier, filter relevant ones per tier
            news_by_tier: Dict[str, List[Dict]] = {}
            for a in articles:
                tier = a.get("news_tier", "recent_general")
                text = a.get("title", "") + " " + a.get("description", "")
                if self._is_relevant(text, company_name, aliases=aliases):
                    news_by_tier.setdefault(tier, []).append(a)

            news_lines = []
            tier_order = ["recent_general", "legal_events", "resolution_tracking"]
            for tier in tier_order:
                items = news_by_tier.get(tier, [])[:5]
                if items:
                    news_lines.append(f"[{tier_labels.get(tier, tier)}]")
                    for i, a in enumerate(items, 1):
                        pub = a.get("pubDate", "")
                        age = ""
                        if pub:
                            try:
                                pd = parsedate_to_datetime(pub)
                                age_days = (datetime.now(timezone.utc) - pd).days
                                age = f" ({age_days}d ago)"
                            except Exception:
                                pass
                        news_lines.append(f"  {i}. {a.get('title', '')}{age}")
                        if a.get("description"):
                            news_lines.append(f"     {a['description'][:150]}")

            if news_lines:
                news_section = "COMPANY NEWS:\n" + "\n".join(news_lines)
            else:
                news_section = "COMPANY NEWS: No company-specific articles found across all tiers."
        else:
            news_section = "COMPANY NEWS: No articles fetched."

        # ── CaseMine section ──────────────────────────────────────────────────
        if has_casemine:
            cm = casemine_data
            cm_lines = [f"  Total indexed judgments: {cm.get('total_hits', 0)}"]
            if cm.get("high_risk_courts"):
                cm_lines.append(f"  High-risk courts: {', '.join(cm['high_risk_courts'])}")
            for c in cm.get("sample_cases", [])[:6]:
                cm_lines.append(f"  [{c['court']}] {c['title']} ({c['date']})")
            cm_section = "CASEMINE COURT RECORDS:\n" + "\n".join(cm_lines)
        else:
            cm_section = "CASEMINE COURT RECORDS: Not available."

        # ── Macro / regulatory section ────────────────────────────────────────
        if has_macro:
            macro_by_type: Dict[str, List[str]] = {}
            for a in (macro_articles or []):
                qt = a.get("query_type", "general")
                macro_by_type.setdefault(qt, []).append(a.get("title", ""))
            macro_lines = []
            type_labels = {
                "company_regulatory": "Company Regulatory/Enforcement",
                "industry_regulatory": "Industry Law & Policy",
                "macro_political": "Macro/Political/Sector Risk",
            }
            for qt, titles in macro_by_type.items():
                label = type_labels.get(qt, qt)
                macro_lines.append(f"  [{label}]")
                for t in titles[:5]:
                    macro_lines.append(f"    - {t}")
            macro_section = "MACRO & REGULATORY CONTEXT (web search):\n" + "\n".join(macro_lines)
        else:
            macro_section = "MACRO & REGULATORY CONTEXT: No additional signals found."

        # ── LLM prompt ────────────────────────────────────────────────────────
        prompt = (
            f"You are a senior credit-risk analyst specialising in Indian corporate litigation "
            f"and regulatory risk.\n\n"
            f"Analyse ALL FOUR evidence sources below for {company_name} (industry: {industry}) "
            f"and produce a single unified litigation + regulatory risk score.\n\n"
            f"{nclt_section}\n\n"
            f"{news_section}\n\n"
            f"{cm_section}\n\n"
            f"{macro_section}\n\n"
            f"SCORING GUIDANCE:\n"
            f"- Active NCLT/IBC insolvency cases (no resolution)  = very high risk (0.7-1.0)\n"
            f"- NCLT/IBC case with stay granted or petition stayed = medium risk (0.3-0.5)\n"
            f"- NCLT case dismissed / petition withdrawn           = low risk (0.0-0.2)\n"
            f"- SEBI/CCI/RBI enforcement or criminal HC (active)   = high risk (0.5-0.8)\n"
            f"- Enforcement action resolved / penalty paid         = low-medium risk (0.1-0.3)\n"
            f"- New law/regulation creating sector headwind        = medium-high risk (0.4-0.6)\n"
            f"- Consumer forum cases at scale                     = medium risk (0.3-0.5)\n"
            f"- Routine tax/customs disputes                       = low risk (0.0-0.2)\n"
            f"- No material issues                                 = 0.0\n\n"
            f"CRITICAL RESOLUTION RULES:\n"
            f"- The RESOLUTION TRACKING tier shows outcomes (stay orders, dismissals, settlements).\n"
            f"- If a legal event was FILED (legal_events tier) but then STAYED or DISMISSED "
            f"(resolution_tracking tier), the risk must reflect the CURRENT STATUS, not the filing.\n"
            f"- A stay order means the issue is not resolved — medium risk remains.\n"
            f"- A dismissed petition = risk substantially reduced.\n"
            f"- Always check if NCLT cases listed in NCLT PENDING CASES are truly still active "
            f"or have been stayed/dismissed per news evidence.\n\n"
            f"INSTRUCTIONS:\n"
            f"1. NCLT case count + type are the strongest signal — weigh heavily.\n"
            f"2. Cross-check RESOLUTION TRACKING news to see if NCLT cases are stayed or resolved.\n"
            f"3. Use Macro/Regulatory context to identify sector-wide headwinds.\n"
            f"4. If a regulatory action was resolved or penalty paid, note it and reduce score.\n"
            f"5. Ignore routine tax disputes for large public companies.\n"
            f"6. Write a SINGLE 3-5 sentence explanation covering current status — "
            f"specifically call out if major risks are ACTIVE, STAYED, or RESOLVED.\n\n"
            f"OUTPUT — strict JSON only, no markdown:\n"
            f'{{"risk_score": 0.0, "severity": "low|medium|high", "confidence": 0.0, '
            f'"risk_types": ["insolvency", "regulatory", "macro", ...], '
            f'"legal_status": "active|stayed|resolved|mixed", '
            f'"explanation": "3-5 sentence summary with current legal status", '
            f'"nclt_flag": true, "news_flag": false, "macro_flag": false}}'
        )

        response = self._call_llm(prompt)

        if not response:
            logger.warning(f"[ExternalScrapingAgent] LLM analysis failed for {company_name}")
            # Fallback: keyword score if LLM is down
            fallback_risk = 0.0
            if has_nclt:
                scores = [NCLT_CASE_TYPE_SCORES.get(c.get("case_type", "").split("/")[0], 0.40)
                          for c in nclt_cases]
                fallback_risk = round(min(1.0, max(scores)), 4) if scores else 0.0
            empty.update({
                "final_risk": fallback_risk,
                "severity": "high" if fallback_risk >= 0.7 else "medium" if fallback_risk >= 0.4 else "low",
                "confidence": 0.3,
                "evidence": f"[LLM unavailable] Keyword fallback: {len(nclt_cases)} NCLT cases found.",
                "analysis_status": "llm_failed_fallback",
            })
            return empty

        risk_score  = round(min(1.0, max(0.0, float(response.get("risk_score", 0.0)))), 4)
        severity    = response.get("severity", "low").lower()
        confidence  = round(min(0.95, float(response.get("confidence", 0.7))), 4)
        explanation  = response.get("explanation", "")
        risk_types   = response.get("risk_types", [])
        legal_status = response.get("legal_status", "unknown").lower()

        if severity not in ("low", "medium", "high"):
            severity = "high" if risk_score >= 0.5 else "medium" if risk_score >= 0.25 else "low"
        if legal_status not in ("active", "stayed", "resolved", "mixed", "unknown"):
            legal_status = "unknown"

        logger.info(
            f"[ExternalScrapingAgent] Unified LLM: company={company_name} "
            f"risk={risk_score} severity={severity} status={legal_status} "
            f"nclt={len(nclt_cases)} conf={confidence}"
        )

        return {
            "final_risk": risk_score,
            "severity": severity,
            "confidence": confidence,
            "evidence": explanation,
            "legal_status": legal_status,
            "analysis_status": "llm_unified",
            "risk_types": risk_types,
            "nclt_case_count": len(nclt_cases),
            "nclt_cases": [c.get("case_no") for c in nclt_cases],
            "casemine_total": casemine_data.get("total_hits", 0) if casemine_data else 0,
            "casemine_high_risk_courts": casemine_data.get("high_risk_courts", []) if casemine_data else [],
        }

    # ─────────────────────────────────────────────────────────────────────────
    # INDUSTRY NEWS LOGIC
    # ─────────────────────────────────────────────────────────────────────────



    def _fetch_company_news_risk(self, company_name: str, aliases: List[str] = None) -> List[Dict]:
        """
        Tiered company news fetch — three separate search passes with different
        time windows and query strategies:

        Tier 1 — RECENT (7 days)
          General company news: business updates, earnings, leadership changes.

        Tier 2 — LEGAL EVENTS (60 days)
          Targeted legal/regulatory queries: insolvency petitions, NCLT filings,
          court orders, SEBI/CCI enforcement, fraud allegations, winding-up.
          Longer window because legal events unfold slowly.

        Tier 3 — RESOLUTION (60 days)
          Explicitly searches for outcomes: stay orders, petition dismissals,
          acquittals, out-of-court settlements, penalty reversals.
          Crucial for cases like Dream11 where a petition was filed then stayed.

        Each article is tagged with `news_tier` so the LLM knows the temporal
        and semantic context of each result.
        """
        all_articles: List[Dict] = []
        seen: set = set()

        # Build name variants for queries
        short_name = re.sub(
            r'\b(limited|ltd|pvt|private|inc|corp|corporation|llp|llc)\b', '',
            company_name, flags=re.IGNORECASE
        ).strip().strip(".")

        name_terms = [f'"{company_name}"']
        if short_name and short_name.lower() != company_name.lower():
            name_terms.append(f'"{short_name}"')
        if aliases:
            for a in aliases:
                if a and len(a) >= 3 and a.lower() not in (company_name.lower(), short_name.lower()):
                    name_terms.append(f'"{a}"')
        name_or = " OR ".join(name_terms[:4])  # cap to avoid overly long query

        tiers = [
            # Tier 1: Recent general news (30 days — broader recent context)
            {
                "query": name_or,
                "days": 30,
                "tier": "recent_general",
                "max_items": 10,
            },
            # Tier 2: Legal events over past 365 days (1 year)
            {
                "query": (
                    f"({name_or}) AND ("
                    f"insolvency OR NCLT OR \"winding up\" OR \"court order\" OR lawsuit OR "
                    f"\"petition filed\" OR SEBI OR RBI OR CCI OR MCA OR fraud OR "
                    f"\"criminal case\" OR \"FIR\" OR penalty OR \"tax demand\" OR "
                    f"\"enforcement\" OR \"show cause\" OR \"IBC\")"
                ),
                "days": 365,
                "tier": "legal_events",
                "max_items": 15,
            },
            # Tier 3: Resolution / outcome tracking (365 days — 1 year)
            {
                "query": (
                    f"({name_or}) AND ("
                    f"\"stay order\" OR \"stay granted\" OR dismissed OR acquitted OR "
                    f"\"petition dismissed\" OR \"order quashed\" OR \"relief granted\" OR "
                    f"settlement OR \"appeal allowed\" OR \"High Court\" OR \"Supreme Court\" OR "
                    f"\"NCLAT\" OR \"order set aside\" OR resolved OR \"out of court\")"
                ),
                "days": 365,
                "tier": "resolution_tracking",
                "max_items": 15,
            },
        ]

        def _rss_fetch(encoded_q: str, days: int, tier: str, max_items: int) -> List[Dict]:
            results = []
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)

            # Google News
            try:
                url = f"{self.GOOGLE_NEWS_RSS_URL}?q={encoded_q}&hl=en-IN&gl=IN&ceid=IN:en"
                resp = _news_breaker.call(self._session.get, url, timeout=20)
                if resp.status_code == 200:
                    root = ET.fromstring(resp.content)
                    for item in root.findall(".//item")[:max_items]:
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = item.findtext("pubDate")
                        if not title or title.lower() in seen:
                            continue
                        if pub:
                            try:
                                if parsedate_to_datetime(pub) < cutoff:
                                    continue
                            except Exception:
                                pass
                        seen.add(title.lower())
                        results.append({"title": title, "description": desc, "pubDate": pub,
                                        "source": "google_news", "news_tier": tier})
            except CircuitOpenError:
                pass
            except Exception as e:
                logger.debug(f"[ExternalScrapingAgent] Google ({tier}) failed: {e}")

            # Bing News
            try:
                url = f"{self.BING_NEWS_RSS_URL}?q={encoded_q}&format=rss"
                resp = self._session.get(url, timeout=20)
                if resp.status_code == 200:
                    root = ET.fromstring(resp.content)
                    for item in root.findall(".//item")[:max_items]:
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = item.findtext("pubDate")
                        if not title or title.lower() in seen:
                            continue
                        if pub:
                            try:
                                if parsedate_to_datetime(pub) < cutoff:
                                    continue
                            except Exception:
                                pass
                        seen.add(title.lower())
                        results.append({"title": title, "description": desc, "pubDate": pub,
                                        "source": "bing_news", "news_tier": tier})
            except Exception as e:
                logger.debug(f"[ExternalScrapingAgent] Bing ({tier}) failed: {e}")

            return results

        for t in tiers:
            encoded = quote_plus(t["query"])
            batch = _rss_fetch(encoded, t["days"], t["tier"], t["max_items"])
            all_articles.extend(batch)
            logger.debug(
                f"[ExternalScrapingAgent] News tier={t['tier']} "
                f"company={company_name} fetched={len(batch)}"
            )

        logger.info(
            f"[ExternalScrapingAgent] News fetch complete: company={company_name} "
            f"total={len(all_articles)} "
            f"(general={sum(1 for a in all_articles if a['news_tier']=='recent_general')}, "
            f"legal={sum(1 for a in all_articles if a['news_tier']=='legal_events')}, "
            f"resolution={sum(1 for a in all_articles if a['news_tier']=='resolution_tracking')})"
        )
        return all_articles[:40]  # cap; LLM sees top-5 per tier anyway


    def _fetch_macro_context(self, company_name: str, industry: str) -> List[Dict]:
        """
        Fetch industry-level macro signals: regulatory changes, new laws, political
        risk, sector-wide enforcement actions, and government policy shifts.

        Runs THREE targeted queries:
          1. Company-specific regulatory/enforcement news
          2. Industry-level regulatory & law changes
          3. RBI / SEBI / MCA / CCI / government policy affecting the sector

        Returns a flat list of article dicts (title, description, pubDate, source, query_type).
        Cached for 3h (shorter than company news since macro events move faster).
        """
        results: List[Dict] = []
        seen: set = set()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=365)   # 1-year window for macro/regulatory news

        # Derive a short sector label for queries
        sector = industry.lower() if industry and industry != "unknown" else ""

        # Company short name (strip "Ltd", "Pvt" etc)
        short_name = re.sub(
            r'\b(limited|ltd|pvt|private|inc|corp|corporation|llp|llc)\b', '',
            company_name, flags=re.IGNORECASE
        ).strip().strip(".")

        queries = [
            # 1. Company-level regulatory / enforcement
            {
                "q": f'"{short_name}" (SEBI OR RBI OR MCA OR CCI OR penalty OR notice OR ban OR enforcement OR regulation)',
                "type": "company_regulatory",
            },
            # 2. Industry-level law / regulation changes
            {
                "q": (
                    f'India {sector} (regulation OR law OR policy OR "government order" OR '
                    f'"new rule" OR compliance OR reform OR enforcement OR ban OR penalty) 2024 OR 2025'
                ),
                "type": "industry_regulatory",
            },
            # 3. Macro / political / sector-wide risk
            {
                "q": (
                    f'India {sector} (insolvency OR "IBC amendment" OR "SEBI circular" OR '
                    f'"RBI directive" OR "ministry notification" OR "budget impact" OR '
                    f'"court ruling" OR "Supreme Court" OR political) 2024 OR 2025'
                ),
                "type": "macro_political",
            },
        ]

        for q_info in queries:
            encoded = quote_plus(q_info["q"])
            q_type  = q_info["type"]

            # Google News RSS
            try:
                url = f"{self.GOOGLE_NEWS_RSS_URL}?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
                resp = _news_breaker.call(self._session.get, url, timeout=15)
                if resp.status_code == 200:
                    root = ET.fromstring(resp.content)
                    for item in root.findall(".//item")[:8]:
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = item.findtext("pubDate")
                        if not title or title.lower() in seen:
                            continue
                        if pub:
                            try:
                                if parsedate_to_datetime(pub) < cutoff_date:
                                    continue
                            except Exception:
                                pass
                        seen.add(title.lower())
                        results.append({
                            "title": title, "description": desc,
                            "pubDate": pub, "source": "google_news",
                            "query_type": q_type,
                        })
            except CircuitOpenError:
                pass
            except Exception as e:
                logger.debug(f"[ExternalScrapingAgent] Macro Google fetch ({q_type}) failed: {e}")

            # Bing News RSS (supplemental)
            try:
                url = f"{self.BING_NEWS_RSS_URL}?q={encoded}&format=rss"
                resp = self._session.get(url, timeout=15)
                if resp.status_code == 200:
                    root = ET.fromstring(resp.content)
                    for item in root.findall(".//item")[:6]:
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = item.findtext("pubDate")
                        if not title or title.lower() in seen:
                            continue
                        if pub:
                            try:
                                if parsedate_to_datetime(pub) < cutoff_date:
                                    continue
                            except Exception:
                                pass
                        seen.add(title.lower())
                        results.append({
                            "title": title, "description": desc,
                            "pubDate": pub, "source": "bing_news",
                            "query_type": q_type,
                        })
            except Exception as e:
                logger.debug(f"[ExternalScrapingAgent] Macro Bing fetch ({q_type}) failed: {e}")

        logger.info(
            f"[ExternalScrapingAgent] Macro context for '{company_name}' "
            f"(industry={industry}): {len(results)} articles"
        )
        return results[:24]   # cap total to keep prompt manageable

    def _fetch_casemine(self, company_name: str) -> Dict:
        """
        Fetch litigation case metadata from CaseMine for a company.

        Returns:
          total_hits   : int   — total indexed judgments (volume signal)
          high_risk_courts: list — courts that indicate serious litigation
                            (NCLT, HC criminal, CCI, Consumer)
          sample_cases : list — up to 8 case dicts {title, court, date}
          court_breakdown: dict — {court_name: count}

        High-risk courts used as a signal (tax disputes excluded as low-risk for
        large corporates — they are routine).
        """
        try:
            url = f"https://www.casemine.com/search/in/{quote_plus(company_name)}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Referer": "https://www.casemine.com/",
            }
            r = self._session.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                return {"total_hits": 0, "court_breakdown": {}, "sample_cases": [], "high_risk_courts": []}

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "lxml")

            # --- Total hit count (JS variable srHits) ---
            total_hits = 0
            for script in soup.find_all("script"):
                txt = script.string or ""
                m = re.search(r"var\s+srHits\s*=\s*'(\d+)'", txt)
                if m:
                    total_hits = int(m.group(1))
                    break

            # --- Court breakdown from filter sidebar ---
            # The sidebar renders lines like: "Income Tax Appellate Tribunal | 993"
            # inside elements with class containing 'overlay_c2'
            court_breakdown = {}
            for el in soup.find_all(class_=re.compile(r"overlay_c2")):
                raw_text = el.get_text(separator="\n", strip=True)
                for line in raw_text.split("\n"):
                    line = line.strip().lstrip("+ ").strip()
                    # Match: "<Court Name>" followed by a standalone integer on the next line
                    # or patterns like "Court Name 993"
                    m2 = re.match(r"^(.+?)\s+(\d+)$", line)
                    if m2:
                        court_name = m2.group(1).strip()
                        count = int(m2.group(2))
                        if len(court_name) > 3 and count > 0:
                            court_breakdown[court_name] = court_breakdown.get(court_name, 0) + count

            # --- Sample case cards ---
            sample_cases = []
            seen_titles = set()
            for a in soup.find_all("a", href=re.compile(r"/judgement/in/")):
                title = a.get_text(strip=True)
                if len(title) < 10 or title in seen_titles:
                    continue
                seen_titles.add(title)
                # Walk up to find context block with Court + Date
                block = a
                for _ in range(6):
                    block = block.parent
                    txt = block.get_text(separator="|", strip=True)
                    if len(txt) > 80:
                        break
                court = ""
                date  = ""
                cm = re.search(r"Court:\s*\|?\s*([^|]{5,60})", txt)
                dm = re.search(r"Date:\s*\|?\s*([^|]{5,30})", txt)
                if cm: court = cm.group(1).strip()
                if dm: date  = dm.group(1).strip()
                sample_cases.append({"title": title[:100], "court": court, "date": date})
                if len(sample_cases) >= 8:
                    break

            # --- Classify high-risk courts ---
            HIGH_RISK_COURT_KEYWORDS = [
                "nclt", "national company law", "insolvency",
                "competition commission", "cci",
                "criminal", "consumer", "debt recovery",
                "securities appellate", "sebi",
            ]
            LOW_RISK_COURT_KEYWORDS = [
                "income tax", "itat", "customs", "cestat", "gst",
            ]
            high_risk_courts = [
                court for court in court_breakdown
                if any(kw in court.lower() for kw in HIGH_RISK_COURT_KEYWORDS)
                and not any(kw in court.lower() for kw in LOW_RISK_COURT_KEYWORDS)
            ]

            logger.info(
                f"[ExternalScrapingAgent] CaseMine: company={company_name}, "
                f"total_hits={total_hits}, high_risk_courts={high_risk_courts}"
            )
            return {
                "total_hits": total_hits,
                "court_breakdown": court_breakdown,
                "sample_cases": sample_cases,
                "high_risk_courts": high_risk_courts,
            }
        except Exception as e:
            logger.warning(f"[ExternalScrapingAgent] CaseMine fetch failed for {company_name}: {e}")
            return {"total_hits": 0, "court_breakdown": {}, "sample_cases": [], "high_risk_courts": []}

    def _is_relevant(self, text: str, company: str, aliases: List[str] = None) -> bool:
        text = text.lower()
        company = company.lower()
        
        clean_company = company.replace("limited", "").replace("ltd", "").replace("pvt", "").replace("private", "").strip()
        variants = set([company, clean_company])
        
        words = clean_company.split()
        if len(words) > 1:
            acronym = "".join([w[0] for w in words if w]).lower()
            if len(acronym) >= 3:
                variants.add(acronym)
                
        if words and len(words[0]) >= 4:
            variants.add(words[0])

        if aliases:
            for alias in aliases:
                if alias and len(alias) >= 3:
                    variants.add(alias.lower())

        variants = {v for v in variants if len(v) >= 3}
        return any(v in text for v in variants)

    def _call_llm(self, prompt: str) -> dict:
        """Call Groq API for LLM-based extraction."""
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            logger.error("[ExternalScrapingAgent] GROQ_API_KEY not found.")
            return {}

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        system_prompt = "You are a financial risk intelligence system. Extract structured risk signals about a company from news articles. Be precise and avoid false positives."
        
        data = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"}
        }
        
        try:
            response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=data, timeout=30)
            if response.status_code == 200:
                result = response.json()
                content = result["choices"][0]["message"]["content"]
                try:
                    return json.loads(content)
                except Exception:
                    logger.error("[ExternalScrapingAgent] Failed to parse LLM JSON")
                    return {}
            else:
                logger.error(f"[ExternalScrapingAgent] Groq API error: {response.text}")
                return {}
        except Exception as e:
            logger.error(f"[ExternalScrapingAgent] LLM call failed: {e}")
            return {}

    def _build_multi_article_context(self, articles: List[Dict]) -> str:
        context_blocks = []
        for i, article in enumerate(articles[:5]):  # limit to 5
            title = article.get("title", "")
            desc = article.get("description", "")
            # calculate age
            pub_date_str = article.get("pubDate")
            age_info = ""
            if pub_date_str:
                try:
                    pub_date = parsedate_to_datetime(pub_date_str)
                    age_hours = (datetime.now(timezone.utc) - pub_date).total_seconds() / 3600
                    age_info = f"\nAge: {round(age_hours, 1)} hours"
                except Exception:
                    pass
            
            context_blocks.append(
                f"[Article {i+1}]\nTitle: {title}\nContent: {desc}{age_info}"
            )
        return "\n\n".join(context_blocks)

    def _analyze_company_news(self, articles: List[Dict], company_name: str,
                              industry: str = "unknown", aliases: List[str] = None,
                              casemine_data: Dict = None) -> Dict:
        """Analyze news articles + CaseMine case context via LLM.

        casemine_data (optional): output of _fetch_casemine().
        When provided, case titles and courts are appended to the LLM prompt
        as additional structured evidence so it can classify case severity
        without needing a raw case_type field.
        """
        if not articles and (not casemine_data or not casemine_data.get("sample_cases")):
            return {
                "litigation_risk_score": 0.0,
                "articles_analyzed": 0,
                "risk": 0.0,
                "evidence": "No relevant news found.",
                "analysis_status": "skipped_no_articles",
                "casemine_total": casemine_data.get("total_hits", 0) if casemine_data else 0,
                "casemine_high_risk_courts": casemine_data.get("high_risk_courts", []) if casemine_data else [],
            }

        # Pre-filter
        relevant_articles = [
            a for a in articles
            if self._is_relevant(a.get("title","") + " " + a.get("description",""), company_name, aliases=aliases)
        ][:5]

        if not relevant_articles:
            return {
                "litigation_risk_score": 0.0,
                "articles_analyzed": 0,
                "risk": 0.0,
                "evidence": "No relevant articles after filtering.",
                "analysis_status": "skipped_filtered",
                "casemine_total": casemine_data.get("total_hits", 0) if casemine_data else 0,
                "casemine_high_risk_courts": casemine_data.get("high_risk_courts", []) if casemine_data else [],
            }

        context = self._build_multi_article_context(relevant_articles)

        # Build CaseMine context block if available
        casemine_context = ""
        if casemine_data and casemine_data.get("sample_cases"):
            cm_lines = []
            for c in casemine_data["sample_cases"][:6]:
                cm_lines.append(f"  - [{c['court']}] {c['title']} ({c['date']})")
            if casemine_data.get("high_risk_courts"):
                cm_lines.insert(0, f"High-risk courts: {', '.join(casemine_data['high_risk_courts'])}")
            cm_lines.insert(0, f"Total indexed judgments: {casemine_data.get('total_hits', 0)}")
            casemine_context = "\n".join(cm_lines)

        # Pre-compute the CaseMine section outside the outer f-string to avoid
        # Python <3.12 restriction on backslashes inside f-string expressions.
        casemine_section = (
            "\nCOURT CASE RECORDS (CaseMine):\n" + casemine_context
        ) if casemine_context else ""

        prompt = f"""You are an advanced financial risk intelligence system.

You are given MULTIPLE news articles and court case records about a company.

Your job is to:
- Analyze all evidence together
- Identify consistent and material risk signals
- Classify the type of legal/regulatory risk
- Ignore routine tax disputes and isolated weak signals
- Avoid false positives

---

COMPANY:
{company_name}

INDUSTRY:
{industry}

NEWS ARTICLES:
{context}{casemine_section}

---

TASK:

1. Determine if there is a CONSISTENT and MATERIAL risk signal.

2. Consider:
   - Are multiple sources reporting the same issue?
   - Is the issue serious (fraud, insolvency, criminal, regulatory ban, consumer fraud)?
   - Court type matters: NCLT/IBC > HC Criminal > CCI/SEBI > Consumer > ITAT/Tax
   - Is the company clearly the primary subject?

3. Ignore:
   - Routine income tax / customs disputes (normal for large companies)
   - Single weak mentions
   - Unrelated companies
   - Generic industry news

---

OUTPUT (STRICT JSON):

{{
  "risk_detected": true/false,
  "risk_types": [],
  "severity": "low/medium/high",
  "confidence": 0.0,
  "summary": "2-3 line explanation combining all evidence",
  "evidence_articles": [],
  "signal_count": 0
}}"""

        response = self._call_llm(prompt)

        if not response:
            return {
                "litigation_risk_score": 0.0,
                "articles_analyzed": len(relevant_articles),
                "risk": 0.0,
                "evidence": "LLM analysis failed. Defaulting to safe score.",
                "analysis_status": "failed",
                "casemine_total": casemine_data.get("total_hits", 0) if casemine_data else 0,
                "casemine_high_risk_courts": casemine_data.get("high_risk_courts", []) if casemine_data else [],
            }

        if not response.get("risk_detected"):
            return {
                "litigation_risk_score": 0.0,
                "articles_analyzed": len(relevant_articles),
                "risk": 0.0,
                "evidence": "No consistent risk signals.",
                "analysis_status": "success_no_risk",
                "casemine_total": casemine_data.get("total_hits", 0) if casemine_data else 0,
                "casemine_high_risk_courts": casemine_data.get("high_risk_courts", []) if casemine_data else [],
            }

        severity_map = {"low": 0.3, "medium": 0.6, "high": 1.0}
        severity = response.get("severity", "low").lower()
        confidence = float(response.get("confidence", 0.5))
        signal_count = int(response.get("signal_count", 1))
        
        # Clamp confidence
        confidence = min(confidence, 0.85)

        # Time decay logic: bounded and robust
        decay = 1.0
        evidence_indices = response.get("evidence_articles", [])
        
        valid_pub_dates = []
        if isinstance(evidence_indices, list):
            for idx in evidence_indices:
                try:
                    i = int(idx) - 1
                    if 0 <= i < len(relevant_articles):
                        pub_date_str = relevant_articles[i].get("pubDate")
                        if pub_date_str:
                            valid_pub_dates.append(parsedate_to_datetime(pub_date_str))
                except Exception:
                    pass
                    
        # Fallback to all relevant articles if indices were bad
        if not valid_pub_dates:
            for article in relevant_articles:
                pub_date_str = article.get("pubDate")
                if pub_date_str:
                    try:
                        valid_pub_dates.append(parsedate_to_datetime(pub_date_str))
                    except Exception:
                        pass

        if valid_pub_dates:
            min_age_hours = min([max(0, (datetime.now(timezone.utc) - pd).total_seconds() / 3600) for pd in valid_pub_dates])
            decay = max(0.5, 1 - (min_age_hours / 72))
            confidence *= decay

        # Signal consistency boost
        if signal_count >= 3:
            confidence = min(0.95, confidence * 1.2)
        elif signal_count == 1:
            confidence *= 0.8

        score = severity_map.get(severity, 0.3) * confidence

        return {
            "litigation_risk_score": round(score, 4),
            "articles_analyzed": len(relevant_articles),
            "risk": round(score, 4),
            "evidence": response.get("summary", ""),
            "analysis_status": "success_risk_found",
            "casemine_total": casemine_data.get("total_hits", 0) if casemine_data else 0,
            "casemine_high_risk_courts": casemine_data.get("high_risk_courts", []) if casemine_data else [],
        }

    # ─────────────────────────────────────────────────────────────────────────
    # EVENT HANDLER
    # ─────────────────────────────────────────────────────────────────────────

    def _temporal_adjustment(self, customer_id: str, current_risk: float) -> tuple[float, float]:
        history = self._risk_history.get(customer_id, [])
        
        if not history:
            return current_risk, 0.0

        now = datetime.utcnow()

        weighted_sum = 0.0
        total_weight = 0.0

        for entry in history:
            age_hours = (now - entry["timestamp"]).total_seconds() / 3600
            
            # decay (older = less weight)
            weight = max(0.2, 1 - (age_hours / 72))
            
            # Use base_risk to prevent compounding adjustments
            raw_risk = entry.get("base_risk", entry.get("risk", 0.0))
            weighted_sum += raw_risk * weight
            total_weight += weight

        historical_avg = weighted_sum / total_weight if total_weight else 0.0

        # trend boost / suppression
        if current_risk > historical_avg:
            adjusted = current_risk * 1.1   # spike boost
        else:
            adjusted = current_risk * 0.9   # decay

        recent = history[-3:]

        # repeated signal boost
        if len(recent) >= 3 and all(h.get("base_risk", h.get("risk", 0.0)) > 0.5 for h in recent):
            adjusted = min(1.0, adjusted * 1.2)

        # sudden spike detection
        if len(recent) >= 1:
            prev = recent[-1].get("base_risk", recent[-1].get("risk", 0.0))
            if current_risk - prev > 0.4:
                adjusted = min(1.0, adjusted * 1.25)

        return min(1.0, max(0.0, round(adjusted, 4))), historical_avg

    def _merge_signals(self, nclt_data: Dict, industry_data: Dict) -> Dict:
        """Fuse NCLT and Industry signals."""
        nclt_risk = nclt_data["nclt_risk"]
        industry_risk = industry_data["risk"]
        analysis_status = industry_data.get("analysis_status", "unknown")
        
        evidence = ""
        confidence = 0.0
        
        if nclt_data["nclt_case_count"] > 0:
            # NCLT is authoritative
            final_risk = 0.75 * nclt_risk + 0.25 * industry_risk
            evidence = f"[NCLT Focus] {nclt_data['nclt_evidence']}"
            if industry_risk > 0:
                evidence += f" | {industry_data['evidence']}"
            confidence = 0.85
        else:
            # High weight on targeted company news since it's directly about them
            final_risk = industry_risk
            if analysis_status == "failed":
                evidence = "[Analysis Failed] Could not complete LLM risk analysis."
                confidence = 0.1
            elif analysis_status == "skipped_no_articles" or analysis_status == "skipped_filtered":
                evidence = "[No News] No relevant articles found."
                confidence = 0.9
            elif analysis_status == "success_no_risk":
                evidence = "[News Clear] " + industry_data.get("evidence", "No consistent risk signals.")
                confidence = 0.8
            else:
                evidence = f"[Web Focus] {industry_data['evidence']}"
                confidence = 0.7
            
        final_risk = round(min(1.0, max(0.0, final_risk)), 4)
        severity = "high" if final_risk >= 0.5 else "medium" if final_risk >= 0.25 else "low"
        
        return {
            "final_risk": final_risk,
            "severity": severity,
            "evidence": evidence,
            "confidence": confidence,
            "analysis_status": analysis_status
        }

    def handle_event(self, event: Event) -> None:
        """Submit scraping work to the thread pool and return immediately.

        A per-customer in-flight guard (_in_flight) prevents duplicate concurrent
        submissions when multiple events for the same customer arrive before a
        prior scrape has finished.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        with self._in_flight_lock:
            if customer_id in self._in_flight:
                logger.debug(
                    "[ExternalScrapingAgent] Skipping %s — scrape already in flight",
                    customer_id,
                )
                return
            self._in_flight[customer_id] = True

        company_name = data.get("company_name")
        self._executor.submit(
            self._scrape_and_publish,
            customer_id,
            company_name,
            event.correlation_id,
        )

    def _scrape_and_publish(
        self,
        customer_id: str,
        company_name: Optional[str],
        correlation_id: Optional[str],
    ) -> None:
        """Full NCLT/news/CaseMine/LLM pipeline executed on an executor thread.

        Previously the body of handle_event().  Clears the in-flight guard when
        done (whether it succeeded or raised an exception).
        """
        try:
            # Resolve company name and industry
            industry = "unknown"
            aliases = []
            try:
                customer = QueryClient.query("get_customer", {"customer_id": customer_id})
                if customer:
                    if customer.get("name"):
                        company_name = customer.get("name")
                    industry = customer.get("industry", customer.get("sector", "unknown"))
                    if customer.get("aliases"):
                        aliases.extend(customer.get("aliases", []))
                    if customer.get("ticker"):
                        aliases.append(customer.get("ticker"))
                    if customer.get("parent_company"):
                        aliases.append(customer.get("parent_company"))
            except Exception:
                pass

            if not company_name or bool(re.match(r'^cust_\d+$', company_name)):
                logger.debug(f"[ExternalScrapingAgent] Skipping {customer_id}: no real company name")
                return

            company_name = company_name.strip()[:100]

            # 1. Fetch NCLT raw cases (cached 12h)
            nclt_cache_key = company_name.lower()
            nclt_entry = self._nclt_cache.get(nclt_cache_key)
            if nclt_entry and (datetime.utcnow() - nclt_entry["timestamp"]).total_seconds() < 43200:
                nclt_cases = nclt_entry["data"]
            else:
                logger.info(f"[ExternalScrapingAgent] Fetching NCLT for {company_name}...")
                nclt_cases = self._fetch_nclt_all_benches(company_name)
                self._nclt_cache[nclt_cache_key] = {"data": nclt_cases, "timestamp": datetime.utcnow()}

            # 2. Fetch news (cached 6h)
            news_cache_key = company_name.lower()
            news_entry = self._news_cache.get(news_cache_key)
            if news_entry and (datetime.utcnow() - news_entry["timestamp"]).total_seconds() < 43200:
                articles = news_entry["data"]
            else:
                logger.info(f"[ExternalScrapingAgent] Fetching news for {company_name}...")
                articles = self._fetch_company_news_risk(company_name, aliases=aliases)
                self._news_cache[news_cache_key] = {"data": articles, "timestamp": datetime.utcnow()}

            # 3. Fetch CaseMine (cached 12h)
            cm_cache_key = f"cm:{company_name.lower()}"
            cm_entry = self._nclt_cache.get(cm_cache_key)
            if cm_entry and (datetime.utcnow() - cm_entry["timestamp"]).total_seconds() < 43200:
                casemine_data = cm_entry["data"]
            else:
                logger.info(f"[ExternalScrapingAgent] Fetching CaseMine for {company_name}...")
                casemine_data = self._fetch_casemine(company_name)
                self._nclt_cache[cm_cache_key] = {"data": casemine_data, "timestamp": datetime.utcnow()}

            # 4. Fetch macro/regulatory context (cached 3h)
            macro_cache_key = f"macro:{company_name.lower()}:{industry.lower()}"
            macro_entry = self._news_cache.get(macro_cache_key)
            if macro_entry and (datetime.utcnow() - macro_entry["timestamp"]).total_seconds() < 21600:
                macro_articles = macro_entry["data"]
            else:
                logger.info(f"[ExternalScrapingAgent] Fetching macro/regulatory context for {company_name}...")
                macro_articles = self._fetch_macro_context(company_name, industry)
                self._news_cache[macro_cache_key] = {"data": macro_articles, "timestamp": datetime.utcnow()}

            # 5. Unified LLM analysis
            analysis_cache_key = f"analysis:{company_name.lower()}"
            analysis_entry = self._news_cache.get(analysis_cache_key)
            if analysis_entry and (datetime.utcnow() - analysis_entry["timestamp"]).total_seconds() < 43200:
                fusion = analysis_entry["data"]
            else:
                fusion = self._unified_llm_analysis(
                    company_name=company_name,
                    industry=industry,
                    nclt_cases=nclt_cases,
                    articles=articles,
                    casemine_data=casemine_data,
                    macro_articles=macro_articles,
                    aliases=aliases,
                )
                self._news_cache[analysis_cache_key] = {"data": fusion, "timestamp": datetime.utcnow()}

            base_risk = fusion["final_risk"]

            # Temporal adjustment
            final_risk, historical_avg = self._temporal_adjustment(customer_id, base_risk)

            # Store history
            history = self._risk_history.setdefault(customer_id, [])
            history.append({
                "timestamp": datetime.utcnow(),
                "base_risk": base_risk,
                "risk": final_risk,
                "source": "unified_llm",
            })
            if len(history) > self._MAX_HISTORY:
                history.pop(0)

            # Deduplication
            last_risk = self._last_published_risk.get(customer_id)
            if last_risk is not None and last_risk == final_risk:
                return

            # Build payload
            nclt_count = fusion.get("nclt_case_count", 0)
            payload = {
                "customer_id": customer_id,
                "company_name": company_name,

                "litigation_flag": nclt_count > 0,
                "litigation_risk": final_risk,
                "severity": fusion["severity"],
                "evidence": fusion["evidence"],
                "legal_status": fusion.get("legal_status", "unknown"),
                "risk_types": fusion.get("risk_types", []),

                "nclt_case_count": nclt_count,
                "nclt_cases": fusion.get("nclt_cases", []),

                "casemine_total": fusion.get("casemine_total", 0),
                "casemine_high_risk_courts": fusion.get("casemine_high_risk_courts", []),

                "source": "nclt+casemine+news+llm_unified",
                "confidence": fusion["confidence"],
                "analysis_status": fusion["analysis_status"],
                "temporal_context": {
                    "history_length": len(self._risk_history.get(customer_id, [])),
                    "recent_avg": round(historical_avg, 4) if history else 0.0,
                },
                "generated_at": datetime.utcnow().isoformat()
            }

            self.publish_event(
                topic=self.TOPIC_OUTPUT,
                event_type="external.litigation.updated",
                entity_id=customer_id,
                payload=payload,
                correlation_id=correlation_id,
            )

            if len(self._last_published_risk) >= self._MAX_RISK_TRACK:
                self._last_published_risk.popitem(last=False)
            self._last_published_risk[customer_id] = final_risk

            logger.info(
                f"[ExternalScrapingAgent] company={company_name} "
                f"final_risk={final_risk:.4f} nclt_cases={nclt_count} "
                f"severity={fusion['severity']} status={fusion['analysis_status']}"
            )

        except Exception as exc:
            logger.error(
                "[ExternalScrapingAgent] _scrape_and_publish failed for %s: %s",
                customer_id, exc, exc_info=True,
            )
        finally:
            # Always clear the in-flight guard so subsequent events for this
            # customer can be queued once the current scrape has completed.
            with self._in_flight_lock:
                self._in_flight.pop(customer_id, None)
