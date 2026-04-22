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
import re
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
    12: "Amaravati", 13: "Cuttack", 14: "Kochi", 15: "Indore"
}

NCLT_CASE_TYPE_SCORES = {
    "IBA": 1.0, "CIRP": 1.0, "LQD": 1.0,
    "PIB": 0.90, "CP": 0.85, "CC": 0.75,
    "CA": 0.60, "IA": 0.30, "MA": 0.20,
}

INDUSTRY_KEYWORDS = {
    "real_estate": ["builder", "realty", "construction", "infrastructure", "housing", "real estate"],
    "banking_finance": ["bank", "nbfc", "finance", "capital", "credit", "leasing", "financial"],
    "manufacturing": ["steel", "cement", "textile", "chemical", "pharma", "auto", "manufacturing"],
    "telecom": ["telecom", "wireless", "broadband", "spectrum", "communications"],
    "energy": ["power", "energy", "coal", "oil", "gas", "solar", "renewable"],
    "retail_consumer": ["retail", "fmcg", "consumer", "grocery", "apparel"],
    "it_services": ["software", "technology", "it", "digital", "consulting", "tech"]
}

NEWS_RISK_KEYWORDS = {
    "fraud": 1.0, "scam": 1.0, "bankruptcy": 1.0, "insolvency": 1.0,
    "investigation": 0.8, "lawsuit": 0.7, "sued": 0.7, "penalty": 0.6,
    "default": 0.6, "violation": 0.5, "probe": 0.5, "indictment": 0.9,
    "embezzlement": 1.0, "misconduct": 0.7, "scandal": 0.8, "settlement": 0.5,
    "nclt": 0.9, "ibc": 0.9, "npa": 0.8, "sebi": 0.7,
    "ban": 0.9, "banned": 0.9, "illegal": 0.9, "restriction": 0.7, "tax evasion": 0.9, "money laundering": 1.0
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

    def __init__(self, kafka_client: Any, query_agent: Optional[Any] = None):
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
        self._query_agent = query_agent
        
        # Caches
        self._nclt_cache = {}      # TTL: 12h
        self._news_cache = {}      # TTL: 6h
        
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

        logger.info("[ExternalScrapingAgent] Initialized with NCLT + Industry News fusion")

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

    def process_event(self, event: Event) -> None:
        if event.event_type in ("customer.metrics.updated", "customer.profile.updated"):
            self.handle_event(event)

    # ─────────────────────────────────────────────────────────────────────────
    # NCLT LOGIC
    # ─────────────────────────────────────────────────────────────────────────

    def _fetch_nclt_bench(self, company_name: str, bench_id: int) -> List[Dict]:
        """Query a single NCLT bench for cases."""
        try:
            # Need fresh cookie sometimes, but usually session handles it. We'll rely on session.
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
                "party_type_party": "R",  # Respondent typically captures filed against
                "party_name_party": company_name,
                "i_case_year_party": "0",
                "status_party": "P", # Pending cases only
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
                
            data = response.json()
            if not isinstance(data, list):
                # Sometimes it returns a string or object on error
                return []
                
            cases = []
            cutoff_date = datetime.now() - timedelta(days=5 * 365)
            
            for item in data:
                # NCLT response mapping
                case_no = item.get("case_no", "")
                case_type = item.get("case_type", "")
                filing_date_str = item.get("filing_date", "")
                status = item.get("status", "Pending")
                
                # We requested Pending, but double check
                if status.upper() != "PENDING":
                    continue
                    
                # Date check
                if filing_date_str:
                    try:
                        # Format usually dd-mm-yyyy or similar
                        # A simple heuristic date parser
                        parts = re.split(r'[-/]', filing_date_str)
                        if len(parts) == 3:
                            if len(parts[2]) == 4: # dd-mm-yyyy
                                f_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                            else: # yyyy-mm-dd
                                f_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                            if f_date < cutoff_date:
                                continue
                    except Exception:
                        pass # If we can't parse, include it safely
                
                cases.append({
                    "case_no": case_no,
                    "case_type": case_type,
                    "bench": NCLT_BENCH_IDS.get(bench_id, "Unknown"),
                    "filing_date": filing_date_str,
                    "status": status
                })
                
            return cases
            
        except Exception as e:
            logger.debug(f"[ExternalScrapingAgent] NCLT fetch failed for bench {bench_id}: {e}")
            return []

    def _fetch_nclt_all_benches(self, company_name: str) -> List[Dict]:
        """Query all NCLT benches and deduplicate results."""
        all_cases = []
        seen_case_nos = set()
        
        # Prime the session cookie first
        try:
            self._nclt_session.get(NCLT_SESSION_URL, timeout=5)
        except Exception as e:
            logger.warning(f"[ExternalScrapingAgent] Failed to prime NCLT session: {e}")
            return []

        # Fetch from all benches concurrently to speed up the process (max 5 workers to be polite)
        def fetch_bench(bench_id):
            return self._fetch_nclt_bench(company_name, bench_id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_bench = {executor.submit(fetch_bench, bid): bid for bid in NCLT_BENCH_IDS.keys()}
            for future in concurrent.futures.as_completed(future_to_bench):
                try:
                    cases = future.result()
                    for c in cases:
                        cno = c.get("case_no")
                        if cno and cno not in seen_case_nos:
                            seen_case_nos.add(cno)
                            all_cases.append(c)
                except Exception as e:
                    logger.debug(f"[ExternalScrapingAgent] Concurrent fetch failed for a bench: {e}")
            
        return all_cases

    def _score_nclt_cases(self, cases: List[Dict]) -> Dict:
        """Score NCLT cases and return risk info."""
        if not cases:
            return {
                "nclt_risk": 0.0,
                "nclt_case_count": 0,
                "nclt_severity": "low",
                "nclt_cases": [],
                "nclt_case_types": [],
                "nclt_evidence": "No pending NCLT cases found."
            }
            
        max_risk = 0.0
        case_types = set()
        
        for case in cases:
            ctype_raw = case.get("case_type", "").strip()
            # Extract abbreviation (e.g. "IBA/123/2023" -> "IBA")
            ctype = ctype_raw.split("/")[0] if "/" in ctype_raw else ctype_raw
            
            score = NCLT_CASE_TYPE_SCORES.get(ctype, 0.40) # default 0.4 for unknown types
            max_risk = max(max_risk, score)
            case_types.add(ctype)
            
        return {
            "nclt_risk": round(min(1.0, max_risk), 4),
            "nclt_case_count": len(cases),
            "nclt_severity": "high" if max_risk >= 0.75 else "medium" if max_risk >= 0.5 else "low",
            "nclt_cases": [c.get("case_no") for c in cases],
            "nclt_case_types": list(case_types),
            "nclt_evidence": f"Found {len(cases)} active NCLT cases, highest risk from {', '.join(list(case_types)[:3])}."
        }

    # ─────────────────────────────────────────────────────────────────────────
    # INDUSTRY NEWS LOGIC
    # ─────────────────────────────────────────────────────────────────────────

    def _detect_industry(self, payload: Dict, company_name: str) -> str:
        """Detect industry from payload or company name."""
        industry_raw = payload.get("industry", "").lower()
        if industry_raw:
            # Map raw industry to tag
            for tag, keywords in INDUSTRY_KEYWORDS.items():
                if industry_raw in tag or any(k in industry_raw for k in keywords):
                    return tag
            return "general_corporate"
            
        # Fallback to company name
        cname_lower = company_name.lower()
        for tag, keywords in INDUSTRY_KEYWORDS.items():
            if any(re.search(rf'\b{re.escape(k)}\b', cname_lower) for k in keywords):
                return tag
                
        return "general_corporate"

    def _fetch_company_news_risk(self, company_name: str) -> List[Dict]:
        """Fetch targeted news for the specific company with negative keywords."""
        search_query = f'"{company_name}" AND (ban OR banned OR illegal OR tax OR penalty OR lawsuit OR scam OR fraud OR restriction OR money laundering)'
        encoded_query = quote_plus(search_query)
        articles = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=3)
        
        # Google News
        try:
            url = f"{self.GOOGLE_NEWS_RSS_URL}?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"
            response = self._session.get(url, timeout=20)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                for item in root.findall(".//item")[:15]:
                    title = (item.findtext("title") or "").strip()
                    pub_date_str = item.findtext("pubDate")
                    
                    if not title: continue
                    if pub_date_str:
                        try:
                            if parsedate_to_datetime(pub_date_str) < cutoff_date: continue
                        except: pass
                        
                    articles.append({"title": title, "source": "google_news"})
        except Exception as e:
            logger.debug(f"[ExternalScrapingAgent] Google company news failed: {e}")

        # Bing News
        try:
            url = f"{self.BING_NEWS_RSS_URL}?q={encoded_query}&format=rss"
            response = self._session.get(url, timeout=20)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                for item in root.findall(".//item")[:15]:
                    title = (item.findtext("title") or "").strip()
                    pub_date_str = item.findtext("pubDate")
                    
                    if not title: continue
                    if pub_date_str:
                        try:
                            if parsedate_to_datetime(pub_date_str) < cutoff_date: continue
                        except: pass
                        
                    articles.append({"title": title, "source": "bing_news"})
        except Exception as e:
            logger.debug(f"[ExternalScrapingAgent] Bing company news failed: {e}")

        # Deduplicate
        merged = {}
        for a in articles:
            key = a["title"].lower()
            if key not in merged:
                merged[key] = a
                
        return list(merged.values())[:20]

    def _analyze_company_news(self, articles: List[Dict]) -> Dict:
        """Score company risk based on targeted news and extract evidence."""
        if not articles:
            return {"risk": 0.0, "evidence": "No relevant targeted news found."}
            
        total_weight = 0.0
        match_count = 0
        top_evidence = []
        
        for article in articles:
            title_lower = article.get("title", "").lower()
            article_weight = 0.0
            matched_keywords = []
            
            for keyword, weight in NEWS_RISK_KEYWORDS.items():
                if keyword in title_lower:
                    article_weight = max(article_weight, weight)
                    matched_keywords.append(keyword)
                    
            if article_weight > 0:
                total_weight += article_weight
                match_count += 1
                if len(top_evidence) < 2:
                    top_evidence.append(f"'{article.get('title')}' (keywords: {','.join(matched_keywords)})")
                
        risk_score = min(1.0, (total_weight / max(1, len(articles))) * 2)
        
        evidence_str = "No major targeted risks detected."
        if top_evidence:
            evidence_str = "Targeted risks detected: " + " | ".join(top_evidence)
            
        return {"risk": round(risk_score, 4), "evidence": evidence_str}

    # ─────────────────────────────────────────────────────────────────────────
    # EVENT HANDLER
    # ─────────────────────────────────────────────────────────────────────────

    def _merge_signals(self, nclt_data: Dict, industry_data: Dict) -> Dict:
        """Fuse NCLT and Industry signals."""
        nclt_risk = nclt_data["nclt_risk"]
        industry_risk = industry_data["risk"]
        
        evidence = ""
        
        if nclt_data["nclt_case_count"] > 0:
            # NCLT is authoritative
            final_risk = 0.75 * nclt_risk + 0.25 * industry_risk
            evidence = f"[NCLT Focus] {nclt_data['nclt_evidence']}"
            if industry_risk > 0:
                evidence += f" | {industry_data['evidence']}"
        else:
            # High weight on targeted company news since it's directly about them
            final_risk = industry_risk
            evidence = f"[Web Focus] {industry_data['evidence']} (NCLT clear)"
            
        final_risk = round(min(1.0, max(0.0, final_risk)), 4)
        severity = "high" if final_risk >= 0.5 else "medium" if final_risk >= 0.25 else "low"
        
        return {
            "final_risk": final_risk,
            "severity": severity,
            "evidence": evidence
        }

    def handle_event(self, event: Event) -> None:
        """Handle customer events and generate litigation risk."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        # Resolve company name
        company_name = data.get("company_name")
        if not company_name and self._query_agent:
            try:
                customer = self._query_agent.get_customer(customer_id)
                if customer and customer.get("name"):
                    company_name = customer.get("name")
            except Exception:
                pass

        if not company_name or bool(re.match(r'^cust_\d+$', company_name)):
            logger.debug(f"[ExternalScrapingAgent] Skipping {customer_id}: no real company name")
            return
            
        company_name = company_name.strip()[:100]

        # 1. Fetch NCLT (Cached 12h)
        nclt_cache_key = company_name.lower()
        nclt_entry = self._nclt_cache.get(nclt_cache_key)
        
        if nclt_entry and (datetime.utcnow() - nclt_entry["timestamp"]).total_seconds() < 43200:
            nclt_data = nclt_entry["data"]
        else:
            logger.info(f"[ExternalScrapingAgent] Fetching NCLT for {company_name}...")
            cases = self._fetch_nclt_all_benches(company_name)
            nclt_data = self._score_nclt_cases(cases)
            self._nclt_cache[nclt_cache_key] = {"data": nclt_data, "timestamp": datetime.utcnow()}

        # 2. Fetch Targeted Company News (Cached 6h)
        news_cache_key = company_name.lower()
        news_cache_entry = self._news_cache.get(news_cache_key)
        
        if news_cache_entry and (datetime.utcnow() - news_cache_entry["timestamp"]).total_seconds() < 21600:
            industry_data = news_cache_entry["data"]
        else:
            logger.info(f"[ExternalScrapingAgent] Fetching targeted news for {company_name}...")
            articles = self._fetch_company_news_risk(company_name)
            industry_data = self._analyze_company_news(articles)
            self._news_cache[news_cache_key] = {"data": industry_data, "timestamp": datetime.utcnow()}

        # 3. Merge Signals
        fusion = self._merge_signals(nclt_data, industry_data)
        final_risk = fusion["final_risk"]

        # Deduplication check
        last_risk = self._last_published_risk.get(customer_id)
        if last_risk is not None and last_risk == final_risk:
            return

        # Build payload
        payload = {
            "customer_id": customer_id,
            "company_name": company_name,

            "litigation_flag": nclt_data["nclt_case_count"] > 0,
            "litigation_risk": final_risk,
            "severity": fusion["severity"],
            "evidence": fusion["evidence"],
            
            "nclt_case_count": nclt_data["nclt_case_count"],
            "nclt_cases": nclt_data["nclt_cases"],
            "nclt_severity": nclt_data["nclt_severity"],
            "nclt_risk": nclt_data["nclt_risk"],
            
            "industry_tag": "targeted_web_search",
            "industry_risk": industry_data["risk"],

            "source": "nclt+industry_news",
            "confidence": 0.85 if nclt_data["nclt_case_count"] > 0 else 0.6,
            "generated_at": datetime.utcnow().isoformat()
        }

        # Publish
        self.publish_event(
            topic=self.TOPIC_OUTPUT,
            event_type="external.litigation.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        if len(self._last_published_risk) >= self._MAX_RISK_TRACK:
            self._last_published_risk.popitem(last=False)
        self._last_published_risk[customer_id] = final_risk

        logger.info(
            f"[ExternalScrapingAgent] company={company_name} "
            f"final_risk={final_risk:.4f} nclt_cases={nclt_data['nclt_case_count']} "
            f"industry={industry_tag} ind_risk={industry_risk:.4f}"
        )
