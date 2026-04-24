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
        # Ignore our own output to prevent self-triggering loops
        if event.event_type == "external.litigation.updated":
            return
            
        if event.event_type.startswith("customer.") or event.event_type.startswith("external."):
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



    def _fetch_company_news_risk(self, company_name: str, aliases: List[str] = None) -> List[Dict]:
        """Fetch targeted news for the specific company and aliases."""
        query_parts = [f'"{company_name}"']
        if aliases:
            for alias in aliases:
                if alias and len(alias) >= 3:
                    query_parts.append(f'"{alias}"')
        search_query = " OR ".join(query_parts)
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
                    description = (item.findtext("description") or "").strip()
                    
                    if not title: continue
                    if pub_date_str:
                        try:
                            if parsedate_to_datetime(pub_date_str) < cutoff_date: continue
                        except: pass
                    articles.append({"title": title, "description": description, "pubDate": pub_date_str, "source": "google_news"})
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
                    description = (item.findtext("description") or "").strip()
                    
                    if not title: continue
                    if pub_date_str:
                        try:
                            if parsedate_to_datetime(pub_date_str) < cutoff_date: continue
                        except: pass
                    articles.append({"title": title, "description": description, "pubDate": pub_date_str, "source": "bing_news"})
        except Exception as e:
            logger.debug(f"[ExternalScrapingAgent] Bing company news failed: {e}")

        # Deduplicate
        merged = {}
        for a in articles:
            key = a["title"].lower()
            if key not in merged:
                merged[key] = a
                
        return list(merged.values())[:20]

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

    def _analyze_company_news(self, articles: List[Dict], company_name: str, industry: str = "unknown", aliases: List[str] = None) -> Dict:
        if not articles:
            return {
                "litigation_risk_score": 0.0,
                "articles_analyzed": 0,
                "risk": 0.0,
                "evidence": "No relevant news found.",
                "analysis_status": "skipped_no_articles"
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
                "analysis_status": "skipped_filtered"
            }

        context = self._build_multi_article_context(relevant_articles)

        prompt = f"""You are an advanced financial risk intelligence system.

You are given MULTIPLE news articles about a company.

Your job is to:
- Analyze all articles together
- Identify consistent risk signals
- Ignore isolated or weak signals
- Avoid false positives

---

COMPANY:
{company_name}

INDUSTRY:
{industry}

ARTICLES:
{context}

---

TASK:

1. Determine if there is a CONSISTENT and MATERIAL risk signal across the articles.

2. Consider:
   - Are multiple sources reporting the same issue?
   - Is the issue serious (fraud, legal, regulatory, insolvency)?
   - Is the company clearly the subject?

3. Ignore:
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
  "summary": "2–3 line explanation combining all evidence",
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
                "analysis_status": "failed"
            }

        if not response.get("risk_detected"):
            return {
                "litigation_risk_score": 0.0,
                "articles_analyzed": len(relevant_articles),
                "risk": 0.0,
                "evidence": "No consistent risk signals.",
                "analysis_status": "success_no_risk"
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
            "analysis_status": "success_risk_found"
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
        """Handle customer events and generate litigation risk."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        # Resolve company name and industry
        company_name = data.get("company_name")
        industry = "unknown"
        aliases = []
        if self._query_agent:
            try:
                customer = self._query_agent.get_customer(customer_id)
                if customer:
                    if customer.get("name"):
                        company_name = customer.get("name")
                    # Try harder to extract real industry/sector
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
            articles = self._fetch_company_news_risk(company_name, aliases=aliases)
            industry_data = self._analyze_company_news(articles, company_name=company_name, industry=industry, aliases=aliases)
            self._news_cache[news_cache_key] = {"data": industry_data, "timestamp": datetime.utcnow()}

        # 3. Merge Signals
        fusion = self._merge_signals(nclt_data, industry_data)
        base_risk = fusion["final_risk"]
        
        # 4. Temporal Adjustment
        final_risk, historical_avg = self._temporal_adjustment(customer_id, base_risk)
        
        # Store History
        history = self._risk_history.setdefault(customer_id, [])
        history.append({
            "timestamp": datetime.utcnow(),
            "base_risk": base_risk,
            "risk": final_risk,
            "source": "nclt+news",
        })
        if len(history) > self._MAX_HISTORY:
            history.pop(0)

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
            "confidence": fusion["confidence"],
            "analysis_status": fusion["analysis_status"],
            "temporal_context": {
                "history_length": len(self._risk_history.get(customer_id, [])),
                "recent_avg": round(historical_avg, 4) if history else 0.0,
            },
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
            f"industry={'targeted_web_search'} ind_risk={industry_data['risk']:.4f}"
        )
