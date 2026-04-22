import logging
from agents.intelligence.external_scrapping_agent import ExternalScrapingAgent

logging.basicConfig(level=logging.INFO)

agent = ExternalScrapingAgent(None)

# Mock NCLT fetch to avoid timeout for this test
agent._fetch_nclt_all_benches = lambda c: [
    {"case_no": "CP/1/2023", "case_type": "CP", "bench": "Mumbai", "filing_date": "2023-01-01", "status": "Pending"},
    {"case_no": "IA/2/2023", "case_type": "IA", "bench": "Mumbai", "filing_date": "2023-01-02", "status": "Pending"}
]

company = "Reliance Industries Limited"
print(f"Testing NCLT fetch for: {company}")
nclt_cases = agent._fetch_nclt_all_benches(company)
print(f"Total deduplicated cases found: {len(nclt_cases)}")
scored = agent._score_nclt_cases(nclt_cases)
print("Scored output:")
print(scored)

print("\nTesting industry news fetch...")
industry = agent._detect_industry({}, company)
print(f"Detected industry: {industry}")
articles = agent._fetch_industry_news(industry)
print(f"Found {len(articles)} industry articles")
industry_risk = agent._analyze_industry_news(articles)
print(f"Industry risk score: {industry_risk}")

print("\nTesting signal fusion...")
fusion = agent._merge_signals(scored, industry_risk)
print("Fusion output:")
print(fusion)
