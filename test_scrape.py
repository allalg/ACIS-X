import requests
from bs4 import BeautifulSoup
import re

def parse_numeric(text):
    if not text: return None
    text = str(text).strip().replace(",", "")
    multiplier = 1.0
    if "Cr" in text: multiplier = 1e7
    elif "Lakh" in text or "Lac" in text: multiplier = 1e5
    match = re.search(r"[-+]?\d*\.?\d+", text)
    if not match: return None
    try: return float(match.group()) * multiplier
    except ValueError: return None

def get_value_by_label(soup, label):
    label_lower = label.lower()
    for li in soup.select("li"):
        name_el = li.select_one(".name")
        value_el = li.select_one(".value, .number")
        if name_el and value_el and label_lower in name_el.get_text(strip=True).lower():
            return value_el.get_text(strip=True)
    for el in soup.find_all(["span", "div", "td"]):
        text = el.get_text(strip=True).lower()
        if label_lower in text and len(text) < 50:
            next_sib = el.find_next_sibling()
            if next_sib:
                val = next_sib.get_text(strip=True)
                if val and re.search(r"[\d.]", val): return val
            parent = el.parent
            if parent:
                children = parent.find_all(recursive=False)
                for i, child in enumerate(children):
                    if child == el and i + 1 < len(children):
                        val = children[i + 1].get_text(strip=True)
                        if val and re.search(r"[\d.]", val): return val
    return None

url = 'https://www.screener.in/company/TCS/'
res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(res.text, 'lxml')

metrics = {
    'market_cap': get_value_by_label(soup, 'Market Cap'),
    'pe': get_value_by_label(soup, 'Stock P/E'),
    'roe': get_value_by_label(soup, 'ROE'),
    'roce': get_value_by_label(soup, 'ROCE'),
    'debt': get_value_by_label(soup, 'Debt'),
    'sales_growth': get_value_by_label(soup, 'Sales growth'),
    'profit_growth': get_value_by_label(soup, 'Profit growth'),
    'opm': get_value_by_label(soup, 'OPM')
}
for k, v in metrics.items():
    print(f"{k}: {v} -> parsed: {parse_numeric(v)}")
