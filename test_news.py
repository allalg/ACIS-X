import requests, urllib.parse, xml.etree.ElementTree as ET
q = urllib.parse.quote_plus('"Dream11" AND (ban OR tax OR illegal OR restriction OR penalty)')
url = f'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'
r = requests.get(url)
root = ET.fromstring(r.content)
print([i.find('title').text for i in root.findall('.//item')][:5])
