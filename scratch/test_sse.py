import urllib.request

url = "https://personal-modern-sending-yale.trycloudflare.com/api/v1/events/stream?api_key=change_me&bypass-tunnel-reminder=true"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

try:
    print(f"Connecting to {url}...")
    with urllib.request.urlopen(req, timeout=10) as resp:
        print(f"Status Code: {resp.status}")
        print("Headers:")
        for k, v in resp.headers.items():
            print(f"  {k}: {v}")
        print("Reading first 3 lines of stream...")
        for _ in range(3):
            line = resp.readline()
            print(f"Received line: {line.decode('utf-8').strip()}")
except Exception as e:
    print(f"Error: {e}")
