"""
Test script for BFF endpoints and database integration.
"""
import os
import sys
from pathlib import Path

# Add project root and acis-api-bff to sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "acis-api-bff"))

from fastapi.testclient import TestClient
from app.main import app

def test_endpoints():
    client = TestClient(app)
    
    print("Testing /api/v1/health...")
    resp = client.get("/api/v1/health")
    print(f"  Status: {resp.status_code}, Response: {resp.json()}")
    assert resp.status_code == 200

    print("Testing /api/v1/dashboard/summary...")
    resp = client.get("/api/v1/dashboard/summary", headers={"X-API-Key": "change_me"})
    print(f"  Status: {resp.status_code}, Keys: {list(resp.json().keys()) if resp.status_code == 200 else resp.text}")
    assert resp.status_code == 200

    print("Testing /api/v1/customers...")
    resp = client.get("/api/v1/customers", headers={"X-API-Key": "change_me"})
    print(f"  Status: {resp.status_code}, Response: {resp.json() if resp.status_code == 200 else resp.text}")
    assert resp.status_code == 200

    print("Testing /api/v1/invoices...")
    resp = client.get("/api/v1/invoices?limit=5", headers={"X-API-Key": "change_me"})
    print(f"  Status: {resp.status_code}, Response: {resp.json() if resp.status_code == 200 else resp.text}")
    assert resp.status_code == 200

    print("Testing /api/v1/agents/status...")
    resp = client.get("/api/v1/agents/status", headers={"X-API-Key": "change_me"})
    print(f"  Status: {resp.status_code}, Response: {resp.json()}")
    assert resp.status_code == 200

    print("Testing /api/v1/system/pipeline...")
    resp = client.get("/api/v1/system/pipeline", headers={"X-API-Key": "change_me"})
    print(f"  Status: {resp.status_code}, Response: {resp.json()}")
    assert resp.status_code == 200

    print("\nAll tested BFF endpoints responded successfully with 200 OK!")

if __name__ == "__main__":
    test_endpoints()
