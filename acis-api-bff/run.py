from datetime import timezone
import os
from pathlib import Path

from dotenv import load_dotenv
import uvicorn

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")
load_dotenv(_ROOT / ".env.cloud", override=True)

if __name__ == '__main__':
    host = os.getenv('ACIS_BFF_HOST', '0.0.0.0')
    port = int(os.getenv('ACIS_BFF_PORT', '8000'))
    uvicorn.run('app.main:app', host=host, port=port, reload=True)
