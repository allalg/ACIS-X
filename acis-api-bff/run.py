import os

import uvicorn


if __name__ == '__main__':
    host = os.getenv('ACIS_BFF_HOST', '0.0.0.0')
    port = int(os.getenv('ACIS_BFF_PORT', '8000'))
    uvicorn.run('app.main:app', host=host, port=port, reload=True)
