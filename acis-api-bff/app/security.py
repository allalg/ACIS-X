from fastapi import Depends, Header, HTTPException, Query, status

from .config import load_settings



def require_api_key(
    x_api_key: str | None = Header(default=None, alias='X-API-Key'),
    api_key_query: str | None = Query(default=None, alias='api_key'),
) -> str:
    settings = load_settings()
    provided = x_api_key or api_key_query
    if not provided or provided != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Access denied. Check API key configuration.',
        )
    return provided


AuthDependency = Depends(require_api_key)
