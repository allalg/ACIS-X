import logging

from fastapi import Depends, Header, HTTPException, Query, status

from .config import load_settings

logger = logging.getLogger(__name__)


def require_api_key(
    x_api_key: str | None = Header(default=None, alias='X-API-Key'),
    api_key_query: str | None = Query(default=None, alias='api_key'),
) -> str:
    """Validate the incoming API key against the configured key.

    Accepts the key from either the ``X-API-Key`` header or the ``api_key``
    query parameter.
    """
    settings = load_settings()
    provided = x_api_key or api_key_query

    if not provided:
        if settings.env == 'development':
            return settings.api_key
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Missing API key. Provide it via X-API-Key header or api_key query param.',
        )

    if provided != settings.api_key:
        logger.warning('Rejected request with invalid API key (first 4 chars: %s…)', provided[:4])
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Invalid API key.',
        )

    return provided


AuthDependency = Depends(require_api_key)
