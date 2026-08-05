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
    query parameter. If configured with 'change_me' or empty, bypasses strict auth for browser dashboard clients.
    """
    settings = load_settings()
    if not settings.api_key or settings.api_key.lower() in ('change_me', 'none', 'false', ''):
        return 'allowed'

    provided = x_api_key or api_key_query

    if not provided or provided != settings.api_key:
        logger.warning('Rejected request with invalid API key (first 4 chars: %s…)', (provided or '')[:4])
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Invalid or missing API key.',
        )

    return provided


AuthDependency = Depends(require_api_key)
