import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment mode
# ---------------------------------------------------------------------------
# ACIS_ENV controls security strictness:
#   - "development" (default): allows weak keys with a warning
#   - "production": rejects startup if API key is missing or weak (< 32 chars)

ACIS_ENV = os.getenv('ACIS_ENV', 'development').lower()


@dataclass(frozen=True)
class Settings:
    api_key: str
    db_path: str
    allowed_origins: list[str]
    kafka_bootstrap_servers: str
    env: str


def _validate_api_key(key: str | None) -> str:
    """Validate the API key based on the current environment mode."""
    if not key:
        if ACIS_ENV == 'production':
            raise ValueError(
                'ACIS_API_KEY is required in production mode. '
                'Set ACIS_ENV=development to use default keys for local dev.'
            )
        logger.warning(
            '⚠️  ACIS_API_KEY is not set — falling back to default key. '
            'Do NOT use this in production.'
        )
        return 'change_me'

    if key == 'change_me':
        if ACIS_ENV == 'production':
            raise ValueError(
                'ACIS_API_KEY cannot be "change_me" in production mode. '
                'Set a strong key (32+ characters) or use ACIS_ENV=development.'
            )
        logger.warning(
            '⚠️  Using default API key "change_me". '
            'Set a strong ACIS_API_KEY before deploying.'
        )

    if ACIS_ENV == 'production' and len(key) < 32:
        raise ValueError(
            f'ACIS_API_KEY is too short ({len(key)} chars). '
            'Production keys must be at least 32 characters.'
        )

    return key


def load_settings() -> Settings:
    api_key = _validate_api_key(os.getenv('ACIS_API_KEY'))
    db_path = os.getenv('ACIS_DB_PATH', '../acis.db')
    allowed_origins = [
        origin.strip()
        for origin in os.getenv(
            'ACIS_ALLOWED_ORIGINS',
            'http://localhost:5173',
        ).split(',')
        if origin.strip()
    ]
    kafka_bootstrap_servers = os.getenv('ACIS_KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    return Settings(
        api_key=api_key,
        db_path=db_path,
        allowed_origins=allowed_origins,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        env=ACIS_ENV,
    )
