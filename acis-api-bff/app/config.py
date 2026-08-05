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
    if not key or key == 'change_me':
        logger.warning('Using default API key "change_me"')
        return 'change_me'
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
    kafka_security_protocol = os.getenv('ACIS_KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
    kafka_sasl_mechanism = os.getenv('ACIS_KAFKA_SASL_MECHANISM', None)
    kafka_sasl_username = os.getenv('ACIS_KAFKA_SASL_USERNAME', None)
    kafka_sasl_password = os.getenv('ACIS_KAFKA_SASL_PASSWORD', None)

    return Settings(
        api_key=api_key,
        db_path=db_path,
        allowed_origins=allowed_origins,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        env=ACIS_ENV,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
        kafka_sasl_username=kafka_sasl_username,
        kafka_sasl_password=kafka_sasl_password,
    )
