import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)

ACIS_ENV = os.getenv('ACIS_ENV', 'development').lower()


@dataclass(frozen=True)
class Settings:
    api_key: str
    db_path: str
    database_url: str | None
    log_path: str
    allowed_origins: list[str]
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str | None
    kafka_sasl_username: str | None
    kafka_sasl_password: str | None
    env: str


def _validate_api_key(key: str | None) -> str:
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
    from pathlib import Path
    from dotenv import load_dotenv
    _root = Path(__file__).resolve().parents[2]
    if (_root / ".env").exists():
        load_dotenv(_root / ".env")

    api_key = _validate_api_key(os.getenv('ACIS_API_KEY'))
    db_path = os.getenv('ACIS_DB_PATH')
    if not db_path:
        if (_root / "acis.db").exists():
            db_path = str(_root / "acis.db")
        else:
            db_path = '../acis.db'
    elif not os.path.isabs(db_path) and not db_path.startswith("file:"):
        if (_root / db_path).exists():
            db_path = str(_root / db_path)
        elif (Path.cwd() / db_path).exists():
            db_path = str(Path.cwd() / db_path)
    database_url = os.getenv('ACIS_DATABASE_URL') or os.getenv('DATABASE_URL')
    log_path = os.getenv('ACIS_LOG_PATH', '../acis.log')
    allowed_origins = [
        origin.strip()
        for origin in os.getenv(
            'ACIS_ALLOWED_ORIGINS',
            'http://localhost:5173,http://localhost:3001',
        ).split(',')
        if origin.strip()
    ]
    kafka_bootstrap_servers = os.getenv('ACIS_KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    return Settings(
        api_key=api_key,
        db_path=db_path,
        database_url=database_url,
        log_path=log_path,
        allowed_origins=allowed_origins,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_security_protocol=os.getenv('ACIS_KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
        kafka_sasl_mechanism=os.getenv('ACIS_KAFKA_SASL_MECHANISM'),
        kafka_sasl_username=os.getenv('ACIS_KAFKA_SASL_USERNAME'),
        kafka_sasl_password=os.getenv('ACIS_KAFKA_SASL_PASSWORD'),
        env=ACIS_ENV,
    )


def kafka_security_kwargs(settings: Settings) -> dict:
    """aiokafka security kwargs for Confluent Cloud."""
    if not settings.kafka_security_protocol or settings.kafka_security_protocol == 'PLAINTEXT':
        return {}
    return {
        'security_protocol': settings.kafka_security_protocol,
        'sasl_mechanism': settings.kafka_sasl_mechanism or 'PLAIN',
        'sasl_plain_username': settings.kafka_sasl_username,
        'sasl_plain_password': settings.kafka_sasl_password,
    }
