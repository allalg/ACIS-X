"""
Centralized configuration for ACIS-X.

All settings are loaded from environment variables with sensible defaults.
Import and use the singleton ``settings`` instance directly::

    from config.settings import settings
    print(settings.kafka_bootstrap_servers)
"""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class ACISSettings(BaseSettings):
    """ACIS-X configuration loaded from environment variables.

    Variable names are prefixed with ``ACIS_`` and are case-insensitive.
    """

    model_config = {
        'env_prefix': 'ACIS_',
        'env_file': '.env',
        'env_file_encoding': 'utf-8',
        'extra': 'ignore',
    }

    # ── Kafka ──────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        default='localhost:9092',
        description='Comma-separated Kafka broker addresses',
    )
    kafka_backend: str = Field(
        default='confluent',
        description='Kafka client backend: "confluent" or "kafka-python"',
    )
    offset_reset: str = Field(
        default='latest',
        description='Consumer auto-offset reset: "latest" or "earliest"',
    )
    kafka_security_protocol: str = Field(
        default='PLAINTEXT',
        description='Kafka security protocol (PLAINTEXT, SASL_SSL, SASL_PLAINTEXT)',
    )
    kafka_sasl_mechanism: str | None = Field(
        default=None,
        description='SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)',
    )
    kafka_sasl_username: str | None = Field(
        default=None,
        description='SASL username / API key',
    )
    kafka_sasl_password: str | None = Field(
        default=None,
        description='SASL password / API secret',
    )

    # ── Agent ──────────────────────────────────────────────────────────────
    default_max_replicas: int = Field(default=3)
    decision_interval: int = Field(default=15, description='Seconds between placement decisions')
    generation_interval: float = Field(
        default=20.0,
        description='Seconds between scenario generation cycles',
    )

    # ── Self-Healing / Monitoring ──────────────────────────────────────────
    heartbeat_interval: int = Field(default=5, description='Heartbeat interval in seconds')
    health_score_threshold: float = Field(default=0.8)
    restart_cooldown: int = Field(default=120)
    scale_cooldown: int = Field(default=180)
    spawn_cooldown: int = Field(default=180)
    fallback_cooldown: int = Field(default=120)
    recovery_event_cooldown: int = Field(default=60)
    placement_request_cooldown: int = Field(default=180)
    degraded_restart_delay: int = Field(default=30)
    lag_scale_threshold: int = Field(default=50)
    critical_lag_threshold: int = Field(default=200)

    # ── Database ───────────────────────────────────────────────────────────
    db_path: str = Field(default='acis.db', description='Path to the SQLite database file')

    # ── Logging ────────────────────────────────────────────────────────────
    log_level: str = Field(default='INFO')

    # ── Registry ───────────────────────────────────────────────────────────
    registry_port: int = Field(default=5000)

    # ── LLM / External ────────────────────────────────────────────────────
    groq_api_key: str | None = Field(default=None, alias='GROQ_API_KEY')

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        v_upper = v.upper()
        if v_upper not in valid:
            raise ValueError(f'Invalid log level "{v}". Must be one of: {valid}')
        return v_upper

    @field_validator('kafka_backend')
    @classmethod
    def validate_kafka_backend(cls, v: str) -> str:
        valid = {'confluent', 'kafka-python'}
        if v not in valid:
            raise ValueError(f'Invalid Kafka backend "{v}". Must be one of: {valid}')
        return v


@lru_cache(maxsize=1)
def get_settings() -> ACISSettings:
    """Return the cached singleton settings instance."""
    return ACISSettings()


# ── Backwards-compatible module-level exports ──────────────────────────────
# These allow existing code like `from config.settings import LOG_LEVEL`
# to continue working without modification.

def _lazy_attr(name: str):
    """Resolve a module-level attribute from the settings singleton."""
    _s = get_settings()
    _map = {
        'ACIS_KAFKA_BOOTSTRAP_SERVERS': _s.kafka_bootstrap_servers,
        'ACIS_KAFKA_BACKEND': _s.kafka_backend,
        'DEFAULT_MAX_REPLICAS': _s.default_max_replicas,
        'DECISION_INTERVAL': _s.decision_interval,
        'HEARTBEAT_INTERVAL': _s.heartbeat_interval,
        'HEALTH_SCORE_THRESHOLD': _s.health_score_threshold,
        'RESTART_COOLDOWN': _s.restart_cooldown,
        'SCALE_COOLDOWN': _s.scale_cooldown,
        'SPAWN_COOLDOWN': _s.spawn_cooldown,
        'FALLBACK_COOLDOWN': _s.fallback_cooldown,
        'RECOVERY_EVENT_COOLDOWN': _s.recovery_event_cooldown,
        'PLACEMENT_REQUEST_COOLDOWN': _s.placement_request_cooldown,
        'DEGRADED_RESTART_DELAY': _s.degraded_restart_delay,
        'LAG_SCALE_THRESHOLD': _s.lag_scale_threshold,
        'CRITICAL_LAG_THRESHOLD': _s.critical_lag_threshold,
        'LOG_LEVEL': _s.log_level,
    }
    if name in _map:
        return _map[name]
    raise AttributeError(f"module 'config.settings' has no attribute {name!r}")


# Eagerly resolve the backward-compatible constants so existing imports work.
_settings = get_settings()
ACIS_KAFKA_BOOTSTRAP_SERVERS = _settings.kafka_bootstrap_servers
ACIS_KAFKA_BACKEND = _settings.kafka_backend
DEFAULT_MAX_REPLICAS = _settings.default_max_replicas
DECISION_INTERVAL = _settings.decision_interval
HEARTBEAT_INTERVAL = _settings.heartbeat_interval
HEALTH_SCORE_THRESHOLD = _settings.health_score_threshold
RESTART_COOLDOWN = _settings.restart_cooldown
SCALE_COOLDOWN = _settings.scale_cooldown
SPAWN_COOLDOWN = _settings.spawn_cooldown
FALLBACK_COOLDOWN = _settings.fallback_cooldown
RECOVERY_EVENT_COOLDOWN = _settings.recovery_event_cooldown
PLACEMENT_REQUEST_COOLDOWN = _settings.placement_request_cooldown
DEGRADED_RESTART_DELAY = _settings.degraded_restart_delay
LAG_SCALE_THRESHOLD = _settings.lag_scale_threshold
CRITICAL_LAG_THRESHOLD = _settings.critical_lag_threshold
LOG_LEVEL = _settings.log_level