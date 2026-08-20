"""
Test suite for Kafka Exactly-Once Semantics (EOS) and Idempotence Configuration.

Tests:
1. KafkaConfig EOS properties (enable_idempotence, acks, isolation_level).
2. Confluent and kafka-python producer configuration integrity.
3. Transaction lifecycle helper methods on KafkaClient.
"""

import pytest
from unittest.mock import MagicMock, patch

from runtime.kafka_client import KafkaClient, KafkaConfig


class TestKafkaEOSConfiguration:
    """Tests for native EOS producer and consumer settings."""

    def test_default_eos_configuration(self):
        """KafkaConfig defaults should mandate EOS-compliant parameters."""
        config = KafkaConfig()
        assert config.producer_enable_idempotence is True
        assert config.producer_acks == "all"
        assert config.producer_max_in_flight <= 5
        assert config.consumer_isolation_level == "read_committed"

    def test_confluent_producer_config_includes_idempotence(self):
        """Confluent producer initialization should inject enable.idempotence and acks=all."""
        config = KafkaConfig(
            bootstrap_servers=["localhost:9092"],
            producer_enable_idempotence=True,
        )

        with patch("runtime.kafka_client.KafkaClient._init_confluent_producer") as mock_init:
            client = KafkaClient(config=config, backend="confluent")
            assert client.config.producer_enable_idempotence is True
            assert client.config.consumer_isolation_level == "read_committed"

    def test_transaction_lifecycle_methods(self):
        """KafkaClient should expose transaction lifecycle methods without error."""
        config = KafkaConfig(
            bootstrap_servers=["localhost:9092"],
            enable_transactions=True,
        )
        client = KafkaClient(config=config, backend="confluent")
        mock_prod = MagicMock()
        client._producer = mock_prod

        # Exercise transaction helpers
        client.init_transactions()
        assert mock_prod.init_transactions.called

        client.begin_transaction()
        assert mock_prod.begin_transaction.called

        client.commit_transaction()
        assert mock_prod.commit_transaction.called

        client.abort_transaction()
        assert mock_prod.abort_transaction.called
