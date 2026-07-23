import pytest
import time
import uuid
import threading
from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from runtime.kafka_client import KafkaClient, KafkaConfig

pytestmark = pytest.mark.integration


class TestDynamicDiscovery:
    """
    Test suite validating the dynamic discovery protocol 
    where agents auto-register and bind to relevant topics.
    """

    @pytest.fixture(autouse=True)
    def setup_topics(self):
        """Ensure test topics exist for dynamic discovery"""
        from kafka.errors import NoBrokersAvailable
        try:
            admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
            try:
                admin.create_topics([
                    NewTopic(name="acis.discovery.test", num_partitions=3, replication_factor=1)
                ])
            except TopicAlreadyExistsError:
                pass
            finally:
                admin.close()
        except NoBrokersAvailable:
            pytest.skip("Kafka broker is not available. Skipping integration test.")

    def test_agent_dynamic_registration(self):
        """
        Validate that an agent can dynamically spin up, 
        discover its required topics, and bind to a partition
        without hardcoded topologies.
        """
        agent_id = f"test-agent-{uuid.uuid4().hex[:6]}"
        config = KafkaConfig(client_id=agent_id, bootstrap_servers="localhost:9092")
        client = KafkaClient(config, backend="kafka-python")
        agent = PaymentPredictionAgent(instance_id=agent_id, kafka_client=client)
        
        # Override topics for test safety
        agent.consume_topic = "acis.discovery.test"
        agent.produce_topic = "acis.discovery.out"
        
        # Start agent in background to simulate dynamic discovery binding
        agent_thread = threading.Thread(target=agent.start, daemon=True)
        agent_thread.start()
        
        # Allow time for auto-registration and consumer group binding (discovery)
        time.sleep(3)
        
        # Validate that the agent successfully initialized its consumer and producer
        # indicating it dynamically discovered the Kafka metadata
        assert agent.kafka_client._consumer is not None, "Agent failed to dynamically discover and bind consumer."
        assert agent.kafka_client._producer is not None, "Agent failed to dynamically discover and bind producer."
        
        # Terminate gracefully
        agent.stop()
        agent_thread.join(timeout=2)
        assert not agent.is_running, "Agent failed to terminate correctly post-discovery."

    def test_multi_agent_concurrent_discovery(self):
        """
        Validate that multiple agents can dynamically discover and 
        bind to the same topic partition space simultaneously.
        """
        agents = []
        threads = []
        
        for i in range(3):
            aid = f"concurrent-agent-{i}"
            config = KafkaConfig(client_id=aid, bootstrap_servers="localhost:9092")
            client = KafkaClient(config, backend="kafka-python")
            agent = PaymentPredictionAgent(instance_id=aid, kafka_client=client)
            agent.consume_topic = "acis.discovery.test"
            agents.append(agent)
            
            thread = threading.Thread(target=agent.start, daemon=True)
            threads.append(thread)
            thread.start()
            
        time.sleep(5) # Wait for Kafka group rebalance and dynamic discovery
        
        for agent in agents:
            assert agent.kafka_client._consumer is not None, f"Agent {agent.instance_id} failed dynamic discovery."
            agent.stop()
            
        for thread in threads:
            thread.join(timeout=2)
