import pytest
from unittest.mock import MagicMock, patch
import numpy as np

from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
from schemas.event_schema import Event

@pytest.fixture
def mock_kafka():
    return MagicMock()

@pytest.fixture
def ppa(mock_kafka):
    agent = PaymentPredictionAgent(kafka_client=mock_kafka)
    # Don't publish during tests
    agent.publish_event = MagicMock()
    return agent

class TestPaymentPredictionML:
    def test_model_warmup(self, ppa):
        """Test that the ML model and SHAP explainer are properly initialized."""
        assert hasattr(ppa, "model")
        assert hasattr(ppa, "explainer")
        assert hasattr(ppa, "expected_value")
        
        # Verify it trained on 8 features
        assert ppa.model.n_features_in_ == 8
        assert ppa.model.n_estimators == 50

    @patch("utils.query_client.QueryClient.query")
    def test_process_event_2_stage_policy(self, mock_query, ppa):
        """Test that the 2-stage policy (ML + Rules) correctly clamps and adds penalties."""
        # Mock customer data
        def mock_query_side_effect(query_name, params, **kwargs):
            if query_name == "get_customer":
                return {"credit_limit": 10000}
            elif query_name == "get_invoices_by_customer":
                return {"invoices": [{"invoice_id": "inv_1", "amount": 1000, "status": "pending"}]}
            return {}
            
        mock_query.side_effect = mock_query_side_effect
        
        # Inject severe litigation risk into external cache
        ppa.external_cache["cust_1"] = {
            "cached_at": 9999999999,
            "data": {
                "financial_risk": 0.2,
                "combined_risk": 0.3,
                "litigation_risk": 0.8  # > 0.5 triggers +0.25 penalty
            }
        }
        
        event = Event(
            event_id="test_event_1",
            event_source="test",
            event_time=1234567890.0,
            event_type="customer.metrics.updated",
            entity_id="cust_1",
            payload={
                "customer_id": "cust_1",
                "total_outstanding": 1000,
                "rating": "C", # Triggers +0.20 penalty
                "avg_delay": 0,
                "on_time_ratio": 1.0
            }
        )
        
        ppa.process_event(event)
        
        # Verify the published event has the explicit policy adjustments
        ppa.publish_event.assert_called_once()
        published_payload = ppa.publish_event.call_args[1]["payload"]
        
        assert "risk_score" in published_payload
        assert published_payload["shap_rating_adjustment"] == 0.20
        assert published_payload["shap_litigation_adjustment"] == 0.25
        assert published_payload["risk_category"] in ["high", "medium", "low"]
        assert "shap_values" in published_payload
        assert len(published_payload["shap_values"]) == 8

    @patch("utils.query_client.QueryClient.query")
    def test_shap_values_integrity(self, mock_query, ppa):
        """Test that SHAP values decompose the ML prediction correctly."""
        def mock_query_side_effect(query_name, params, **kwargs):
            if query_name == "get_customer":
                return {"credit_limit": 10000}
            elif query_name == "get_invoices_by_customer":
                return {"invoices": [{"invoice_id": "inv_2", "amount": 5000, "status": "pending"}]}
            return {}
            
        mock_query.side_effect = mock_query_side_effect
        
        # Clean external cache
        ppa.external_cache = {}
        
        event = Event(
            event_id="test_event_2",
            event_source="test",
            event_time=1234567890.0,
            event_type="customer.metrics.updated",
            entity_id="cust_2",
            payload={
                "customer_id": "cust_2",
                "total_outstanding": 5000,
                "rating": "A",
                "avg_delay": 15,
                "on_time_ratio": 0.5
            }
        )
        
        ppa.process_event(event)
        
        ppa.publish_event.assert_called_once()
        published_payload = ppa.publish_event.call_args[1]["payload"]
        
        shap_sum = published_payload["shap_sum"]
        baseline = published_payload["shap_baseline"]
        
        # The ML base risk score is roughly baseline + shap_sum
        # Since Random Forest output = Expected Value + Sum of SHAP
        ml_risk_score = baseline + shap_sum
        
        # Because rating A and no litigation, policy adjustments are 0
        assert published_payload["shap_rating_adjustment"] == 0.0
        assert published_payload["shap_litigation_adjustment"] == 0.0
        
        # Final risk score should match ML score (accounting for floating point and rounding)
        # We round in the agent to 4 decimal places
        assert abs(published_payload["risk_score"] - round(ml_risk_score, 4)) < 0.01
