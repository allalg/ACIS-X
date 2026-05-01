"""
tests/regular_tests/conftest.py

Shared fixtures for the ACIS-X regular (functional/smoke) test suite.
These are inherited from the root conftest.py; add any regular_tests-specific
fixtures here.
"""
# All fixtures (mock_kafka_client, temp_db_path, db_agent, query_agent,
# sample_customer_event, sample_invoice_event) are provided by the root
# conftest.py and automatically available here via pytest's fixture discovery.

# No additional fixtures are needed for regular tests.
