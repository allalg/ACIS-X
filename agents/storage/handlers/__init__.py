"""
Storage event handlers for ACIS-X DBAgent.

Each handler module provides a function that accepts a DBAgent instance
and an Event, performing the database write logic for a specific domain.
"""

from .invoice_handler import handle_invoice_upsert
from .payment_handler import handle_payment_received
from .collection_handler import handle_collection_action
from .customer_handler import handle_customer_profile
from .metrics_handler import handle_metrics_updated
from .risk_handler import (
    handle_litigation_event,
    handle_customer_risk_profile,
    handle_risk_scored,
)
from .schema import SCHEMA_DDL, SCHEMA_MIGRATIONS, SCHEMA_VERSION

__all__ = [
    "handle_invoice_upsert",
    "handle_payment_received",
    "handle_collection_action",
    "handle_customer_profile",
    "handle_metrics_updated",
    "handle_litigation_event",
    "handle_customer_risk_profile",
    "handle_risk_scored",
    "SCHEMA_DDL",
    "SCHEMA_MIGRATIONS",
    "SCHEMA_VERSION",
]
