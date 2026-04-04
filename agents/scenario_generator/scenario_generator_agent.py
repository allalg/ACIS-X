import random
import threading
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class ScenarioGeneratorAgent(BaseAgent):
    """
    Scenario Generator Agent for ACIS-X.

    Generates synthetic test data including:
    - Customer profiles
    - Invoices
    - Payments

    Produces to:
    - acis.customers
    - acis.invoices
    - acis.payments
    - acis.metrics
    - acis.commands
    """

    # Topic constants
    TOPIC_CUSTOMERS = "acis.customers"
    TOPIC_INVOICES = "acis.invoices"
    TOPIC_PAYMENTS = "acis.payments"
    TOPIC_METRICS = "acis.metrics"
    TOPIC_COMMANDS = "acis.commands"

    # Synthetic data constants
    INDUSTRIES = [
        "manufacturing", "technology", "healthcare", "finance",
        "retail", "construction", "logistics", "energy",
        "telecommunications", "pharmaceuticals"
    ]

    COUNTRIES = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "SG", "NL", "CH"]

    CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY", "SGD", "CHF"]

    RATINGS = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C", "D"]

    RISK_LEVELS = ["low", "medium", "high", "critical"]

    PAYMENT_METHODS = ["wire_transfer", "ach", "check", "credit_card", "direct_debit"]

    COMPANY_PREFIXES = [
        "Acme", "Global", "United", "Pacific", "Atlantic", "Summit",
        "Pioneer", "Apex", "Vertex", "Horizon", "Quantum", "Synergy",
        "Nexus", "Pinnacle", "Sterling", "Premier", "Elite", "Prime"
    ]

    COMPANY_SUFFIXES = [
        "Corp", "Inc", "LLC", "Ltd", "Holdings", "Group", "Industries",
        "Solutions", "Technologies", "Enterprises", "Partners", "Systems"
    ]

    def __init__(
        self,
        kafka_client: Any,
        generation_interval_seconds: float = 5.0,
        customers_per_batch: int = 1,
        invoices_per_batch: int = 2,
        payments_per_batch: int = 1,
    ):
        super().__init__(
            agent_name="ScenarioGeneratorAgent",
            agent_version="1.0.0",
            group_id="scenario-generator-group",
            subscribed_topics=[],  # Generator doesn't consume
            capabilities=[
                "customer_generation",
                "invoice_generation",
                "payment_generation",
                "scenario_simulation",
            ],
            kafka_client=kafka_client,
            agent_type="ScenarioGeneratorAgent",
        )

        self.generation_interval = generation_interval_seconds
        self.customers_per_batch = customers_per_batch
        self.invoices_per_batch = invoices_per_batch
        self.payments_per_batch = payments_per_batch

        # In-memory state for data relationships
        self._customers: Dict[str, Dict[str, Any]] = {}
        self._invoices: Dict[str, Dict[str, Any]] = {}
        self._customer_counter = 0
        self._invoice_counter = 0
        self._payment_counter = 0

        # Generator thread
        self._generator_thread: Optional[threading.Thread] = None

    # -------------------------------------------------------------------------
    # BaseAgent abstract methods
    # -------------------------------------------------------------------------

    def subscribe(self) -> List[str]:
        """ScenarioGenerator doesn't consume any topics."""
        return []

    def process_event(self, event: Event) -> None:
        """ScenarioGenerator doesn't process events."""
        pass

    # -------------------------------------------------------------------------
    # Lifecycle overrides
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start the generator with the generation loop."""
        logger.info("Starting ScenarioGeneratorAgent")

        self._running = True
        self._start_time = datetime.utcnow()

        topics = self.subscribe()
        self.subscribed_topics = topics

        # Register with registry
        self._register_with_registry()

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"{self.agent_name}-heartbeat"
        )
        self._heartbeat_thread.start()

        # Start generator thread
        self._generator_thread = threading.Thread(
            target=self._generation_loop,
            daemon=True,
            name=f"{self.agent_name}-generator"
        )
        self._generator_thread.start()

        logger.info("ScenarioGeneratorAgent started")

    def stop(self) -> None:
        """Stop the generator."""
        logger.info("Stopping ScenarioGeneratorAgent")

        self._running = False
        self._shutdown_event.set()

        # Deregister from registry
        self._deregister_from_registry()

        # Wait for threads
        if self._generator_thread and self._generator_thread.is_alive():
            self._generator_thread.join(timeout=5)

        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=5)

        self.kafka_client.close()
        logger.info("ScenarioGeneratorAgent stopped")

    # -------------------------------------------------------------------------
    # Generation loop
    # -------------------------------------------------------------------------

    def _generation_loop(self) -> None:
        """Main loop that generates synthetic data at intervals."""
        logger.info("Generation loop started")

        while self._running:
            try:
                self._generate_batch()
            except Exception as e:
                logger.error(f"Error in generation loop: {e}")

            # Wait for next interval or shutdown
            self._shutdown_event.wait(timeout=self.generation_interval)

        logger.info("Generation loop stopped")

    def _generate_batch(self) -> None:
        """Generate a batch of synthetic events."""
        correlation_id = self.create_correlation_id()

        # Generate customers
        for _ in range(self.customers_per_batch):
            self._generate_customer(correlation_id)

        # Generate invoices (need existing customers)
        if self._customers:
            for _ in range(self.invoices_per_batch):
                self._generate_invoice(correlation_id)

        # Generate payments (need existing invoices)
        if self._invoices:
            for _ in range(self.payments_per_batch):
                self._generate_payment(correlation_id)

        # Occasionally generate special scenarios
        if random.random() < 0.1:  # 10% chance
            self._generate_scenario(correlation_id)

    # -------------------------------------------------------------------------
    # Customer generation
    # -------------------------------------------------------------------------

    def _generate_customer(self, correlation_id: str) -> str:
        """Generate a new customer or update existing one."""
        # Decide: new customer or update existing
        if self._customers and random.random() < 0.3:  # 30% updates
            return self._update_customer(correlation_id)

        return self._create_customer(correlation_id)

    def _create_customer(self, correlation_id: str) -> str:
        """Create a new customer."""
        self._customer_counter += 1
        customer_id = f"cust_{self._customer_counter:05d}"

        country = random.choice(self.COUNTRIES)
        currency = self._get_currency_for_country(country)

        customer_data = {
            "customer_id": customer_id,
            "customer_name": self._generate_company_name(),
            "credit_limit": self._generate_credit_limit(),
            "currency": currency,
            "risk_level": random.choices(
                self.RISK_LEVELS,
                weights=[0.5, 0.3, 0.15, 0.05]  # Weighted toward lower risk
            )[0],
            "industry": random.choice(self.INDUSTRIES),
            "country": country,
            "rating": random.choices(
                self.RATINGS,
                weights=[0.05, 0.1, 0.15, 0.25, 0.2, 0.1, 0.08, 0.04, 0.02, 0.01]
            )[0],
            "status": "active",
            "updated_at": datetime.utcnow().isoformat(),
        }

        # Store customer
        self._customers[customer_id] = customer_data

        # Publish event
        self.publish_event(
            topic=self.TOPIC_CUSTOMERS,
            event_type="customer.profile.updated",
            entity_id=customer_id,
            payload=customer_data,
            correlation_id=correlation_id,
        )

        logger.info(f"Created customer: {customer_id}")
        return customer_id

    def _update_customer(self, correlation_id: str) -> str:
        """Update an existing customer."""
        customer_id = random.choice(list(self._customers.keys()))
        customer = self._customers[customer_id].copy()

        # Store previous values
        previous_values = {}

        # Randomly update some fields
        if random.random() < 0.3:
            previous_values["credit_limit"] = customer["credit_limit"]
            customer["credit_limit"] = self._generate_credit_limit()

        if random.random() < 0.2:
            previous_values["risk_level"] = customer["risk_level"]
            customer["risk_level"] = random.choice(self.RISK_LEVELS)

        if random.random() < 0.1:
            previous_values["rating"] = customer["rating"]
            current_idx = self.RATINGS.index(customer["rating"])
            # Rating can move up or down by 1-2 notches
            new_idx = max(0, min(len(self.RATINGS) - 1, current_idx + random.randint(-2, 2)))
            customer["rating"] = self.RATINGS[new_idx]

        customer["updated_at"] = datetime.utcnow().isoformat()
        customer["previous_values"] = previous_values

        # Update stored customer
        self._customers[customer_id] = customer

        # Publish event
        self.publish_event(
            topic=self.TOPIC_CUSTOMERS,
            event_type="customer.profile.updated",
            entity_id=customer_id,
            payload=customer,
            correlation_id=correlation_id,
        )

        logger.info(f"Updated customer: {customer_id}")
        return customer_id

    # -------------------------------------------------------------------------
    # Invoice generation
    # -------------------------------------------------------------------------

    def _generate_invoice(self, correlation_id: str) -> str:
        """Generate a new invoice or update existing one."""
        # Decide: new invoice or status change
        if self._invoices and random.random() < 0.2:  # 20% status updates
            return self._update_invoice_status(correlation_id)

        return self._create_invoice(correlation_id)

    def _create_invoice(self, correlation_id: str) -> str:
        """Create a new invoice."""
        self._invoice_counter += 1
        invoice_id = f"inv_{self._invoice_counter:05d}"

        # Pick a random customer
        customer_id = random.choice(list(self._customers.keys()))
        customer = self._customers[customer_id]

        amount = self._generate_invoice_amount(customer["credit_limit"])
        created_at = datetime.utcnow()
        due_date = created_at + timedelta(days=random.choice([15, 30, 45, 60, 90]))

        invoice_data = {
            "invoice_id": invoice_id,
            "customer_id": customer_id,
            "amount": amount,
            "remaining_amount": amount,
            "currency": customer["currency"],
            "due_date": due_date.strftime("%Y-%m-%d"),
            "status": "pending",
            "created_at": created_at.isoformat(),
            "updated_at": None,
            "line_items": self._generate_line_items(amount),
            "notes": None,
        }

        # Store invoice
        self._invoices[invoice_id] = invoice_data

        # Publish event
        self.publish_event(
            topic=self.TOPIC_INVOICES,
            event_type="invoice.created",
            entity_id=customer_id,
            payload=invoice_data,
            correlation_id=correlation_id,
        )

        logger.info(f"Created invoice: {invoice_id} for {customer_id}")
        return invoice_id

    def _update_invoice_status(self, correlation_id: str) -> str:
        """Update invoice status (overdue, disputed, etc.)."""
        # Find pending invoices
        pending_invoices = [
            inv_id for inv_id, inv in self._invoices.items()
            if inv["status"] == "pending"
        ]

        if not pending_invoices:
            return self._create_invoice(correlation_id)

        invoice_id = random.choice(pending_invoices)
        invoice = self._invoices[invoice_id].copy()

        # Determine new status
        status_choice = random.choices(
            ["overdue", "disputed", "cancelled"],
            weights=[0.6, 0.25, 0.15]
        )[0]

        invoice["status"] = status_choice
        invoice["updated_at"] = datetime.utcnow().isoformat()

        if status_choice == "overdue":
            invoice["days_overdue"] = random.randint(1, 30)
        elif status_choice == "disputed":
            invoice["dispute_reason"] = random.choice([
                "Services not delivered as agreed",
                "Incorrect amount billed",
                "Quality issues with delivered goods",
                "Duplicate invoice",
            ])
        elif status_choice == "cancelled":
            invoice["cancellation_reason"] = random.choice([
                "Order cancelled by customer",
                "Duplicate invoice voided",
                "Contract terminated",
            ])

        # Update stored invoice
        self._invoices[invoice_id] = invoice

        # Publish event
        event_type = f"invoice.{status_choice}"
        self.publish_event(
            topic=self.TOPIC_INVOICES,
            event_type=event_type,
            entity_id=invoice["customer_id"],
            payload=invoice,
            correlation_id=correlation_id,
        )

        logger.info(f"Updated invoice {invoice_id} to {status_choice}")
        return invoice_id

    # -------------------------------------------------------------------------
    # Payment generation
    # -------------------------------------------------------------------------

    def _generate_payment(self, correlation_id: str) -> str:
        """Generate a payment for an existing invoice."""
        # Find invoices that can receive payments
        payable_invoices = [
            inv_id for inv_id, inv in self._invoices.items()
            if inv["status"] in ["pending", "overdue"]
        ]

        if not payable_invoices:
            return ""

        invoice_id = random.choice(payable_invoices)
        invoice = self._invoices[invoice_id]

        # Safety check: invoice should exist
        if invoice_id not in self._invoices:
            logger.warning(f"Skipping payment: invoice {invoice_id} not created")
            return ""

        self._payment_counter += 1
        payment_id = f"pay_{self._payment_counter:05d}"

        # Get payable amount (use remaining_amount if partial paid, else full amount)
        payable_amount = invoice.get("remaining_amount", invoice["amount"])

        # Decide: full or partial payment
        is_partial = random.random() < 0.2  # 20% partial payments

        if is_partial:
            amount = round(payable_amount * random.uniform(0.3, 0.7), 2)
            status = "partial"
            remaining = round(payable_amount - amount, 2)
        else:
            amount = payable_amount
            status = "completed"
            remaining = 0

        payment_data = {
            "payment_id": payment_id,
            "invoice_id": invoice_id,
            "customer_id": invoice["customer_id"],
            "amount": amount,
            "currency": invoice["currency"],
            "payment_date": datetime.utcnow().isoformat(),
            "payment_method": random.choice(self.PAYMENT_METHODS),
            "status": status,
            "reference": f"{self.PAYMENT_METHODS[0].upper()[:2]}-{datetime.utcnow().strftime('%Y-%m-%d')}-{payment_id}",
        }

        if is_partial:
            payment_data["remaining_balance"] = remaining
            # Track remaining amount in internal state
            self._invoices[invoice_id]["remaining_amount"] = remaining
            self._invoices[invoice_id]["updated_at"] = datetime.utcnow().isoformat()
        else:
            # Update invoice status if fully paid
            self._invoices[invoice_id]["status"] = "paid"
            self._invoices[invoice_id]["updated_at"] = datetime.utcnow().isoformat()

        # Publish event
        event_type = "payment.partial" if is_partial else "payment.received"
        self.publish_event(
            topic=self.TOPIC_PAYMENTS,
            event_type=event_type,
            entity_id=invoice["customer_id"],
            payload=payment_data,
            correlation_id=correlation_id,
        )

        logger.info(f"Created payment: {payment_id} for invoice {invoice_id}")
        return payment_id

    # -------------------------------------------------------------------------
    # Scenario generation
    # -------------------------------------------------------------------------

    def _generate_scenario(self, correlation_id: str) -> None:
        """Generate special scenarios for testing."""
        scenario = random.choice([
            "high_risk_customer",
            "overdue_cascade",
            "external_data_request",
            "payment_pattern_change",
        ])

        logger.info(f"Generating scenario: {scenario}")

        if scenario == "high_risk_customer":
            self._scenario_high_risk_customer(correlation_id)
        elif scenario == "overdue_cascade":
            self._scenario_overdue_cascade(correlation_id)
        elif scenario == "external_data_request":
            self._scenario_external_data_request(correlation_id)
        elif scenario == "payment_pattern_change":
            self._scenario_payment_pattern(correlation_id)

    def _scenario_high_risk_customer(self, correlation_id: str) -> None:
        """Create a high-risk customer scenario."""
        customer_id = self._create_customer(correlation_id)
        customer = self._customers[customer_id]

        # Update to high risk
        customer["risk_level"] = "high"
        customer["rating"] = random.choice(["CCC", "CC", "C"])
        customer["previous_values"] = {
            "risk_level": "medium",
            "rating": "BB",
        }
        customer["updated_at"] = datetime.utcnow().isoformat()

        self._customers[customer_id] = customer

        self.publish_event(
            topic=self.TOPIC_CUSTOMERS,
            event_type="customer.profile.updated",
            entity_id=customer_id,
            payload=customer,
            correlation_id=correlation_id,
        )

        # Create multiple invoices for this customer
        for _ in range(3):
            self._invoice_counter += 1
            invoice_id = f"inv_{self._invoice_counter:05d}"

            created_at = datetime.utcnow() - timedelta(days=random.randint(45, 90))
            due_date = datetime.utcnow() - timedelta(days=random.randint(5, 30))

            amount = self._generate_invoice_amount(customer["credit_limit"])
            invoice_data = {
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "amount": amount,
                "remaining_amount": amount,
                "currency": customer["currency"],
                "due_date": due_date.strftime("%Y-%m-%d"),
                "status": "pending",
                "created_at": created_at.isoformat(),
                "updated_at": None,
                "line_items": None,
                "notes": None,
            }

            self._invoices[invoice_id] = invoice_data

            # Step 1: Publish invoice.created
            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.created",
                entity_id=customer_id,
                payload=invoice_data,
                correlation_id=correlation_id,
            )

            # Step 2: Publish invoice.overdue
            invoice_data_overdue = invoice_data.copy()
            invoice_data_overdue["status"] = "overdue"
            invoice_data_overdue["updated_at"] = datetime.utcnow().isoformat()
            invoice_data_overdue["days_overdue"] = random.randint(5, 30)

            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.overdue",
                entity_id=customer_id,
                payload=invoice_data_overdue,
                correlation_id=correlation_id,
            )

            # Update internal state to match published state
            self._invoices[invoice_id] = invoice_data_overdue

    def _scenario_overdue_cascade(self, correlation_id: str) -> None:
        """Create multiple overdue invoices for a customer."""
        if not self._customers:
            return

        customer_id = random.choice(list(self._customers.keys()))
        customer = self._customers[customer_id]

        for i in range(random.randint(2, 4)):
            self._invoice_counter += 1
            invoice_id = f"inv_{self._invoice_counter:05d}"

            days_overdue = (i + 1) * 10
            created_at = datetime.utcnow() - timedelta(days=days_overdue + 30)
            due_date = datetime.utcnow() - timedelta(days=days_overdue)

            amount = self._generate_invoice_amount(customer["credit_limit"])
            invoice_data = {
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "amount": amount,
                "remaining_amount": amount,
                "currency": customer["currency"],
                "due_date": due_date.strftime("%Y-%m-%d"),
                "status": "pending",
                "created_at": created_at.isoformat(),
                "updated_at": None,
                "line_items": None,
                "notes": None,
            }

            self._invoices[invoice_id] = invoice_data

            # Step 1: Publish invoice.created
            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.created",
                entity_id=customer_id,
                payload=invoice_data,
                correlation_id=correlation_id,
            )

            # Step 2: Publish invoice.overdue
            invoice_data_overdue = invoice_data.copy()
            invoice_data_overdue["status"] = "overdue"
            invoice_data_overdue["updated_at"] = datetime.utcnow().isoformat()
            invoice_data_overdue["days_overdue"] = days_overdue

            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.overdue",
                entity_id=customer_id,
                payload=invoice_data_overdue,
                correlation_id=correlation_id,
            )

            # Update internal state to match published state
            self._invoices[invoice_id] = invoice_data_overdue

    def _scenario_external_data_request(self, correlation_id: str) -> None:
        """Simulate external data request and response."""
        if not self._customers:
            return

        customer_id = random.choice(list(self._customers.keys()))

        # Request external data
        request_payload = {
            "command_type": "fetch_external_data",
            "target_agent": "ExternalIntelligenceAgent",
            "customer_id": customer_id,
            "priority": "normal",
            "request_details": {
                "data_type": "credit_report",
                "source_preference": ["Experian", "TransUnion"],
            },
            "requester": self.agent_name,
        }

        self.publish_event(
            topic=self.TOPIC_COMMANDS,
            event_type="external.data.requested",
            entity_id=customer_id,
            payload=request_payload,
            correlation_id=correlation_id,
        )

        # Simulate external data response
        external_payload = {
            "customer_id": customer_id,
            "source": random.choice(["Experian", "TransUnion", "Equifax"]),
            "source_type": "credit_bureau",
            "score": random.randint(550, 850),
            "risk_flag": random.random() < 0.2,
            "details": {
                "credit_score": random.randint(550, 850),
                "payment_history": random.choice(["excellent", "good", "fair", "poor"]),
                "credit_utilization": round(random.uniform(0.1, 0.9), 2),
                "accounts_in_good_standing": random.randint(3, 15),
                "derogatory_marks": random.randint(0, 3),
                "hard_inquiries": random.randint(0, 5),
            },
            "fetched_at": datetime.utcnow().isoformat(),
            "valid_until": (datetime.utcnow() + timedelta(days=30)).isoformat(),
        }

        self.publish_event(
            topic=self.TOPIC_METRICS,
            event_type="external.credit.report.received",
            entity_id=customer_id,
            payload=external_payload,
            correlation_id=correlation_id,
        )

    def _scenario_payment_pattern(self, correlation_id: str) -> None:
        """Simulate changing payment patterns."""
        if not self._customers:
            return

        customer_id = random.choice(list(self._customers.keys()))
        customer = self._customers[customer_id]

        # Create a series of late partial payments
        for i in range(3):
            self._invoice_counter += 1
            invoice_id = f"inv_{self._invoice_counter:05d}"

            amount = self._generate_invoice_amount(customer["credit_limit"])
            due_date = datetime.utcnow() - timedelta(days=(i + 1) * 15)
            created_at = due_date - timedelta(days=30)

            invoice_data = {
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "amount": amount,
                "remaining_amount": amount,
                "currency": customer["currency"],
                "due_date": due_date.strftime("%Y-%m-%d"),
                "status": "pending",
                "created_at": created_at.isoformat(),
                "updated_at": None,
                "line_items": None,
                "notes": None,
            }

            self._invoices[invoice_id] = invoice_data

            # Step 1: Publish invoice.created
            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.created",
                entity_id=customer_id,
                payload=invoice_data,
                correlation_id=correlation_id,
            )

            # Step 2: Publish invoice.overdue
            invoice_data_overdue = invoice_data.copy()
            invoice_data_overdue["status"] = "overdue"
            invoice_data_overdue["updated_at"] = datetime.utcnow().isoformat()
            invoice_data_overdue["days_overdue"] = (i + 1) * 15

            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.overdue",
                entity_id=customer_id,
                payload=invoice_data_overdue,
                correlation_id=correlation_id,
            )

            # Update internal state to match published state
            self._invoices[invoice_id] = invoice_data_overdue

            # Generate partial payment
            self._payment_counter += 1
            payment_id = f"pay_{self._payment_counter:05d}"

            partial_amount = round(amount * random.uniform(0.3, 0.5), 2)

            payment_data = {
                "payment_id": payment_id,
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "amount": partial_amount,
                "currency": customer["currency"],
                "payment_date": datetime.utcnow().isoformat(),
                "payment_method": random.choice(self.PAYMENT_METHODS),
                "status": "partial",
                "reference": f"PAT-{datetime.utcnow().strftime('%Y-%m-%d')}-{payment_id}",
                "remaining_balance": round(amount - partial_amount, 2),
            }

            self.publish_event(
                topic=self.TOPIC_PAYMENTS,
                event_type="payment.partial",
                entity_id=customer_id,
                payload=payment_data,
                correlation_id=correlation_id,
            )

    # -------------------------------------------------------------------------
    # Helper methods
    # -------------------------------------------------------------------------

    def _generate_company_name(self) -> str:
        """Generate a random company name."""
        prefix = random.choice(self.COMPANY_PREFIXES)
        suffix = random.choice(self.COMPANY_SUFFIXES)
        return f"{prefix} {suffix}"

    def _generate_credit_limit(self) -> float:
        """Generate a realistic credit limit."""
        # Log-normal distribution for realistic spread
        base = random.choice([50000, 100000, 250000, 500000, 1000000])
        variation = random.uniform(0.5, 1.5)
        return round(base * variation, 2)

    def _generate_invoice_amount(self, credit_limit: float) -> float:
        """Generate invoice amount based on customer credit limit."""
        # Invoice typically 5-30% of credit limit
        percentage = random.uniform(0.05, 0.30)
        amount = credit_limit * percentage
        return round(amount, 2)

    def _generate_line_items(self, total_amount: float) -> List[Dict[str, Any]]:
        """Generate invoice line items."""
        num_items = random.randint(1, 4)
        items = []

        remaining = total_amount
        item_descriptions = [
            "Consulting Services",
            "Software License",
            "Support & Maintenance",
            "Professional Services",
            "Hardware Equipment",
            "Training Services",
            "Implementation Fee",
            "Monthly Subscription",
        ]

        for i in range(num_items):
            if i == num_items - 1:
                item_amount = remaining
            else:
                item_amount = round(remaining * random.uniform(0.2, 0.5), 2)
                remaining -= item_amount

            items.append({
                "description": random.choice(item_descriptions),
                "amount": round(item_amount, 2),
            })

        return items

    def _get_currency_for_country(self, country: str) -> str:
        """Get appropriate currency for country."""
        country_currency = {
            "US": "USD",
            "GB": "GBP",
            "DE": "EUR",
            "FR": "EUR",
            "CA": "CAD",
            "AU": "AUD",
            "JP": "JPY",
            "SG": "SGD",
            "NL": "EUR",
            "CH": "CHF",
        }
        return country_currency.get(country, "USD")

    # -------------------------------------------------------------------------
    # Registry override
    # -------------------------------------------------------------------------

    def _register_with_registry(self) -> None:
        """Register with registry including produced topics."""
        registration_payload = {
            "agent_id": self._get_agent_id(),
            "agent_name": self.agent_name,
            "agent_type": self.agent_type,
            "capabilities": self.capabilities,
            "topics": {
                "consumes": self.subscribed_topics,
                "produces": [
                    self.TOPIC_CUSTOMERS,
                    self.TOPIC_INVOICES,
                    self.TOPIC_PAYMENTS,
                    self.TOPIC_METRICS,
                    self.TOPIC_COMMANDS,
                ],
            },
            "group_id": self.group_id,
            "version": self.agent_version,
            "status": "registered",
            "host": self.host,
            "instance_id": self.instance_id,
            "registered_at": datetime.utcnow().isoformat(),
            "replica_index": self.replica_index,
            "replica_count": self.replica_count,
            "max_replicas": self.max_replicas,
        }

        self.publish_event(
            topic=self.REGISTRY_TOPIC,
            event_type="registry.agent.registered",
            entity_id=self.agent_name,
            payload=registration_payload,
            correlation_id=None,
            metadata={"environment": "production"},
        )

        logger.info(f"Agent {self.agent_name} registered with registry")

    # -------------------------------------------------------------------------
    # Manual generation methods (for external triggering)
    # -------------------------------------------------------------------------

    def generate_customer(self) -> str:
        """Manually trigger customer generation."""
        correlation_id = self.create_correlation_id()
        return self._create_customer(correlation_id)

    def generate_invoice(self, customer_id: Optional[str] = None) -> str:
        """Manually trigger invoice generation."""
        if customer_id and customer_id not in self._customers:
            raise ValueError(f"Customer {customer_id} not found")

        correlation_id = self.create_correlation_id()

        if customer_id:
            # Generate for specific customer
            self._invoice_counter += 1
            invoice_id = f"inv_{self._invoice_counter:05d}"
            customer = self._customers[customer_id]

            amount = self._generate_invoice_amount(customer["credit_limit"])
            invoice_data = {
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "amount": amount,
                "remaining_amount": amount,
                "currency": customer["currency"],
                "due_date": (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d"),
                "status": "pending",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": None,
                "line_items": None,
                "notes": None,
            }

            self._invoices[invoice_id] = invoice_data

            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.created",
                entity_id=customer_id,
                payload=invoice_data,
                correlation_id=correlation_id,
            )

            return invoice_id

        return self._create_invoice(correlation_id)

    def generate_payment(self, invoice_id: str) -> str:
        """Manually trigger payment generation for specific invoice."""
        if invoice_id not in self._invoices:
            raise ValueError(f"Invoice {invoice_id} not found")

        correlation_id = self.create_correlation_id()
        invoice = self._invoices[invoice_id]

        self._payment_counter += 1
        payment_id = f"pay_{self._payment_counter:05d}"

        payment_data = {
            "payment_id": payment_id,
            "invoice_id": invoice_id,
            "customer_id": invoice["customer_id"],
            "amount": invoice["amount"],
            "currency": invoice["currency"],
            "payment_date": datetime.utcnow().isoformat(),
            "payment_method": random.choice(self.PAYMENT_METHODS),
            "status": "completed",
            "reference": f"MAN-{datetime.utcnow().strftime('%Y-%m-%d')}-{payment_id}",
        }

        self._invoices[invoice_id]["status"] = "paid"
        self._invoices[invoice_id]["updated_at"] = datetime.utcnow().isoformat()

        self.publish_event(
            topic=self.TOPIC_PAYMENTS,
            event_type="payment.received",
            entity_id=invoice["customer_id"],
            payload=payment_data,
            correlation_id=correlation_id,
        )

        return payment_id
