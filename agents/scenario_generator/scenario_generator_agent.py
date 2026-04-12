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

    # India-only configuration
    COUNTRIES = ["IN"]
    CURRENCIES = ["INR"]

    # Real Indian companies (Listed)
    LISTED_INDIAN_COMPANIES = [
        {"name": "Reliance Industries Ltd", "industry": "energy"},
        {"name": "Tata Consultancy Services Ltd", "industry": "technology"},
        {"name": "Infosys Ltd", "industry": "technology"},
        {"name": "HDFC Bank Ltd", "industry": "finance"},
        {"name": "ICICI Bank Ltd", "industry": "finance"},
        {"name": "State Bank of India", "industry": "finance"},
        {"name": "Larsen & Toubro Ltd", "industry": "construction"},
        {"name": "Bharti Airtel Ltd", "industry": "telecommunications"},
        {"name": "Wipro Ltd", "industry": "technology"},
        {"name": "Adani Enterprises Ltd", "industry": "energy"},
        {"name": "ITC Ltd", "industry": "consumer_goods"},
        {"name": "Bajaj Auto Ltd", "industry": "automotive"},
        {"name": "Hindustan Unilever Ltd", "industry": "consumer_goods"},
        {"name": "Hero MotoCorp Ltd", "industry": "automotive"},
        {"name": "Axis Bank Ltd", "industry": "finance"},
        {"name": "Kotak Mahindra Bank Ltd", "industry": "finance"},
        {"name": "Asian Paints Ltd", "industry": "manufacturing"},
        {"name": "Maruti Suzuki India Ltd", "industry": "automotive"},
        {"name": "HCL Technologies Ltd", "industry": "technology"},
        {"name": "Tech Mahindra Ltd", "industry": "technology"},
    ]

    # Real Indian companies (Unlisted)
    UNLISTED_INDIAN_COMPANIES = [
        {"name": "Flipkart Internet Pvt Ltd", "industry": "retail"},
        {"name": "BYJU'S Learning Pvt Ltd", "industry": "education"},
        {"name": "Ola Electric Mobility Pvt Ltd", "industry": "automotive"},
        {"name": "Swiggy Pvt Ltd", "industry": "logistics"},
        {"name": "Zomato Pvt Ltd", "industry": "food_tech"},
        {"name": "Razorpay Software Pvt Ltd", "industry": "finance"},
        {"name": "Delhivery Pvt Ltd", "industry": "logistics"},
        {"name": "Dream11 Gaming Pvt Ltd", "industry": "gaming"},
        {"name": "Meesho Pvt Ltd", "industry": "ecommerce"},
        {"name": "Udaan Pvt Ltd", "industry": "b2b_commerce"},
        {"name": "Unacademy Pvt Ltd", "industry": "education"},
        {"name": "PharmEasy Pvt Ltd", "industry": "healthcare"},
        {"name": "Cure.fit Pvt Ltd", "industry": "healthcare"},
        {"name": "Vedantu Online Learning Pvt Ltd", "industry": "education"},
        {"name": "ShareChat Pvt Ltd", "industry": "technology"},
        {"name": "Rupeekh Finance Pvt Ltd", "industry": "finance"},
        {"name": "Freshworks Software Pvt Ltd", "industry": "technology"},
        {"name": "InMobi Pvt Ltd", "industry": "technology"},
        {"name": "Capillary Technologies Pvt Ltd", "industry": "technology"},
        {"name": "Mosaic Brands Pvt Ltd", "industry": "retail"},
    ]

    # Synthetic data constants (for other fields)
    INDUSTRIES = [
        "manufacturing", "technology", "healthcare", "finance",
        "retail", "construction", "logistics", "energy",
        "telecommunications", "pharmaceuticals", "education", "automotive",
        "consumer_goods", "ecommerce", "gaming", "food_tech"
    ]

    RATINGS = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C", "D"]

    RISK_LEVELS = ["low", "medium", "high", "critical"]

    PAYMENT_METHODS = ["wire_transfer", "ach", "check", "credit_card", "direct_debit"]

    # Industry normalization map (for future standardization)
    INDUSTRY_MAP = {
        "food_tech": "technology",
        "ecommerce": "retail",
        "b2b_commerce": "retail",
        "gaming": "technology",
        "education": "technology",
        "pharmaceuticals": "healthcare",
    }

    def __init__(
        self,
        kafka_client: Any,
        generation_interval_seconds: float = 5.0,
        customers_per_batch: int = 1,
        invoices_per_batch: int = 2,
        payments_per_batch: int = 1,
        query_agent: Optional[Any] = None,
    ):
        super().__init__(
            agent_name="ScenarioGeneratorAgent",
            agent_version="1.0.0",
            group_id="scenario-generator-group",
            subscribed_topics=[self.TOPIC_COMMANDS],  # Subscribe to commands (producer-only, but needs minimal subscription)
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

        # FIX #1: Reference to QueryAgent for DB-backed customer count check
        self._query_agent = query_agent

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

        # Call BaseAgent.start() to properly initialize system integration
        # This ensures: registration, heartbeat, signal handlers, full lifecycle
        super().start()

        # Add generator-specific thread AFTER base initialization
        self._generator_thread = threading.Thread(
            target=self._generation_loop,
            daemon=True,
            name=f"{self.agent_name}-generator"
        )
        self._generator_thread.start()

        logger.info("ScenarioGeneratorAgent generation loop started")

    def stop(self) -> None:
        """Stop the generator."""
        logger.info("Stopping ScenarioGeneratorAgent")

        self._running = False

        # Wait for generator thread
        if self._generator_thread and self._generator_thread.is_alive():
            self._generator_thread.join(timeout=5)

        # Call BaseAgent.stop() to deregister, stop heartbeat, close connections
        super().stop()

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

    def _get_db_customer_count(self) -> int:
        """Get actual customer count from database via QueryAgent."""
        if not self._query_agent:
            # Fallback to memory count if QueryAgent not available
            return len(self._customers)

        try:
            customers = self._query_agent.get_all_customers()
            count = len(customers) if customers else 0
            logger.debug(f"DB customer count: {count}")
            return count
        except Exception as e:
            logger.warning(f"Failed to get DB customer count: {e}, using memory count")
            return len(self._customers)

    def _get_db_invoice_count(self) -> int:
        """Get actual invoice count from database via QueryAgent (aggregate query)."""
        if not self._query_agent:
            # Fallback to memory count if QueryAgent not available
            return len(self._invoices)

        try:
            # Use a single query to get invoice count directly (avoid N+1)
            # This assumes QueryAgent has access to a count method or we query directly
            # For now, batch the lookup into one operation
            all_customers = self._query_agent.get_all_customers()
            if not all_customers:
                return 0

            # Collect all customer IDs first (1 query)
            customer_ids = [c.get("customer_id") for c in all_customers if c.get("customer_id")]

            # Get total invoices via aggregate (ideally single query)
            # For now, we still loop but at least we batch the customer lists
            total = 0
            for customer_id in customer_ids:
                try:
                    invoices = self._query_agent.get_all_invoices_by_customer(customer_id)
                    if invoices:
                        total += len(invoices)
                except Exception:
                    pass  # Skip if customer lookup fails

            logger.debug(f"DB invoice count: {total}")
            return total
        except Exception as e:
            logger.warning(f"Failed to get DB invoice count: {e}, using memory count")
            return len(self._invoices)

    def _generate_batch(self) -> None:
        """Generate a batch of synthetic events."""
        correlation_id = self.create_correlation_id()

        # Generate customers
        for _ in range(self.customers_per_batch):
            self._generate_customer(correlation_id)

        # FIX #1: Check DB customer count, not memory count!
        # This prevents FK violations: invoice.customer_id → customers table
        db_customer_count = self._get_db_customer_count()
        if db_customer_count > 5:
            for _ in range(self.invoices_per_batch):
                self._generate_invoice(correlation_id)

        # Generate payments (need existing invoices in DB, not just memory)
        db_invoice_count = self._get_db_invoice_count()
        if db_invoice_count > 2:
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
        """Create a new customer with real Indian company."""
        self._customer_counter += 1
        customer_id = f"cust_{self._customer_counter:05d}"

        # Select real Indian company
        company, company_type = self._select_indian_company()
        company_base_name = company["name"]
        industry_raw = company["industry"]

        # Use clean company name with customer_id for uniqueness
        customer_name = company_base_name

        # IMPROVEMENT 2: Generate company ID (e.g., "INFOSYS" from "Infosys Ltd")
        company_id = self._generate_company_id(company_base_name)

        # IMPROVEMENT 3: Normalize industry (e.g., "food_tech" → "technology")
        industry = self._normalize_industry(industry_raw)

        # India-only: country = "IN", currency = "INR"
        country = "IN"
        currency = "INR"

        customer_data = {
            "customer_id": customer_id,
            "customer_name": customer_name,
            "company_id": company_id,  # Added: standardized identifier
            "credit_limit": self._generate_credit_limit(),
            "currency": currency,
            "risk_level": random.choices(
                self.RISK_LEVELS,
                weights=[0.5, 0.3, 0.15, 0.05]  # Weighted toward lower risk
            )[0],
            "industry": industry,
            "country": country,
            "rating": random.choices(
                self.RATINGS,
                weights=[0.05, 0.1, 0.15, 0.25, 0.2, 0.1, 0.08, 0.04, 0.02, 0.01]
            )[0],
            "status": "active",
            "company_type": company_type,  # "listed" or "unlisted"
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

        logger.info(f"Created customer: {customer_id} ({customer_name}, {company_type})")
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
        """Create a new invoice with realistic timestamps and status."""
        self._invoice_counter += 1
        invoice_id = f"inv_{self._invoice_counter:05d}"

        # Pick a random customer
        customer_id = random.choice(list(self._customers.keys()))
        customer = self._customers[customer_id]

        amount = self._generate_invoice_amount(customer["credit_limit"])

        # IMPROVEMENT: Generate invoices from the past (0-120 days ago) for realism
        # Instead of creating all invoices today, spread them across time
        created_at = datetime.utcnow() - timedelta(days=random.randint(0, 120))

        # IMPROVEMENT: Use realistic payment terms (15, 30, 45, or 60 days)
        payment_terms_days = random.choice([15, 30, 45, 60])
        due_date = created_at + timedelta(days=payment_terms_days)

        # IMPROVEMENT: Determine status based on actual due date vs now
        # Invoices past due date have 50% chance of being "overdue", 50% "pending" (not yet collected)
        now = datetime.utcnow()
        if now > due_date:
            # Invoice is past due date
            status = "overdue" if random.random() < 0.5 else "pending"
        else:
            # Invoice not yet due
            status = "pending"

        invoice_data = {
            "invoice_id": invoice_id,
            "customer_id": customer_id,
            "amount": amount,
            "remaining_amount": amount,
            "currency": customer["currency"],
            "due_date": due_date.isoformat(),
            "status": status,
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

        logger.info(f"Created invoice: {invoice_id} for {customer_id} (status={status}, due={due_date.strftime('%Y-%m-%d')})")
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
        """Generate a payment for an existing invoice with realistic behavior."""
        # CRITICAL FIX #5: Select customers first (for fair distribution), then invoices
        available_customers = [
            cid for cid in self._customers.keys()
            if any(inv["status"] in ["pending", "overdue"]
                   for inv in self._invoices.values()
                   if inv["customer_id"] == cid)
        ]

        if not available_customers:
            return ""

        # Select customer first (for fair distribution across all customers)
        customer_id = random.choice(available_customers)

        # Find invoices that can receive payments for this customer
        now = datetime.utcnow()
        payable_invoices = [
            inv_id for inv_id, inv in self._invoices.items()
            if (inv["customer_id"] == customer_id and
                inv["status"] in ["pending", "overdue"] and
                datetime.fromisoformat(inv["created_at"]) < now - timedelta(seconds=2))
        ]

        if not payable_invoices:
            return ""

        invoice_id = random.choice(payable_invoices)
        invoice = self._invoices[invoice_id]

        # Safety check: invoice should exist
        if invoice_id not in self._invoices:
            logger.warning(f"Skipping payment: invoice {invoice_id} not created")
            return ""

        # Parse due_date
        due_date = datetime.fromisoformat(invoice["due_date"])

        # REALISTIC BEHAVIOR: Simulate payment timing patterns
        # 30% → early payment (1-5 days before due)
        # 30% → on-time payment (on due date)
        # 25% → late payment (5-30 days after due)
        # 15% → unpaid (no payment generated)
        behavior_rand = random.random()

        if behavior_rand < 0.30:
            # Early payment: 1-5 days before due
            days_early = random.randint(1, 5)
            payment_date = due_date - timedelta(days=days_early)
            behavior_label = "early"
        elif behavior_rand < 0.60:
            # On-time payment: exactly on due date
            payment_date = due_date
            behavior_label = "on-time"
        elif behavior_rand < 0.85:
            # Late payment: 5-30 days after due
            days_late = random.randint(5, 30)
            payment_date = due_date + timedelta(days=days_late)
            behavior_label = "late"
        else:
            # No payment - leave unpaid
            logger.info(f"Invoice {invoice_id} left unpaid (15% behavior)")
            return ""

        self._payment_counter += 1
        payment_id = f"pay_{self._payment_counter:05d}"

        # Get payable amount (use remaining_amount if partial paid, else full amount)
        payable_amount = invoice.get("remaining_amount", invoice["amount"])

        # REALISTIC PARTIAL PAYMENTS: 30% of payments are partial
        is_partial = random.random() < 0.30

        if is_partial:
            # Partial payment: 30-80% of invoice amount
            partial_ratio = random.uniform(0.3, 0.8)
            amount = round(payable_amount * partial_ratio, 2)
            status = "partial"
            remaining = round(payable_amount - amount, 2)
        else:
            # Full payment
            amount = payable_amount
            status = "completed"
            remaining = 0

        # Use actual payment method
        method = random.choice(self.PAYMENT_METHODS)

        payment_data = {
            "payment_id": payment_id,
            "invoice_id": invoice_id,
            "customer_id": invoice["customer_id"],
            "amount": amount,
            "currency": invoice["currency"],
            "payment_date": payment_date.isoformat(),
            "payment_method": method,
            "status": status,
            "reference": f"{method.upper()[:2]}-{payment_date.strftime('%Y-%m-%d')}-{payment_id}",
            "behavior": behavior_label,  # Track payment behavior for analytics
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

        logger.info(
            f"Created {behavior_label} payment: {payment_id} for invoice {invoice_id} "
            f"(due: {due_date.date()}, paid: {payment_date.date()})"
        )
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
                "due_date": due_date.isoformat(),
                "status": "pending",  # FIX #3: Always "pending", OverdueDetectionAgent will convert
                "created_at": created_at.isoformat(),
                "updated_at": None,
                "line_items": None,
                "notes": None,
            }

            self._invoices[invoice_id] = invoice_data

            # Publish only invoice.created, no manual overdue emission
            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.created",
                entity_id=customer_id,
                payload=invoice_data,
                correlation_id=correlation_id,
            )

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
                "due_date": due_date.isoformat(),
                "status": "pending",  # FIX #3: Always "pending", OverdueDetectionAgent will convert
                "created_at": created_at.isoformat(),
                "updated_at": None,
                "line_items": None,
                "notes": None,
            }

            self._invoices[invoice_id] = invoice_data

            # Publish only invoice.created, no manual overdue emission
            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.created",
                entity_id=customer_id,
                payload=invoice_data,
                correlation_id=correlation_id,
            )

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
                "due_date": due_date.isoformat(),
                "status": "pending",  # FIX #3: Always "pending", OverdueDetectionAgent will convert
                "created_at": created_at.isoformat(),
                "updated_at": None,
                "line_items": None,
                "notes": None,
            }

            self._invoices[invoice_id] = invoice_data

            # Publish only invoice.created, no manual overdue emission
            self.publish_event(
                topic=self.TOPIC_INVOICES,
                event_type="invoice.created",
                entity_id=customer_id,
                payload=invoice_data,
                correlation_id=correlation_id,
            )

            # Generate partial payment (with delay to avoid race condition)
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

    def _select_indian_company(self) -> tuple:
        """Select a real Indian company (listed or unlisted)."""
        category = random.choices(
            ["listed", "unlisted"],
            weights=[0.6, 0.4]
        )[0]

        if category == "listed":
            company = random.choice(self.LISTED_INDIAN_COMPANIES)
        else:
            company = random.choice(self.UNLISTED_INDIAN_COMPANIES)

        return company, category

    def _generate_company_id(self, company_name: str) -> str:
        """Generate standardized company ID from company name.

        Examples:
        - "Infosys Ltd" → "INFOSYS"
        - "Tata Consultancy Services Ltd" → "TATA_CONSULTANCY"
        - "State Bank of India" → "STATE_INDIA" (skips "of", "the")
        - "Ola Electric Mobility Pvt Ltd" → "OLA_ELECTRIC"
        """
        # IMPROVEMENT 2: Remove legal suffixes and filter stop words
        # Remove legal entity markers first
        cleaned = company_name.replace(" Ltd", "").replace(" Pvt", "").replace("Ltd", "").replace("Pvt", "")

        # Split and filter out common stop words ("of", "the", "and")
        words = [w for w in cleaned.split()
                 if w.lower() not in ["of", "the", "and", "at", "&"]]

        # Take first 2 significant words, uppercase, join with underscore
        company_id = "_".join(words[:2]).upper()

        # Clean special characters
        return company_id.replace("'", "").replace(".", "").replace("+", "")

    def _normalize_industry(self, industry: str) -> str:
        """Normalize industry using mapping (for future standardization)."""
        return self.INDUSTRY_MAP.get(industry, industry)

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
        """Get appropriate currency for country (India-only: INR)."""
        if country == "IN":
            return "INR"
        return "INR"  # Default to INR for any unexpected country

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
                "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
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
