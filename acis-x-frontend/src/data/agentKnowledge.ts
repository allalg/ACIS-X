export type AgentKnowledge = {
  name: string
  category: 'business' | 'operational' | 'infrastructure'
  title: string
  purpose: string
  workflow: string[]
  formulas: { title: string; plainEnglishFormula: string; description: string; modelType?: string; weightRationale?: string }[]
  consumedEvents: string[]
  emittedEvents: string[]
  databaseTables: string[]
}

export const AGENT_KNOWLEDGE_BASE: Record<string, AgentKnowledge> = {
  ScenarioGeneratorAgent: {
    name: 'ScenarioGeneratorAgent',
    category: 'business',
    title: 'Synthetic B2B Data & Scenario Generator',
    purpose: 'Simulates realistic B2B enterprise activity by generating synthetic customers, invoices, and payments at configurable intervals.',
    workflow: [
      'Periodically wakes up on generation_interval (default 20s).',
      'Generates synthetic enterprise customer profiles with GSTIN, company names, and credit limits.',
      'Emits customer.created, invoice.created, and payment.created events to acis.customers, acis.invoices, and acis.payments topics.',
      'Supports instant pause/resume via scenario.pause control events on acis.control.',
    ],
    formulas: [
      {
        title: 'Invoice Due Date Calculation',
        plainEnglishFormula: 'Invoice Due Date = Invoice Creation Date + Payment Terms Period (e.g. 15, 30, or 60 Days)',
        description: 'Determines the exact calendar date when an invoice is expected to be settled based on agreed customer terms.',
        modelType: 'DETERMINISTIC RULE',
        weightRationale: 'Uses agreed credit contract terms (NET 15, NET 30, NET 60) standard in B2B enterprise sales.',
      },
      {
        title: 'Random Payment Amount Distribution',
        plainEnglishFormula: 'Payment Amount = Random Percentage (between 20% and 100%) × Total Invoice Amount',
        description: 'Simulates partial or full B2B customer payments across generated invoice batches.',
        modelType: 'PROBABILISTIC STOCHASTIC SAMPLING',
        weightRationale: 'Uniform random sampling between 20% and 100% models realistic enterprise partial payment behaviors.',
      },
    ],
    consumedEvents: ['acis.control (scenario.pause, scenario.resume)'],
    emittedEvents: ['customer.created', 'invoice.created', 'payment.created'],
    databaseTables: ['customers', 'invoices', 'payments'],
  },

  CustomerStateAgent: {
    name: 'CustomerStateAgent',
    category: 'business',
    title: 'Customer Ledger & State Aggregator',
    purpose: 'Maintains running transactional state for each customer, calculating outstanding balances, average payment delays, and on-time ratios.',
    workflow: [
      'Consumes customer.created, invoice.created, and payment.created events.',
      'Recalculates total outstanding AR, total overdue AR, and payment delay history per customer.',
      'Emits customer.metrics.updated to acis.metrics for downstream risk and credit scoring.',
    ],
    formulas: [
      {
        title: 'Outstanding Balance Aggregation',
        plainEnglishFormula: 'Total Outstanding Balance = (Sum of All Issued Invoice Amounts) - (Sum of All Payments Received)',
        description: 'Net unpaid balance remaining across all historical and active customer invoices.',
        modelType: 'DETERMINISTIC ACCOUNTING AGGREGATION',
        weightRationale: 'Exact double-entry accounting identity to ensure financial ledger accuracy.',
      },
      {
        title: 'Average Payment Delay (Days)',
        plainEnglishFormula: 'Average Payment Delay = Total Delay Days across Settled Invoices / Total Number of Settled Invoices',
        description: 'Average number of days a customer takes past the due date to complete their payments.',
        modelType: 'HISTORICAL ROLLING AVERAGE',
        weightRationale: 'Rolling mean across past settled invoices provides a stable baseline for customer payment punctuality.',
      },
      {
        title: 'On-Time Payment Ratio',
        plainEnglishFormula: 'On-Time Payment Percentage = (Number of Invoices Paid On or Before Due Date / Total Settled Invoices) × 100%',
        description: 'Percentage of past invoices settled without crossing the due date threshold.',
        modelType: 'HISTORICAL RATIO',
        weightRationale: 'Direct ratio quantifying customer reliability and adherence to credit terms.',
      },
    ],
    consumedEvents: ['customer.created', 'invoice.created', 'payment.created'],
    emittedEvents: ['customer.metrics.updated'],
    databaseTables: ['customer_metrics', 'customers', 'invoices', 'payments'],
  },

  CustomerProfileAgent: {
    name: 'CustomerProfileAgent',
    category: 'business',
    title: 'Customer Risk Profile & Credit Limit Engine',
    purpose: 'Aggregates multi-dimensional risk scores and manages assigned credit limits for enterprise customers.',
    workflow: [
      'Listens for customer.metrics.updated and risk.scored events.',
      'Evaluates credit limit utilization and financial health tiering.',
      'Emits customer.profile.updated to acis.customers.',
    ],
    formulas: [
      {
        title: 'Credit Utilization Ratio',
        plainEnglishFormula: 'Credit Utilization Percentage = (Current Outstanding Balance / Total Approved Credit Limit) × 100%',
        description: 'Measures how much of the customer\'s authorized credit line is currently tied up in unpaid invoices.',
        modelType: 'DETERMINISTIC CAPACITY RATIO',
        weightRationale: 'Standard treasury metric to detect customers operating near or beyond their pre-approved credit ceiling.',
      },
    ],
    consumedEvents: ['customer.metrics.updated', 'risk.scored'],
    emittedEvents: ['customer.profile.updated'],
    databaseTables: ['customer_risk_profile', 'customers'],
  },

  PaymentPredictionAgent: {
    name: 'PaymentPredictionAgent',
    category: 'business',
    title: 'Payment Delay ML Prediction Engine',
    purpose: 'Predicts the probability of payment delay and estimated delay days for open invoices using machine learning.',
    workflow: [
      'Consumes invoice.created and customer.metrics.updated.',
      'Applies ML regression & SHAP (SHapley Additive exPlanations) models on historical delay, invoice size, and customer tier.',
      'Emits payment.risk.predicted to acis.predictions.',
    ],
    formulas: [
      {
        title: 'Payment Delay Probability Score',
        plainEnglishFormula: 'Delay Probability = ML Model Score combining (Customer\'s Historical Avg Delay + Credit Utilization + Overdue Percentage + Invoice Amount)',
        description: 'Calculates the likelihood (0% to 100%) that an open invoice will default or be delayed past 30 days.',
        modelType: 'MACHINE LEARNING (LOGISTIC REGRESSION & XGBOOST)',
        weightRationale: 'Uses dynamic machine learning weights trained on historical payment settlement data rather than static rules, dynamically adapting to changing customer payment behaviors.',
      },
      {
        title: 'Primary Risk Driver Identification (SHAP)',
        plainEnglishFormula: 'Top Risk Driver = SHAP Value Feature Attribution identifying the single factor contributing most heavily to predicted delay',
        description: 'Pinpoints the exact reason (e.g. High Credit Line Usage or Historical Payment Delays) why a customer or invoice received a high risk classification.',
        modelType: 'MACHINE LEARNING EXPLAINABILITY (SHAP VALUES)',
        weightRationale: 'SHAP (SHapley Additive exPlanations) provides mathematical explainability for regulatory compliance, decomposing the ML prediction into individual feature contribution weights.',
      },
    ],
    consumedEvents: ['invoice.created', 'customer.metrics.updated'],
    emittedEvents: ['payment.risk.predicted'],
    databaseTables: ['risk_explanations'],
  },

  RiskScoringAgent: {
    name: 'RiskScoringAgent',
    category: 'business',
    title: 'Multi-Factor Comprehensive Risk Engine',
    purpose: 'Combines internal financial risk metrics with external litigation intelligence to compute a composite risk score (0.0 to 1.0) and stage classification.',
    workflow: [
      'Consumes payment.risk.predicted and external.data.updated events.',
      'Calculates 60% Financial Risk + 40% Litigation Risk score.',
      'Maps scores into Low [0-0.3], Medium (0.3-0.7], High (0.7-0.95], and Critical (>0.95] stages.',
      'Emits risk.scored to acis.risk.',
    ],
    formulas: [
      {
        title: 'Composite Risk Weighting',
        plainEnglishFormula: 'Combined Risk Score = (60% × Internal Financial Risk Score) + (40% × External Legal & Litigation Risk Score)',
        description: 'Weighted combination combining internal invoice payment performance with external court cases and legal filings.',
        modelType: 'STATIC WEIGHTED MODEL',
        weightRationale: 'Internal financial performance carries 60% weight as the primary direct measure of working capital risk, while external litigation carries 40% weight to serve as an early warning hedge against potential corporate insolvency or legal disputes.',
      },
      {
        title: 'Financial Risk Breakdown',
        plainEnglishFormula: 'Internal Financial Risk = (40% × Predicted Payment Delay) + (35% × Credit Line Utilization) + (25% × Late Payment Frequency)',
        description: 'Decomposes internal financial risk into payment delay likelihood, credit line usage, and historical late payments.',
        modelType: 'STATIC WEIGHTED MODEL',
        weightRationale: 'Predicted payment delay (40%) is the single strongest indicator of default risk, credit line utilization (35%) measures financial strain, and late payment frequency (25%) reflects historical reliability.',
      },
      {
        title: 'Data Completeness Confidence Adjustment',
        plainEnglishFormula: 'Final Adjusted Risk Score = Base Risk Score × [ 70% + (30% × Data Completeness Factor) ]',
        description: 'Adjusts the final risk score based on how complete the available customer data is.',
        modelType: 'STATIC CONFIDENCE BLENDING',
        weightRationale: 'Guarantees at least 70% baseline confidence while scaling the remaining 30% dynamically based on data availability, preventing incomplete data from causing false low-risk scores.',
      },
    ],
    consumedEvents: ['payment.risk.predicted', 'external.data.updated'],
    emittedEvents: ['risk.scored'],
    databaseTables: ['customer_risk_profile', 'risk_explanations'],
  },

  CreditPolicyAgent: {
    name: 'CreditPolicyAgent',
    category: 'business',
    title: 'Credit Policy & Order Hold Enforcer',
    purpose: 'Enforces credit policy rules, placing credit holds on customers exceeding risk thresholds or credit limits.',
    workflow: [
      'Consumes risk.scored and customer.profile.updated events.',
      'Checks for policy breaches (e.g. Risk Score > 0.85 or Credit Utilization > 100%).',
      'Emits credit.hold.applied or credit.hold.released to acis.credit.',
    ],
    formulas: [
      {
        title: 'Automated Credit Hold Rule',
        plainEnglishFormula: 'Trigger Credit Hold = IF (Risk Score exceeds 85%) OR (Credit Utilization exceeds 100%) OR (Overdue Days exceed 90 Days)',
        description: 'Automated policy rule that places customer orders on hold if financial risk or overdue balances cross safety thresholds.',
        modelType: 'STATIC BUSINESS POLICY BREAKERS',
        weightRationale: 'Policy thresholds (85% Risk, 100% Utilization, 90 Days Overdue) are set based on enterprise risk management standards to halt new credit sales before bad debt write-offs occur.',
      },
    ],
    consumedEvents: ['risk.scored', 'customer.profile.updated'],
    emittedEvents: ['credit.hold.applied', 'credit.hold.released'],
    databaseTables: ['customer_risk_profile'],
  },

  CollectionsAgent: {
    name: 'CollectionsAgent',
    category: 'business',
    title: 'Automated Collections Strategy & Escalation Engine',
    purpose: 'Orchestrates automated dunning letter generation, email/SMS outreach, and legal litigation escalation based on invoice overdue stages.',
    workflow: [
      'Consumes risk.scored and invoice.overdue events.',
      'Determines appropriate dunning action (Friendly Reminder -> Formal Notice -> Executive Escalation -> Legal Notice).',
      'Emits collections.action.triggered to acis.collections and writes to collections_log table.',
    ],
    formulas: [
      {
        title: 'Collections Escalation Strategy Schedule',
        plainEnglishFormula: '• Overdue 1 to 15 Days: Friendly Payment Reminder (Email/SMS)\n• Overdue 16 to 30 Days: Formal Dunning Letter & Invoice Statement\n• Overdue 31 to 60 Days: Executive Warning & Credit Limit Hold\n• Overdue 60+ Days: Final Legal Escalation & Formal Litigation Notice',
        description: 'Escalates outreach intensity step-by-step based on how long an invoice remains past due.',
        modelType: 'DETERMINISTIC DUNNING MATRIX',
        weightRationale: 'Industry-standard dunning timeline balances customer relationship preservation during early delays (1-15 days) with aggressive legal enforcement during severe delays (60+ days).',
      },
    ],
    consumedEvents: ['risk.scored', 'invoice.overdue'],
    emittedEvents: ['collections.action.triggered'],
    databaseTables: ['collections_log'],
  },

  OverdueDetectionAgent: {
    name: 'OverdueDetectionAgent',
    category: 'operational',
    title: 'Invoice Overdue Detection Agent',
    purpose: 'Scans database invoices on simulated time ticks and detects when open invoices cross their payment due dates.',
    workflow: [
      'Consumes time.tick events on acis.time.',
      'Queries database for unpaid invoices where Current Time > Due Date.',
      'Emits invoice.overdue events to acis.invoices.',
    ],
    formulas: [
      {
        title: 'Overdue Detection Trigger Rule',
        plainEnglishFormula: 'Mark Invoice Overdue = IF (Current Simulated Date IS LATER THAN Invoice Due Date) AND (Status IS NOT \'Paid\')',
        description: 'Identifies active invoices that have passed their agreed payment deadline.',
        modelType: 'DETERMINISTIC TEMPORAL RULE',
        weightRationale: 'Binary calendar evaluation ensuring instant trigger as soon as an invoice passes its contractually agreed payment date.',
      },
    ],
    consumedEvents: ['time.tick'],
    emittedEvents: ['invoice.overdue'],
    databaseTables: ['invoices'],
  },

  ExternalDataAgent: {
    name: 'ExternalDataAgent',
    category: 'operational',
    title: 'External Legal & Litigation Intelligence Agent',
    purpose: 'Retrieves external court litigation data, insolvency registries, and news sentiment for enterprise customers.',
    workflow: [
      'Consumes customer.metrics.updated events.',
      'Searches external legal databases and news scrapers for GSTIN / corporate legal cases.',
      'Emits external.data.updated to acis.intelligence.',
    ],
    formulas: [
      {
        title: 'External Legal Exposure Score',
        plainEnglishFormula: 'Litigation Exposure Score = (50% × Active Court Cases Count) + (30% × Legal Claim Amount vs Revenue Ratio) + (20% × Adverse News Sentiment)',
        description: 'Quantifies external legal and court risk based on public filings, lawsuit claim sizes, and press coverage.',
        modelType: 'STATIC WEIGHTED MODEL',
        weightRationale: 'Active court case count (50%) is the strongest predictor of impending legal disruption, claim ratio (30%) reflects financial severity relative to customer size, and media sentiment (20%) acts as a early public signal.',
      },
    ],
    consumedEvents: ['customer.metrics.updated'],
    emittedEvents: ['external.data.updated'],
    databaseTables: ['external_litigation', 'external_financials'],
  },

  TimeTickAgent: {
    name: 'TimeTickAgent',
    category: 'infrastructure',
    title: 'Simulated Time Clock Generator',
    purpose: 'Drives simulated time progression across the platform by broadcasting time.tick events at regular intervals.',
    workflow: [
      'Runs a 5-second tick loop publishing current simulated ISO timestamp.',
      'Emits time.tick to acis.time.',
      'Supports time.pause and time.resume for simulation freezing.',
    ],
    formulas: [
      {
        title: 'Clock Step Increment',
        plainEnglishFormula: 'Next Simulated Date = Current Simulated Date + 1 Simulated Time Step',
        description: 'Advances the internal simulated calendar clock on each real-world timer tick interval.',
        modelType: 'DETERMINISTIC CLOCK ACCELERATOR',
        weightRationale: 'Fixed time step allows real-time acceleration of multi-month B2B credit cycles into manageable simulation windows.',
      },
    ],
    consumedEvents: ['acis.control (time.pause, time.resume)'],
    emittedEvents: ['time.tick'],
    databaseTables: [],
  },

  DBAgent: {
    name: 'DBAgent',
    category: 'infrastructure',
    title: 'SQLite Relational Persistence Agent',
    purpose: 'Persists event-stream state mutations into SQLite database tables (customers, invoices, payments, risk profiles, logs).',
    workflow: [
      'Subscribes to all domain topics (acis.customers, acis.invoices, acis.payments, acis.risk, acis.collections).',
      'Executes idempotent SQL UPSERT commands into acis.db.',
      'Handles WAL journal locking and transaction safety across containers.',
    ],
    formulas: [
      {
        title: 'Database Transaction Lock Prevention',
        plainEnglishFormula: 'Database Connection = file:/data/acis.db?nolock=1 (Disables OS-level file locking for WSL2/Docker volume compatibility)',
        description: 'Ensures database writes execute cleanly across concurrent Docker containers without file locking errors.',
        modelType: 'INFRASTRUCTURE CONFIGURATION RULE',
        weightRationale: 'Disabling OS file locking prevents WSL2 virtual filesystem deadlocks during high concurrent database writes.',
      },
    ],
    consumedEvents: ['customer.*', 'invoice.*', 'payment.*', 'risk.*', 'collections.*'],
    emittedEvents: [],
    databaseTables: ['customers', 'invoices', 'payments', 'customer_metrics', 'customer_risk_profile', 'collections_log', 'risk_explanations'],
  },

  MemoryAgent: {
    name: 'MemoryAgent',
    category: 'infrastructure',
    title: 'In-Memory Context & Cache Agent',
    purpose: 'Maintains high-speed in-memory state caches for fast low-latency query access by agents.',
    workflow: [
      'Caches customer metrics and active invoice states in RAM.',
      'Reduces SQLite disk I/O bottleneck during high event volume scenarios.',
    ],
    formulas: [
      {
        title: 'Memory Cache Hit Rate',
        plainEnglishFormula: 'Cache Efficiency Percentage = (Requests Served from RAM / Total Data Requests Received) × 100%',
        description: 'Measures how efficiently data requests are answered directly from high-speed memory without touching the disk.',
        modelType: 'INFRASTRUCTURE METRIC RATIO',
        weightRationale: 'Monitors memory efficiency to ensure agent query latency stays below 5 milliseconds.',
      },
    ],
    consumedEvents: ['customer.*', 'invoice.*', 'payment.*'],
    emittedEvents: [],
    databaseTables: [],
  },

  QueryAgent: {
    name: 'QueryAgent',
    category: 'infrastructure',
    title: 'Read-Model Query & Search Client',
    purpose: 'Provides decoupled read-model REST/gRPC endpoints for dashboard views and BFF aggregation.',
    workflow: [
      'Queries acis.db and MemoryAgent cache.',
      'Serves REST queries for dashboard overview, customer ledgers, and metrics.',
    ],
    formulas: [
      {
        title: 'Read-Model Response Speed',
        plainEnglishFormula: 'Average API Response Time = Total Elapsed Time for All Data Queries / Total Number of Requests Handled',
        description: 'Tracks the speed and responsiveness of backend data endpoints serving the user interface.',
        modelType: 'INFRASTRUCTURE LATENCY METRIC',
        weightRationale: 'Maintains low API latency (<20ms) for real-time frontend dashboard rendering.',
      },
    ],
    consumedEvents: [],
    emittedEvents: [],
    databaseTables: ['customers', 'invoices', 'payments', 'customer_metrics', 'customer_risk_profile'],
  },

  MonitoringAgent: {
    name: 'MonitoringAgent',
    category: 'infrastructure',
    title: 'System Health & Anomaly Detector',
    purpose: 'Monitors agent heartbeat health, CPU/Memory consumption, and consumer lag across Kafka consumer groups.',
    workflow: [
      'Consumes agent.heartbeat and agent.metrics.updated from acis.agent.health.',
      'Calculates Z-score anomaly metrics for memory usage and consumer lag.',
      'Triggers agent.restart.requested on health failures.',
    ],
    formulas: [
      {
        title: 'Consumer Lag Anomaly Z-Score',
        plainEnglishFormula: 'Lag Anomaly Indicator (Z-Score) = (Current Message Backlog - Average Historical Backlog) / Standard Backlog Deviation',
        description: 'Detects if an agent is falling behind on processing messages compared to its normal baseline.',
        modelType: 'STATISTICAL Z-SCORE ANOMALY MODEL',
        weightRationale: 'Z-score thresholding (> 3.0 standard deviations) provides dynamic anomaly detection without requiring hardcoded static lag limits across different workloads.',
      },
    ],
    consumedEvents: ['agent.heartbeat', 'agent.metrics.updated'],
    emittedEvents: ['agent.restart.requested', 'agent.health.critical'],
    databaseTables: [],
  },

  SelfHealingAgent: {
    name: 'SelfHealingAgent',
    category: 'infrastructure',
    title: 'Autonomous Self-Healing & Reboot Orchestrator',
    purpose: 'Orchestrates automatic agent process restarts and fault recovery when crash or lag anomalies are detected.',
    workflow: [
      'Listens for agent.restart.requested from MonitoringAgent or BFF manual fault injection.',
      'Issues restart command to agent container runtime.',
      'Tracks agent restart counts and recovery execution sequences.',
    ],
    formulas: [
      {
        title: 'Recovery Wait Time Between Restarts',
        plainEnglishFormula: 'Restart Delay = Wait 2 Seconds × (2 ^ Number of Consecutive Restarts) [Capped at 300 Seconds Max]',
        description: 'Uses exponential backoff waiting intervals between consecutive restart attempts to prevent crash loops.',
        modelType: 'EXPONENTIAL BACKOFF ALGORITHM',
        weightRationale: 'Exponential scaling (2s, 4s, 8s, 16s...) prevents continuous tight-loop restarting when external dependencies (like Kafka or SQLite) are temporarily unavailable.',
      },
    ],
    consumedEvents: ['agent.restart.requested', 'acis.control (agent.reboot)'],
    emittedEvents: ['agent.rebooting', 'agent.rebooted'],
    databaseTables: [],
  },

  DLQMonitorAgent: {
    name: 'DLQMonitorAgent',
    category: 'infrastructure',
    title: 'Dead-Letter Queue (DLQ) Exception Analyzer',
    purpose: 'Inspects failed or schema-corrupted events routed to acis.dlq, analyzing failure root causes.',
    workflow: [
      'Consumes events from acis.dlq.',
      'Categorizes failure types (Validation Error, Deserialization Error, Stale Event).',
      'Alerts monitoring and logging dashboards.',
    ],
    formulas: [
      {
        title: 'Dead-Letter Failure Percentage',
        plainEnglishFormula: 'System Failure Rate = (Total Failed Dead-Letter Events / Total Bus Event Traffic) × 100%',
        description: 'Calculates the percentage of total event messages that failed validation or processing.',
        modelType: 'STATISTICAL ERROR RATIO',
        weightRationale: 'Monitors system health to alert operators if event validation failures exceed 1% of total traffic.',
      },
    ],
    consumedEvents: ['acis.dlq'],
    emittedEvents: ['dlq.alert.emitted'],
    databaseTables: [],
  },
}
