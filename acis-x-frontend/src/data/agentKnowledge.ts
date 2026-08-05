export type AgentKnowledge = {
  name: string
  category: 'business' | 'operational' | 'infrastructure'
  title: string
  purpose: string
  workflow: string[]
  formulas: { title: string; latex: string; description: string }[]
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
        latex: '\\text{Due Date} = \\text{Invoice Date} + \\text{Payment Terms (Days)}',
        description: 'Calculates due date based on customer credit terms (NET 15, NET 30, NET 60).',
      },
      {
        title: 'Random Payment Amount Distribution',
        latex: '\\text{Payment Amount} = U(0.2, 1.0) \\times \\text{Invoice Total Amount}',
        description: 'Simulates partial or full invoice payments using uniform random scaling.',
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
        latex: '\\text{Outstanding} = \\sum_{i \\in \\text{Unpaid}} \\text{Invoice Total}_i - \\sum_{j \\in \\text{Payments}} \\text{Payment Amount}_j',
        description: 'Sum of all unpaid invoice amounts minus partial payments received.',
      },
      {
        title: 'Average Payment Delay (Days)',
        latex: '\\text{Avg Delay} = \\frac{1}{N} \\sum_{k=1}^N \\max(0, \\text{Payment Date}_k - \\text{Due Date}_k)',
        description: 'Mean number of days payments were received past the due date across past N settled invoices.',
      },
      {
        title: 'On-Time Payment Ratio',
        latex: 'R_{\\text{on\\_time}} = \\frac{\\text{Number of On-Time Payments}}{\\text{Total Settled Payments}}',
        description: 'Proportion of historical payments received on or before the due date.',
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
        latex: 'U_{\\text{credit}} = \\frac{\\text{Total Outstanding Balance}}{\\text{Assigned Credit Limit}}',
        description: 'Ratio of active AR balance relative to allocated credit ceiling.',
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
    purpose: 'Predicts the probability of payment delay and estimated delay days for open invoices.',
    workflow: [
      'Consumes invoice.created and customer.metrics.updated.',
      'Applies ML regression & heuristic models on historical delay, invoice size, and customer tier.',
      'Emits payment.risk.predicted to acis.predictions.',
    ],
    formulas: [
      {
        title: 'Logistic Payment Delay Probability',
        latex: 'P(\\text{Delay}) = \\frac{1}{1 + e^{-(\\beta_0 + \\beta_1 \\cdot \\text{AvgDelay} + \\beta_2 \\cdot \\text{Utilization} + \\beta_3 \\cdot \\text{OverdueRatio})}}',
        description: 'Sigmoid activation calculating probability of payment default or delay past 30 days.',
      },
      {
        title: 'Top Risk Driver Identification',
        latex: '\\text{Driver}_k = \\arg\\max_i |\\beta_i \\cdot X_i|',
        description: 'Identifies which financial factor contributes most heavily to the predicted delay.',
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
        latex: '\\text{Combined Risk} = 0.60 \\times \\text{Financial Risk} + 0.40 \\times \\text{Litigation Risk}',
        description: 'Weighted combination of internal payment performance (60%) and external legal/court filings (40%).',
      },
      {
        title: 'Financial Risk Sub-score',
        latex: '\\text{Financial Risk} = 0.40 \\cdot R_{\\text{delay}} + 0.35 \\cdot U_{\\text{credit}} + 0.25 \\cdot (1 - R_{\\text{on\\_time}})',
        description: 'Decomposition of financial risk into delay probability, credit utilization, and late payment frequency.',
      },
      {
        title: 'Confidence Adjustment',
        latex: 'S_{\\text{refined}} = S_{\\text{base}} \\times (0.7 + 0.3 \\times C_{\\text{data}})',
        description: 'Scales raw risk score by data completeness confidence score C_data.',
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
        title: 'Credit Hold Condition',
        latex: '\\text{Credit Hold} = (S_{\\text{risk}} > 0.85) \\lor (U_{\\text{credit}} > 1.00) \\lor (\\text{Max Overdue Days} > 90)',
        description: 'Boolean rule triggering automated credit hold when risk or overdue metrics cross thresholds.',
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
        title: 'Collections Escalation Stage Matrix',
        latex: '\\text{Stage} = \\begin{cases} \\text{Reminder} & 1 \\le D_{\\text{overdue}} \\le 15 \\\\ \\text{Formal Dunning} & 16 \\le D_{\\text{overdue}} \\le 30 \\\\ \\text{Executive Notice} & 31 \\le D_{\\text{overdue}} \\le 60 \\\\ \\text{Legal Escalation} & D_{\\text{overdue}} > 60 \\end{cases}',
        description: 'Categorizes collection action intensity based on days overdue and risk stage.',
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
        title: 'Overdue Condition',
        latex: '\\text{Is Overdue} = (T_{\\text{current}} > T_{\\text{due}}) \\land (\\text{Status} \\ne \\text{\'paid\'})',
        description: 'Identifies open invoices that have passed their payment due date.',
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
        title: 'Litigation Severity Score',
        latex: 'S_{\\text{litigation}} = 0.50 \\cdot N_{\\text{pending\\_cases}} + 0.30 \\cdot \\frac{\\text{Claim Amount}}{\\text{Annual Revenue}} + 0.20 \\cdot S_{\\text{adverse\\_news}}',
        description: 'Quantifies external legal exposure based on active court cases and claim magnitude.',
      },
    ],
    consumedEvents: ['customer.metrics.updated'],
    emittedEvents: ['external.data.updated'],
    databaseTables: ['external_intelligence_cache'],
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
        title: 'Simulated Time Step',
        latex: 'T_{k+1} = T_k + \\Delta t_{\\text{step}}',
        description: 'Advances internal clock by simulated step increment on each real-world tick interval.',
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
        title: 'WAL SQLite URI Lock Avoidance',
        latex: '\\text{URI} = \\text{file:/data/acis.db?nolock=1\\&check\\_same\\_thread=False}',
        description: 'Uses special URI parameters to prevent WSL2/Docker volume SQLite locking bugs.',
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
        title: 'LRU Cache Hit Ratio',
        latex: 'H_{\\text{cache}} = \\frac{\\text{Cache Hits}}{\\text{Cache Hits} + \\text{Cache Misses}}',
        description: 'Tracks in-memory caching efficiency.',
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
    formulas: [],
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
        title: 'Lag Anomaly Z-Score',
        latex: 'Z_{\\text{lag}} = \\frac{L_{\\text{current}} - \\mu_{\\text{lag}}}{\\sigma_{\\text{lag}}}',
        description: 'Measures consumer lag deviation relative to historical baseline.',
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
        title: 'Exponential Backoff Restart Delay',
        latex: 'T_{\\text{backoff}} = \\min(T_{\\text{max}}, T_{\\text{base}} \\times 2^{R_{\\text{count}}})',
        description: 'Prevents restart loops by doubling wait time between consecutive failures.',
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
        title: 'DLQ Exception Frequency Ratio',
        latex: 'E_{\\text{rate}} = \\frac{N_{\\text{DLQ Events}}}{N_{\\text{Total Events Published}}}',
        description: 'Ratio of failed events relative to total bus traffic.',
      },
    ],
    consumedEvents: ['acis.dlq'],
    emittedEvents: ['dlq.alert.emitted'],
    databaseTables: [],
  },
}
