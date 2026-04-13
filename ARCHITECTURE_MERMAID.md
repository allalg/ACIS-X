# ACIS-X Architecture (Mermaid)

## Compressed System Architecture

```mermaid
graph TD
    SG["🎯 ScenarioGeneratorAgent"]
    CSA["📈 CustomerStateAgent"]
    EDA["📊 ExternalDataAgent"]
    ESA["📰 ExternalScrapingAgent"]
    AA["🔀 AggregatorAgent"]
    CPA["👥 CustomerProfileAgent"]
    PPA["🤖 PaymentPredictionAgent"]
    RSA["⚠️ RiskScoringAgent"]
    ODA["📅 OverdueDetectionAgent"]
    CPA_POL["📋 CreditPolicyAgent"]
    CA["📞 CollectionsAgent"]
    DBA["💾 DBAgent"]
    QA["🔍 QueryAgent"]
    CACHE["⚡ MemoryAgent"]
    TTA["⏰ TimeTickAgent"]
    
    KAFKA["📨 Kafka Broker"]
    SQLITE["🗄️ SQLite DB"]
    
    SG --> KAFKA
    CSA --> KAFKA
    
    KAFKA --> EDA
    KAFKA --> ESA
    
    EDA --> AA
    ESA --> AA
    
    AA --> KAFKA
    AA --> DBA
    
    KAFKA --> CPA
    CPA --> KAFKA
    
    KAFKA --> PPA
    PPA --> KAFKA
    
    KAFKA --> RSA
    RSA --> KAFKA
    
    TTA --> KAFKA
    KAFKA --> ODA
    ODA --> KAFKA
    
    KAFKA --> CPA_POL
    CPA_POL --> CA
    
    DBA --> SQLITE
    QA --> SQLITE
    CACHE --> SQLITE
    
    EDA -.-> QA
    ESA -.-> QA
    CPA -.-> QA
    
    style KAFKA fill:#ff6b6b,stroke:#cc0000,stroke-width:2px
    style SQLITE fill:#51cf66,stroke:#00cc00,stroke-width:2px
    style DBA fill:#339af0
    style QA fill:#ffd43b
    style CACHE fill:#ff922b
```

---

## Complete Data Flow (Mermaid)

```mermaid
graph TD
    SG["1️⃣ ScenarioGeneratorAgent<br/>Publish: customer_created<br/>invoice_created, payment_created"]
    
    CSA["2️⃣ CustomerStateAgent<br/>Read: customer, invoice, payment<br/>Calculate: AR, aging<br/>Publish: metrics_calculated"]
    
    EDA["3️⃣ ExternalDataAgent<br/>Read: metrics_calculated<br/>Query: Screener.in API<br/>Publish: financial_risk"]
    
    ESA["3️⃣ ExternalScrapingAgent<br/>Read: metrics_calculated<br/>Query: Google News API<br/>Publish: litigation_risk"]
    
    AA["5️⃣ AggregatorAgent<br/>Fuse: financial + litigation<br/>Dedup: change tracking<br/>Publish: aggregated_risk"]
    
    DBA1["6️⃣ DBAgent - Store<br/>Insert: customer_risk_profile<br/>financial_data, litigation_data<br/>Cache: 90 days"]
    
    CPA["7️⃣ CustomerProfileAgent<br/>Read: aggregated_risk<br/>Match: customer_id to name<br/>Publish: profile_updated"]
    
    PPA["8️⃣ PaymentPredictionAgent<br/>Read: profile_updated<br/>Predict: default_risk<br/>Publish: prediction"]
    
    RSA["9️⃣ RiskScoringAgent<br/>Combine: all risk signals<br/>Final Score: 0.0 - 1.0<br/>Publish: risk_scored"]
    
    TTA["⏰ TimeTickAgent<br/>Tick every 5 seconds<br/>Publish: time_tick"]
    
    ODA["🔟 OverdueDetectionAgent<br/>On time_tick: check aging<br/>Publish: overdue_alert"]
    
    CPOL["1️⃣1️⃣ CreditPolicyAgent<br/>Read: risk_scored, overdue<br/>Apply: credit policies<br/>Route: to collections"]
    
    CA["1️⃣2️⃣ CollectionsAgent<br/>Take: action on high-risk<br/>Publish: collections_action<br/>Store: to database"]
    
    DB["💾 SQLite Database<br/>customers | invoices | payments<br/>customer_risk_profile<br/>financial_data | litigation_data<br/>collections_actions"]
    
    SG --> CSA
    CSA --> EDA
    CSA --> ESA
    
    EDA --> AA
    ESA --> AA
    
    AA --> DBA1
    DBA1 --> DB
    
    AA --> CPA
    CPA --> PPA
    PPA --> RSA
    
    TTA --> ODA
    ODA --> CPOL
    
    RSA --> CPOL
    CPOL --> CA
    
    CA --> DB
    
    style SG fill:#e3f2fd
    style CSA fill:#e3f2fd
    style EDA fill:#fff3e0
    style ESA fill:#fff3e0
    style AA fill:#fff3e0
    style DBA1 fill:#f3e5f5
    style CPA fill:#f3e5f5
    style PPA fill:#f3e5f5
    style RSA fill:#f3e5f5
    style TTA fill:#e8f5e9
    style ODA fill:#e8f5e9
    style CPOL fill:#e8f5e9
    style CA fill:#e8f5e9
    style DB fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

---

## Compact Topic Flow

```mermaid
graph LR
    SG["ScenarioGen<br/>CustomerState"]
    
    EDA["ExternalData<br/>ExternalScrape<br/>Aggregator"]
    
    CPA["Profile Builder<br/>Predictions<br/>Risk Scorer"]
    
    ODA["Overdue Det<br/>Credit Policy<br/>Collections"]
    
    DB["🗄️ SQLite<br/>+ Cache"]
    
    SG -->|metrics| EDA
    EDA -->|aggregated_risk| CPA
    CPA -->|risk_scored| ODA
    
    EDA --> DB
    CPA --> DB
    ODA --> DB
    
    DB -->|QueryAgent| EDA
    DB -->|QueryAgent| CPA
    
    style SG fill:#e1f5ff
    style EDA fill:#fff3e0
    style CPA fill:#f3e5f5
    style ODA fill:#e8f5e9
    style DB fill:#fce4ec
```

---

## Agent Dependency Matrix

```mermaid
graph TB
    SG["ScenarioGeneratorAgent"]
    CSA["CustomerStateAgent"]
    EDA["ExternalDataAgent"]
    ESA["ExternalScrapingAgent"]
    AA["AggregatorAgent"]
    CPA["CustomerProfileAgent"]
    PPA["PaymentPredictionAgent"]
    RSA["RiskScoringAgent"]
    ODA["OverdueDetectionAgent"]
    CPOL["CreditPolicyAgent"]
    CA["CollectionsAgent"]
    
    TTA["TimeTickAgent"]
    QA["QueryAgent"]
    DBA["DBAgent"]
    
    SG --> CSA
    CSA --> EDA
    CSA --> ESA
    
    EDA --> AA
    ESA --> AA
    
    AA --> CPA
    AA --> RSA
    
    CPA --> PPA
    PPA --> RSA
    
    RSA --> CPOL
    
    TTA --> ODA
    ODA --> CPOL
    
    CPOL --> CA
    
    SG --> DBA
    CSA --> DBA
    EDA --> DBA
    ESA --> DBA
    AA --> DBA
    CPA --> DBA
    PPA --> DBA
    RSA --> DBA
    ODA --> DBA
    CA --> DBA
    
    EDA -.-> QA
    ESA -.-> QA
    CPA -.-> QA
    
    QA --> DBA
    
    style SG fill:#e1f5ff
    style CSA fill:#e1f5ff
    style EDA fill:#fff3e0
    style ESA fill:#fff3e0
    style AA fill:#fff3e0
    style CPA fill:#f3e5f5
    style PPA fill:#f3e5f5
    style RSA fill:#f3e5f5
    style ODA fill:#e8f5e9
    style CPOL fill:#e8f5e9
    style CA fill:#e8f5e9
    style DBA fill:#fce4ec
    style QA fill:#fce4ec
```

---

## Event Stream Architecture

```mermaid
graph TD
    KAFKA["📨 Apache Kafka"]
    
    T1["commands"]
    T2["metrics"]
    T3["enrichment"]
    T4["risks"]
    T5["predictions"]
    T6["overdue"]
    T7["actions"]
    T8["time_ticks"]
    
    CG1["acis_metrics<br/>3 partitions"]
    CG2["acis_external<br/>3 partitions"]
    CG3["acis_risks<br/>3 partitions"]
    CG4["acis_collections<br/>3 partitions"]
    
    A1["CustomerStateAgent"]
    A2["ExternalDataAgent"]
    A3["ExternalScrapingAgent"]
    A4["AggregatorAgent"]
    A5["RiskScoringAgent"]
    A6["CustomerProfileAgent"]
    A7["OverdueDetectionAgent"]
    A8["CreditPolicyAgent"]
    A9["CollectionsAgent"]
    
    T1 --> KAFKA
    T2 --> KAFKA
    T3 --> KAFKA
    T4 --> KAFKA
    T5 --> KAFKA
    T6 --> KAFKA
    T7 --> KAFKA
    T8 --> KAFKA
    
    KAFKA --> CG1
    KAFKA --> CG2
    KAFKA --> CG3
    KAFKA --> CG4
    
    CG1 --> A1
    CG2 --> A2
    CG2 --> A3
    CG2 --> A4
    CG3 --> A5
    CG3 --> A6
    CG4 --> A7
    CG4 --> A8
    CG4 --> A9
    
    style KAFKA fill:#ff6b6b,stroke:#cc0000,stroke-width:2px
    style CG1 fill:#cce5ff
    style CG2 fill:#ffe5cc
    style CG3 fill:#e5ccff
    style CG4 fill:#ccffe5
```

---

## Request-Response with Persistence

```mermaid
sequenceDiagram
    participant SG as ScenarioGen
    participant CSA as CustomerState
    participant EDA as ExternalData
    participant ESA as ExternalScrape
    participant AA as Aggregator
    participant CPA as Profile
    participant PPA as Prediction
    participant RSA as RiskScorer
    participant CPOL as CreditPolicy
    participant CA as Collections
    participant DB as DBAgent
    participant API as External APIs
    
    SG->>DB: create customers
    CSA->>DB: metrics calculated
    
    EDA->>API: Screener.in
    API-->>EDA: financial data
    EDA->>DB: store data
    
    ESA->>API: Google News
    API-->>ESA: litigation
    ESA->>DB: store data
    
    EDA->>AA: financial risk
    ESA->>AA: litigation risk
    AA->>DB: aggregated risk
    
    AA->>CPA: trigger
    CPA->>DB: resolve company
    DB-->>CPA: company name
    CPA->>PPA: profile
    
    PPA->>RSA: prediction
    RSA->>DB: risk score
    
    RSA->>CPOL: risk event
    CPOL->>CA: action
    CA->>DB: collection action
```

---

## Data Consistency & Caching Strategy

```mermaid
graph TB
    FIN["Financial Data<br/>Quarterly Updates"]
    LIT["Litigation Data<br/>Event-driven"]
    
    CD1["Track last_published"]
    CD2["Compare before store"]
    CD3["Skip if NULL data"]
    
    PS1["INSERT OR REPLACE"]
    PS2["PK: customer_id"]
    PS3["Zero duplicates"]
    
    NR1["Try financial_data"]
    NR2["Try litigation_data"]
    NR3["Query database"]
    NR4["Skip if still ID"]
    
    EDA["ExternalDataAgent"]
    ESA["ExternalScrapingAgent"]
    
    EDA --> FIN
    ESA --> LIT
    
    FIN --> CD1
    LIT --> CD1
    
    CD1 --> CD2
    CD2 --> CD3
    
    CD3 --> PS1
    PS1 --> PS2
    PS2 --> PS3
    
    PS3 --> NR1
    NR1 --> NR2
    NR2 --> NR3
    NR3 --> NR4
    
    style FIN fill:#fff9c4
    style LIT fill:#fff9c4
    style CD1 fill:#e0f2f1
    style CD2 fill:#e0f2f1
    style CD3 fill:#e0f2f1
    style PS1 fill:#bbdefb
    style PS2 fill:#bbdefb
    style PS3 fill:#bbdefb
    style NR1 fill:#f8bbd0
    style NR2 fill:#f8bbd0
    style NR3 fill:#f8bbd0
    style NR4 fill:#f8bbd0
```

