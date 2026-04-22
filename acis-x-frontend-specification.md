# ACIS-X Frontend Specification v2
## For: Senior Frontend Agent
### Project: Autonomous Credit Intelligence System — Event-Driven Multi-Agent Architecture
### Stack target: React + TypeScript + Vite, deployed on Vercel

---

## 0. Project Context

ACIS-X is a real-time, event-driven multi-agent credit risk intelligence system. Python agents communicate through **Apache Kafka** as the shared event bus. The frontend connects to a **FastAPI BFF** (`acis-api-bff`) that exposes a clean HTTP/SSE interface over the local runtime — the BFF does not exist yet and must be scaffolded as part of this project.

**Backend deployment model:**
- `acis-api-bff` runs locally, exposed via Cloudflare Tunnel at `https://api.yourdomain.com`
- Frontend is deployed on Vercel; it reads `VITE_API_BASE_URL` and `VITE_STREAM_URL` from environment
- Database: SQLite (local), never exposed directly
- Auth: API key for prototype phase (sent as `X-API-Key` header)

**What is verified and locked from the codebase:**
- 16 business agents + 5 operational agents (exact names in Section 2)
- Event envelope is canonical from `event_schema.py`
- All payload schemas are confirmed from emitter/consumer source files
- DB has 8 tables (confirmed from `db_agent.py` init)
- `acis-api-bff` must be built from scratch — API contract defined in Section 6

---

## 1. Design System

### 1.1 Aesthetic Direction

**"Precision Dark"** — a refined, high-information-density command center. Think: institutional-grade fintech intelligence platform, not a startup SaaS. The visual language should communicate authority, real-time awareness, and controlled complexity.

Key references to draw from (not copy):
- The attached dashboard showcase (OnlyGenius): borrow the structural logic — dark base, clean card anatomy, sidebar nav, data tables — but aggressively desaturate the accent colors. That reference is too vibrant for a credit risk context. Muted, deliberate color use only.
- Linear.app: extreme focus, stripped-down elegance, micro-interactions that reward attention
- Grafana dark theme: data-density done right, no decorative clutter

**Mood board in words**: a senior credit analyst's command center at 11pm. Calm, precise, purposeful. Every element has a reason to exist.

### 1.2 Color Palette

Define all colors as CSS custom properties. No hardcoded hex values in component code.

```css
:root {
  /* ── Backgrounds ────────────────────────────── */
  --bg-base:          #090b0f;   /* page background — near black with blue undertone */
  --bg-surface:       #0e1118;   /* cards, panels */
  --bg-elevated:      #141923;   /* modals, dropdowns, hover states */
  --bg-border:        #1c2333;   /* all borders and dividers */
  --bg-border-subtle: #141923;   /* inner card dividers */

  /* ── Accent colors — intentionally muted ────── */
  --accent-blue:       #3b82f6;   /* primary interactive, active nav, links */
  --accent-blue-soft:  rgba(59, 130, 246, 0.12);
  --accent-green:      #22c55e;   /* healthy / paid / on-time / LIVE indicator */
  --accent-green-soft: rgba(34, 197, 94, 0.10);
  --accent-amber:      #f59e0b;   /* Kafka bus, warning, moderate risk */
  --accent-amber-soft: rgba(245, 158, 11, 0.12);
  --accent-red:        #ef4444;   /* danger, high risk, fault, overdue */
  --accent-red-soft:   rgba(239, 68, 68, 0.10);

  /* ── Typography ─────────────────────────────── */
  --text-primary:    #dde3ed;
  --text-secondary:  #8b95a8;
  --text-muted:      #4a5568;
  --text-disabled:   #2d3748;

  /* ── Agent node colors — each agent a distinct muted hue ── */
  /* Business agents */
  --agent-scenario:         #38bdf8;   /* ScenarioGeneratorAgent — sky */
  --agent-customer-state:   #34d399;   /* CustomerStateAgent — emerald */
  --agent-aggregator:       #a78bfa;   /* AggregatorAgent — violet */
  --agent-profile:          #fb923c;   /* CustomerProfileAgent — orange */
  --agent-risk-scoring:     #f87171;   /* RiskScoringAgent — rose */
  --agent-payment-pred:     #60a5fa;   /* PaymentPredictionAgent — blue */
  --agent-collections:      #4ade80;   /* CollectionsAgent — light green */
  --agent-overdue:          #fbbf24;   /* OverdueDetectionAgent — yellow */
  --agent-credit-policy:    #e879f9;   /* CreditPolicyAgent — fuchsia */
  --agent-external-data:    #67e8f9;   /* ExternalDataAgent — cyan */
  --agent-external-scrape:  #a3e635;   /* ExternalScrapingAgent — lime */
  --agent-db:               #94a3b8;   /* DBAgent — slate */
  --agent-memory:           #c084fc;   /* MemoryAgent — purple */
  --agent-query:            #7dd3fc;   /* QueryAgent — light sky */
  --agent-time-tick:        #fcd34d;   /* TimeTickAgent — amber-yellow */

  /* Operational agents */
  --agent-monitoring:       #f59e0b;   /* MonitoringAgent — amber (matches Kafka) */
  --agent-self-healing:     #10b981;   /* SelfHealingAgent — teal */
  --agent-runtime:          #6366f1;   /* RuntimeManager — indigo */
  --agent-placement:        #8b5cf6;   /* PlacementEngine — violet */
  --agent-registry:         #ec4899;   /* RegistryService — pink */

  /* ── Kafka bus — special treatment ─────────── */
  --kafka-color:      #f59e0b;
  --kafka-glow:       rgba(245, 158, 11, 0.18);
  --kafka-flow:       rgba(245, 158, 11, 0.35);

  /* ── Risk severity ──────────────────────────── */
  --risk-low:         #22c55e;
  --risk-medium:      #f59e0b;
  --risk-high:        #ef4444;
  --risk-critical:    #dc2626;   /* darker red, pulsing */
}
```

### 1.3 Typography

```
Display / headings:   "DM Mono" — monospace, technical authority for titles and labels
Body / prose:         "Geist" — clean, modern, highly legible at small sizes
Numeric data:         "JetBrains Mono" — ensures column alignment in tables and risk scores
```

Import all three via Fontsource (`@fontsource/dm-mono`, `@fontsource/geist`, `@fontsource/jetbrains-mono`).

Type scale:
```css
--text-xs:    11px;
--text-sm:    12px;
--text-base:  13px;   /* default body */
--text-md:    14px;
--text-lg:    16px;
--text-xl:    20px;
--text-2xl:   24px;
--text-3xl:   30px;

--font-body:    'Geist', 'ui-sans-serif', sans-serif;
--font-mono:    'DM Mono', 'JetBrains Mono', 'ui-monospace', monospace;
--font-numeric: 'JetBrains Mono', 'ui-monospace', monospace;
```

Use `font-family: var(--font-numeric)` on all cells containing risk scores, amounts, timestamps, counts.

### 1.4 Spacing & Layout

- Base unit: `4px`
- Card padding: `20px`
- Card gap: `12px`
- Section gaps: `28px`
- Sidebar width: `216px` (collapsed: `52px` icon rail)
- Border radius: `6px` for cards, `4px` for badges/chips, `8px` for modals
- Card anatomy: `border: 1px solid var(--bg-border)` + `background: var(--bg-surface)` + `box-shadow: inset 0 1px 0 rgba(255,255,255,0.03)`

### 1.5 Component Libraries

- **ShadCN/UI** — use for: Table, Tooltip, Popover, Select, Dialog, Sheet, Badge, Skeleton, Separator, Tabs, ScrollArea, Sonner (toasts), Command (search)
- **ReactBits** — use for: animated number counters on KPI cards and risk scores; text scramble effect when agent names appear in registry; staggered list reveal animations
- **Recharts** — all data charts. Custom theme: no default Recharts colors anywhere. Every chart must be explicitly styled with palette colors above.
- **Framer Motion** — all layout animations, agent node state transitions, data packet flight paths, page transitions, self-healing animation sequences
- **react-grid-layout** — metrics page widget canvas only
- **react-window** — event log virtualization on simulation page
- **TanStack React Query v5** — all data fetching, polling, and cache management

### 1.6 Absolute Rules (Non-Negotiable)

- No emojis anywhere in the UI — not in labels, toasts, empty states, or error messages
- No refresh buttons on Simulation or Ledger pages — all data auto-updates
- No default ShadCN light theme leaking in — all ShadCN components must be explicitly dark-themed
- No placeholder lorem ipsum in delivered code
- No raw `fetch` calls in component files — all HTTP goes through the API service layer
- No inline styles — CSS Modules or CSS variables only
- No icon libraries (Lucide, Heroicons, etc.) — all icons are custom SVG components (see Section 7)
- No `console.log` in production code paths
- No Tailwind utility classes in JSX unless strictly required by ShadCN internals — if used, wrap and abstract
- Amount values: variable range (no hardcoded min/max assumptions in UI — derive scale dynamically from data)

---

## 2. Agent Roster (Exact Names for UI)

Use these exact names as labels everywhere — sidebar, node labels, event logs, registry panel.

### Business Agents (shown on Kafka bus canvas)

| UI Label | Class | Agent Color Variable |
|---|---|---|
| ScenarioGeneratorAgent | `scenario_generator_agent.py` | `--agent-scenario` |
| CustomerStateAgent | `customer_state_agent.py` | `--agent-customer-state` |
| AggregatorAgent | `aggregator_agent.py` | `--agent-aggregator` |
| CustomerProfileAgent | `customer_profile_agent.py` | `--agent-profile` |
| RiskScoringAgent | `risk_scoring_agent.py` | `--agent-risk-scoring` |
| PaymentPredictionAgent | `payment_prediction_agent.py` | `--agent-payment-pred` |
| CollectionsAgent | `collections_agent.py` | `--agent-collections` |
| OverdueDetectionAgent | `overdue_detection_agent.py` | `--agent-overdue` |
| CreditPolicyAgent | `credit_policy_agent.py` | `--agent-credit-policy` |
| ExternalDataAgent | `external_data_agent.py` | `--agent-external-data` |
| ExternalScrapingAgent | `external_scrapping_agent.py` | `--agent-external-scrape` |
| DBAgent | `db_agent.py` | `--agent-db` |
| MemoryAgent | `memory_agent.py` | `--agent-memory` |
| QueryAgent | `query_agent.py` | `--agent-query` |
| TimeTickAgent | `time_tick_agent.py` | `--agent-time-tick` |

### Operational Agents (shown in a dedicated operational strip, visually separated from business agents)

| UI Label | Class | Agent Color Variable |
|---|---|---|
| MonitoringAgent | `monitoring_agent.py` | `--agent-monitoring` |
| SelfHealingAgent | `self_healing_agent.py` | `--agent-self-healing` |
| RuntimeManager | `runtime_manager.py` | `--agent-runtime` |
| PlacementEngine | `placement_engine.py` | `--agent-placement` |
| RegistryService | `registry_service.py` | `--agent-registry` |

---

## 3. Application Structure

### 3.1 Routes

```
/                   → redirect to /simulation
/simulation         → Kafka Event Bus + Agent Flow Visualization
/ledger             → Customer Invoices & Payments (real-time)
/metrics            → Risk Scores, Predictions, Windowed Analytics
/customers          → Customer List + Profile Drilldown
```

### 3.2 Navigation (Left Sidebar)

```
Top:
  Brand: "ACIS-X" in var(--font-mono), 13px, letter-spacing: 0.15em
         Subtitle: "Credit Intelligence" in --text-muted, 10px

Nav items (icon + label):
  Simulation
  Ledger
  Metrics
  Customers

Bottom of sidebar:
  System health pill:
    All agents healthy  → green dot + "OPERATIONAL"
    One or more degraded → amber dot + "DEGRADED"
    Fault detected      → red dot + "FAULT" (pulsing)
  Connects to /health endpoint, polls every 5s
```

Active nav item: `background: var(--bg-elevated)`, `border-left: 2px solid var(--accent-blue)`, text in `--text-primary`.

Sidebar collapses to 52px icon rail on viewport < 1280px. Expand/collapse toggle at bottom.

---

## 4. Page Specifications

---

### PAGE 1 — Simulation (`/simulation`)

**Purpose**: The flagship view. Real-time visualization of every agent and the Kafka event bus. Should feel like a network operations center display, not a chart dashboard.

#### 4.1.1 Page Layout

```
┌──────────────────────────────────────────────────────────┐
│  PAGE HEADER: "SIMULATION" + filter controls + health   │
├──────────────────────────────────────────────────────────┤
│                                                          │
│   BUS CANVAS ZONE  (SVG, ~45vh)                          │
│   ─ Kafka bus + all agent branches + data packets ─      │
│                                                          │
├──────────────────────────────────────────────────────────┤
│  EVENT STREAM ZONE  (~55vh, two columns)                 │
│  Left 60%: live event log   Right 40%: agent panels     │
└──────────────────────────────────────────────────────────┘
```

#### 4.1.2 Kafka Bus Canvas (SVG)

Render as an `<svg>` element with a `viewBox` that scales responsively. Use a `ResizeObserver` to update the SVG's computed positions when the container resizes.

**The Bus:**
```
- A thick horizontal path spanning full canvas width, vertically centered in the Bus Canvas Zone
- Stroke color: var(--kafka-color)
- Stroke width: 3px
- Label: "KAFKA EVENT BUS" in small-caps DM Mono, positioned above the left end
- Ambient glow: SVG filter (feGaussianBlur + feComposite) creating a soft amber halo — blur radius 6px
- Flow animation: a repeating pattern of short dashes moving left-to-right at ~60px/s
  Implement as an SVG <path> with stroke-dasharray + stroke-dashoffset animated via CSS keyframes
  This ambient flow runs always, independent of specific events — it represents background Kafka throughput
```

**Agent Branches — Business Agents:**
```
Layout: 15 agents equally spaced along the bus (horizontal). 
Each agent renders as:
  - A vertical line descending from the bus to the node circle
  - A circle (node), 44px diameter
  - Agent label below the circle: 10px DM Mono, --text-secondary

Vertical line: stroke = agent color, stroke-width = 1.5px, opacity = 0.4 when idle
Node circle:
  - fill: var(--bg-surface)
  - stroke: [agent color]
  - stroke-width: 1.5px
```

**Operational Agent Strip:**
```
Render the 5 operational agents as a second, thinner horizontal row ABOVE the main bus,
connected to the bus by shorter upward lines.
These are visually distinct — smaller nodes (32px), slightly dimmer.
MonitoringAgent and SelfHealingAgent should be positioned closest to center,
as they have the most interaction with other agents.
```

**Agent Status States (Framer Motion variants):**
```
IDLE:
  - Node stroke opacity: 0.35
  - No glow
  - Static

ACTIVE (processing an event):
  - Node stroke opacity: 1.0
  - Outer pulse ring: <circle> that scales from 1 → 1.4 and fades opacity 0.8 → 0, 0.9s ease-out
  - Subtle box-shadow equivalent via SVG filter

PROCESSING (sustained work, multi-event):
  - Spinning arc: SVG <circle> with stroke-dasharray + stroke-dashoffset rotating 360deg, 1.2s linear infinite
  - Arc color: agent color

HEARTBEAT (regular ping from TimeTickAgent):
  - Brief single pulse (ring expands and fades), 0.3s — less dramatic than ACTIVE

ERROR:
  - Node stroke: var(--accent-red)
  - Shake: translateX(−3px, 3px, −3px, 0), 0.4s
  - Small "ERR" badge appears above node

SELF-HEALING (see dedicated section 4.1.6):
  - Amber rotating dashed ring
```

#### 4.1.3 Data Packet Animations (Framer Motion)

Data packets are small animated pills traveling along branch lines and the bus.

**Packet Anatomy:**
```
Shape: rounded rect, height 16px, width auto (content-dependent), border-radius 8px
Background: agent color at 20% opacity
Border: 1px solid agent color at 60% opacity
Label: 3-4 char event type abbreviation, 9px DM Mono, agent color

Event type abbreviations (confirmed from event_schema.py event types):
  INV       invoice.created / invoice.updated
  PAY       payment.received / payment.applied
  STATE     customer.metrics.updated
  RISK      risk.profile.updated / risk.scored
  PROF      customer.profile.updated
  COLL      collections action
  OVRD      overdue.detected
  EXT       external data signal
  TICK      time.tick
  REG       agent.registered
  HEALTH    agent.health
  HEAL      self.healing action
  DB        db write confirmation
  MEM       memory agent event
  PRED      payment prediction
  POLICY    credit policy event
```

**Confirmed Data Flows (from codebase verification):**
```
ScenarioGeneratorAgent → bus:  INV, PAY
Bus → CustomerStateAgent:      INV, PAY
CustomerStateAgent → bus:      STATE
Bus → AggregatorAgent:         STATE + EXT
Bus → RiskScoringAgent:        INV (invoice-level risk)
AggregatorAgent → bus:         RISK (profile-level)
RiskScoringAgent → bus:        RISK (invoice-level)
Bus → CustomerProfileAgent:    RISK
CustomerProfileAgent → bus:    PROF
Bus → OverdueDetectionAgent:   INV, TICK
OverdueDetectionAgent → bus:   OVRD
Bus → CollectionsAgent:        RISK, OVRD
CollectionsAgent → bus:        COLL
Bus → CreditPolicyAgent:       RISK, STATE
Bus → PaymentPredictionAgent:  STATE, PAY
PaymentPredictionAgent → bus:  PRED
ExternalDataAgent → bus:       EXT
ExternalScrapingAgent → bus:   EXT
Bus → MemoryAgent:             (wide subscription — multiple types)
Bus → DBAgent:                 all types (persistence)
TimeTickAgent → bus:           TICK
MonitoringAgent → bus:         HEALTH (monitoring events)
Bus → SelfHealingAgent:        HEALTH
SelfHealingAgent → bus:        HEAL
```

**Packet Animation:**
```
Producer → Bus:
  Packet spawns at agent node, travels up the branch line to the bus junction point
  Duration: ~400ms, easing: easeIn

Bus transit:
  Packet moves horizontally along the bus toward the consuming agent's junction
  Duration: proportional to horizontal distance, ~200–600ms

Bus → Consumer:
  Packet descends from junction down the branch to the consuming agent node
  Duration: ~400ms, easing: easeOut
  On arrival: brief node ACTIVE state triggers, packet dissolves with fade

Multiple packets in flight simultaneously — no queue blocking.
Stagger packets from the same source by ~150ms.

For DBAgent (consumes everything): do NOT render individual packets for every event.
Instead, render a soft continuous shimmer on the DBAgent branch to indicate sustained write traffic.
```

#### 4.1.4 Registry Panel

```
Position: top-right corner of Bus Canvas Zone
Size: 240px wide, scrollable

Header: "AGENT REGISTRY" in small-caps, 10px

Each entry:
  [colored dot, 6px] [agent name, 11px mono] [registered_at as relative time]
  Fields from registration payload: agent_id, agent_name, status, version

Animations:
  New registration: slide down + fade in (Framer Motion AnimatePresence)
  Deregistration: fade out + collapse height
  Status change: dot color transitions (CSS transition: 0.3s)

Status dot colors:
  registered / healthy → agent's color
  degraded             → --accent-amber
  error                → --accent-red
  offline              → --text-muted
```

#### 4.1.5 Event Stream Zone

**Left column — Live Event Log (60% width):**
```
Header: "EVENT STREAM" + LIVE indicator + event count

Virtualized list (react-window FixedSizeList):
  Row height: 28px
  Auto-scroll to newest entry
  Pause auto-scroll on hover (resume 3s after mouse leaves)

Each row columns:
  [timestamp]    [event_id truncated]    [event_type]    [entity_id]    [event_source]

Exact fields mapped from event_schema.py canonical envelope:
  event_time    → formatted "HH:mm:ss.ms"
  event_id      → first 8 chars + "…"
  event_type    → full string (e.g. "invoice.created")
  entity_id     → customer_id or invoice_id
  event_source  → agent name abbreviated

All text: 11px JetBrains Mono, --text-secondary
Timestamp: --text-muted

New row entry animation: background flash (row agent color at 12% opacity → transparent, 1.8s)
event_type column: color = agent color of the source agent
```

**Right column — Agent Activity Panels (40% width):**
```
A scrollable list of compact cards — one per business agent (15 cards).
Operational agents are shown in a separate collapsed section at the bottom.

Each card:
  Header: [colored dot] [agent name] [status badge]
  Metrics row:
    Events/min   Last event (relative)   Lag (queue depth if available)
  Sparkline: tiny bar chart, last 60 seconds of event rate, 1 bar = 1 second
  
Card pulses (box-shadow glow in agent color) briefly when the agent processes a new event.
Status badge uses same states as node: IDLE / ACTIVE / PROCESSING / ERROR
```

#### 4.1.6 Self-Healing Animation Sequence

Trigger: SSE event with `event_type: "self.healing.triggered"` or `"agent.health"` indicating a fault + recovery.

```
PHASE 1 — FAULT DETECTED (MonitoringAgent triggers):
  Affected agent node: stroke → --accent-red, shake animation
  "ERR" badge appears above node
  Toast (bottom-right): "Fault detected: [AgentName] — [reason from event payload]"

PHASE 2 — SELF-HEALER ACTIVATES:
  SelfHealingAgent node: ACTIVE state, color → --agent-self-healing (teal)
  MonitoringAgent node: ACTIVE state, amber pulse

PHASE 3 — HEAL COMMAND IN FLIGHT:
  A dashed amber arc travels along the bus from MonitoringAgent/SelfHealingAgent 
  → affected agent's branch junction → down to the affected node
  Arc is SVG <path> with stroke-dasharray animated (draw-on effect via stroke-dashoffset)
  Duration: ~1200ms

PHASE 4 — RECOVERY:
  Affected agent node: amber dashed spinning ring ("RECOVERING" state)
  Duration: variable — hold this state until a recovery confirmation event arrives

PHASE 5 — RESTORED:
  Affected agent node: brief green flash (stroke → --accent-green for 0.5s) then returns to normal
  Toast: "Recovered: [AgentName] restored to operational state"
  All animation states clear

Implement as a Framer Motion orchestrated sequence using `animate` with `delay` chaining.
Sequence must handle multiple simultaneous faults independently.
```

#### 4.1.7 Simulation Controls Bar

```
Thin bar directly below the page header:

Left side:
  "SPEED" label + segmented control: 0.5x | 1x | 2x | 4x
  (Controls animation playback speed multiplier and SSE reconnect behavior)

Center:
  "FOCUS AGENT" — ShadCN Select: filters event log to show only events for selected agent
  "EVENT TYPES" — multi-select chips (all event type abbreviations from 4.1.3)

Right side:
  System health badge (matches sidebar bottom indicator, same polling source)
  Connection status: "CONNECTED" (green) / "RECONNECTING…" (amber) / "DISCONNECTED" (red)

NO REFRESH BUTTON. SSE is persistent; React Query handles reconnection automatically.
```

---

### PAGE 2 — Ledger (`/ledger`)

**Purpose**: Real-time view of invoices and payments. Zero manual interaction required for data to stay current. This page is a live accounts receivable feed.

#### 4.2.1 Layout

```
┌───────────────────────────────────────────────────────┐
│ Header: "LEDGER" + subtitle + LIVE badge + last-sync  │
├──────────────────────┬────────────────────────────────┤
│  INVOICES  (55%)     │  PAYMENTS  (45%)               │
│                      │                                 │
│  [table]             │  [table]                        │
│                      │                                 │
├──────────────────────┴────────────────────────────────┤
│  KPI SUMMARY STRIP  (4 cards)                         │
└───────────────────────────────────────────────────────┘
```

#### 4.2.2 Invoice Table

Polling: `GET /invoices` every 3 seconds via React Query (`refetchInterval: 3000`, `staleTime: 0`).

**Columns (from verified schema):**
```
Invoice ID     | Customer       | Amount (total_amount)  | Due Date
Paid Amount    | Status         | Days Overdue           | Actions
```

**Status Badges (ShadCN Badge, custom styled):**
```
pending     → background: var(--accent-amber-soft), color: var(--accent-amber),  text: "PENDING"
overdue     → background: var(--accent-red-soft),   color: var(--accent-red),    text: "OVERDUE"
paid        → background: var(--accent-green-soft),  color: var(--accent-green),  text: "PAID"
partial     → background: var(--accent-blue-soft),   color: var(--accent-blue),   text: "PARTIAL"
cancelled   → background: transparent, color: var(--text-muted), text: "CANCELLED"
disputed    → border: 1px solid var(--accent-amber), text: "DISPUTED"
```

**Overdue visual treatment:**
```
Rows with status=overdue:
  Left border: 2px solid var(--accent-red)
  No background color change — left border alone is sufficient signal
  Days overdue cell: color var(--accent-red), font-weight 600
```

**Amount display:**
- Use `Intl.NumberFormat` for all amounts — currency symbol from the `currency` field in payload
- `total_amount` and `paid_amount` in separate columns so partial payment progress is visible

**Real-time behavior:**
```
New invoice rows: slide in from top + background pulse (--accent-blue-soft flash, 2s fade)
Status change: badge color transitions via CSS transition: 0.4s
Sorting: all columns sortable, default: due_date ascending
Quick filter chips above table: All | Pending | Overdue | Paid | Partial
Infinite scroll (preferred over pagination — more consistent with "live feed" feel)
```

#### 4.2.3 Payment Table

Polling: same strategy as invoices.

**Columns (from verified schema):**
```
Payment ID     | Customer       | Invoice Ref  | Amount
Payment Date   | Method         | Status       | Reference
```

**Method icons** — custom SVG components (monochrome, stroke-based, 16×16):
```
bank_transfer  → BankTransferIcon
card           → CardPaymentIcon
cheque         → ChequeIcon
other          → GenericPaymentIcon
```

**Status badges:**
```
received    → green
processing  → blue (animated shimmer on text)
applied     → green (slightly dimmer than received)
failed      → red
```

**Real-time behavior:**
```
New payments: slide in from top + green glow flash on row (--accent-green-soft, 2.5s fade)
Payment linking: hover a payment row highlights the linked invoice row (if visible in left table)
  Implement via shared `hoveredInvoiceId` state lifted to Ledger page
```

#### 4.2.4 KPI Summary Cards

Four cards in a horizontal strip at the bottom. All numbers animate on value change (ReactBits animated counter, 400ms ease-out).

```
Card 1: Total Outstanding
  Large number (total_amount - paid_amount, summed across open invoices)
  Sub-label: "[N] open invoices"
  Color: --text-primary

Card 2: Overdue Amount
  Large number — sum of all overdue invoice outstanding amounts
  Color: --accent-red if > 0, otherwise --text-muted
  Sub-label: "[N] overdue invoices"

Card 3: Payments Received Today
  Large number — sum of payments with payment_date = today
  Color: --accent-green
  Sub-label: "[N] transactions"

Card 4: On-Time Payment Ratio
  Small donut chart inside the card (Recharts PieChart, 64px)
  Derived from /dashboard/summary: on_time_ratio field
  Arc colors: green (on-time) / red (late)
  Center label: percentage value
```

---

### PAGE 3 — Metrics (`/metrics`)

**Purpose**: Risk scores, predictions, and analytics per customer. Windowed layout with movable, resizable widgets. The only page with a manual compute trigger — because metric computation aggregates all accumulated data and is expensive.

#### 4.3.1 Page Header

```
Left: "METRICS" + "Risk & Prediction Analytics" subtitle
Right: 
  Timestamp: "Last computed: [relative time]" (e.g. "4 min ago")
  "COMPUTE METRICS" button:
    - Triggers POST /metrics/compute, then polls /metrics/result/:jobId
    - On click: button → loading state (spinner replaces label, button disabled)
    - All widgets enter loading/computing state simultaneously
    - When results arrive: widgets animate data in with staggered reveals
```

#### 4.3.2 Widget Canvas

```
Library: react-grid-layout
Grid: 12 columns, rowHeight: 72px, margin: [12, 12]
Drag handle: a 32px wide strip at top-left of each widget card (shows on hover only)
Resize handle: bottom-right corner
Layout persisted: localStorage key "acis-x-metrics-layout"
"Reset Layout" button in page header: clears localStorage and restores defaults
```

Each widget card:
```
Header bar (32px):
  Left: widget title in 11px DM Mono small-caps
  Right: settings icon (opens ShadCN Popover) + expand icon (opens ShadCN Dialog fullscreen)
  Full header is the drag handle — cursor: grab, cursor: grabbing on drag
Border: 1px solid var(--bg-border)
Body: widget content with 16px padding
```

#### 4.3.3 Widget Specifications

**W1 — Customer Risk Score Table**
```
Default size: 8 cols × 6 rows

Columns (fields from customer_risk_profile table + customers table):
  Customer Name | Risk Score (0–100) | Severity | Financial Risk | Litigation Risk | Confidence | Updated

Risk Score display:
  Number: font-family var(--font-numeric), color by severity
  Progress bar: 80px horizontal bar next to number, color = severity color

Severity badge:
  low      → --risk-low (green)
  medium   → --risk-medium (amber)
  high     → --risk-high (red)
  critical → --risk-critical (darker red) + keyframe pulse on badge background

Row click → opens ShadCN Sheet (right slide-in panel) with full customer profile (see Page 4 detail)
Default sort: combined_risk descending
```

**W2 — Risk Distribution**
```
Default size: 6 cols × 4 rows

Recharts BarChart:
  X-axis: severity buckets (low / medium / high / critical)
  Y-axis: customer count
  Bar colors: --risk-low, --risk-medium, --risk-high, --risk-critical respectively
  No gradient — solid fills
  Tooltip: "[N] customers" + list of customer names (max 5, then "+N more")
```

**W3 — Payment Behavior Trend**
```
Default size: 6 cols × 4 rows

Recharts AreaChart:
  X-axis: date (last 30 days from last compute)
  Y-axis: on_time_ratio (0–100%)
  Data source: avg_delay and on_time_ratio from customer_metrics table over time
  Render top-5 riskiest customers as separate colored line series
  Each line: agent color? No — use a distinct set: slate, rose, sky, amber, violet (in that order)
  Semi-transparent area fill under each line (15% opacity)
  Legend: customer names
  Tooltip: all customer values at that date
```

**W4 — Outstanding Exposure by Aging Bucket**
```
Default size: 6 cols × 4 rows

Table-based heatmap:
  Rows: customers (top 20 by outstanding)
  Columns: aging buckets — Current (<30d) | 30-60d | 60-90d | 90d+
  Cell content: amount value
  Cell background: linear scale from var(--bg-elevated) at 0 → var(--accent-red-soft) at max
  No hardcoded max — normalize to the maximum cell value in the dataset
  Hover: ShadCN Tooltip showing exact amount + invoice count
  Column totals row at bottom: bold, slightly brighter background
```

**W5 — Top Risk Customers**
```
Default size: 4 cols × 5 rows

Ranked list (1–10):
  [rank] [customer name] [risk bar] [combined_risk score] [delta indicator]

Delta indicator: change in combined_risk vs previous compute
  Worsened (↑): --accent-red, upward arrow SVG
  Improved (↓): --accent-green, downward arrow SVG  
  No change: --text-muted, dash

On row click: same Sheet panel as W1 row click
```

**W6 — Model Confidence Meter**
```
Default size: 3 cols × 3 rows

Data source: avg confidence from customer_risk_profile table

Circular SVG gauge:
  Outer ring: track (--bg-border)
  Inner arc: fill proportional to avg confidence value (0–1 → 0–360deg)
  Arc color: > 0.8 → green | 0.6–0.8 → amber | < 0.6 → red
  Center: large percentage number + "CONFIDENCE" label below
  
Below gauge: "Based on [N] risk profiles"
No animated spinner here — static display, updates on compute
```

**W7 — Collection Priority Queue**
```
Default size: 4 cols × 5 rows

Data source: customers with high/critical severity, sorted by combined_risk DESC

Columns:
  Customer | Urgency Score | Recommended Action | Days Since Last Payment | Outstanding

Urgency Score: derived as combined_risk × (1 + days_overdue_factor)
  Display as a 0–10 number, 1 decimal place, font-numeric

Recommended Action: text from CollectionsAgent events if available, else derived rule:
  combined_risk > 0.85 → "Escalate immediately"
  combined_risk > 0.7  → "Send formal notice"
  combined_risk > 0.5  → "Follow up — payment due"
  else                 → "Monitor"

No row click action on this widget — it is read-only display
```

#### 4.3.4 Widget Loading State

```
On compute trigger (all widgets simultaneously):
  - Widget body: ShadCN Skeleton overlay, matching widget's current height
  - Skeleton shimmer: NOT the default gray — use --bg-elevated / --bg-border alternating
  - "Computing…" text below widget title, 10px, --text-muted, no spinner icon
  - Global thin progress bar at page top (2px height, --accent-blue, animates 0→100% over estimated compute time)

On data arrival:
  Framer Motion staggerChildren: widgets reveal their content one by one, 80ms stagger
  Each widget content: fade-in + translateY(8px → 0)
  Counter values: ReactBits animated roll-up
```

#### 4.3.5 Widget Interactions

```
DRAG: Other widgets dim to 0.65 opacity while dragging one
RESIZE: Recharts ResponsiveContainer handles re-render; debounce resize by 100ms
MINIMIZE: Double-click widget header → collapses to header bar only (height: 32px)
MAXIMIZE: Expand icon → ShadCN Dialog, full viewport with 40px padding
SETTINGS: Per-widget ShadCN Popover:
  - W3: date range selector
  - W1, W5: sort field selector
  - W4: top-N customer count selector (10 / 20 / 50)
```

---

### PAGE 4 — Customers (`/customers`)

**Purpose**: Customer list and individual risk profiles. Implement last.

#### 4.4.1 Customer List

```
Header: "CUSTOMERS" + search (ShadCN Command) + filter by severity

Table columns (from customers table + customer_risk_profile):
  Customer ID | Name | Credit Limit | Risk Score | Severity | Avg Delay | On-Time Ratio | Status

Severity filter tabs: All | Low | Medium | High | Critical

Sorting: all columns sortable
Pagination: 25 per page (ShadCN pagination)
Row click → navigate to /customers/:id
Search: real-time filter by customer name or customer_id
```

#### 4.4.2 Customer Profile Page (`/customers/:id`)

```
Fetch: GET /customers/:id

Header section:
  Customer name (large) + customer_id (mono, muted) + severity badge
  Credit limit | Risk score | On-time ratio — as KPI chips

Tab layout (ShadCN Tabs):
  Overview | Invoices | Payments | Risk History | Collections

OVERVIEW tab:
  Two columns:
    Left: metrics from customer_metrics (avg_delay, total_outstanding, last_payment_date, on_time_ratio)
    Right: risk profile (financial_risk, litigation_risk, combined_risk, confidence, severity, sources)
  Risk breakdown: horizontal stacked bar (financial vs litigation contribution)

INVOICES tab:
  Same table as Ledger page, pre-filtered to this customer
  Polls every 5s (less aggressive than ledger main view)

PAYMENTS tab:
  Same table as Ledger page, pre-filtered to this customer

RISK HISTORY tab:
  Recharts LineChart: combined_risk over time (from historical customer_risk_profile records)
  Severity threshold lines as reference: dashed horizontal lines at 0.4 and 0.7

COLLECTIONS tab:
  Log of collections actions from collections_log table
  Columns: action_type | date | notes | outcome
```

---

## 5. Global UI Patterns

### 5.1 LIVE Indicator

Used on Simulation and Ledger page headers:

```css
.live-indicator {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-family: var(--font-mono);
  font-size: var(--text-xs);
  color: var(--accent-green);
  letter-spacing: 0.1em;
}

.live-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--accent-green);
  animation: livePulse 2s ease-in-out infinite;
}

@keyframes livePulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50%       { opacity: 0.5; transform: scale(0.85); }
}
```

When SSE/polling fails: dot → `--accent-red`, label → "OFFLINE", no animation.

### 5.2 Toast Notifications (ShadCN Sonner)

```
Position: bottom-right
Max visible: 3
Duration: 5000ms (errors: persistent until dismissed)

Custom styling — override Sonner defaults entirely:
  background: var(--bg-elevated)
  border: 1px solid var(--bg-border)
  color: var(--text-primary)
  font-family: var(--font-mono)
  font-size: var(--text-sm)
  border-radius: 6px

Toast types and triggers:
  INFO  (blue left border):  New agent registered, minor system events
  WARN  (amber left border): Agent degraded, high-risk customer detected
  ERROR (red left border):   API connection lost, fault detected
  OK    (green left border): Self-healing recovery complete, system restored

No icons in toasts — left border color alone communicates type.
No emojis.
```

### 5.3 Loading States

```
Page initial load:
  Skeleton screens (ShadCN Skeleton) for every section
  Skeleton shimmer: --bg-elevated → --bg-border, CSS animation
  Minimum display: 300ms even if data arrives faster (prevents flash)
  Do NOT use a page-level spinner — skeleton layouts communicate structure

Background refetch:
  2px top progress bar (NProgress-style), --accent-blue
  No skeleton — data updates in-place with brief highlight

Widget computing (Metrics page):
  Skeleton overlay on widget body only
  Header remains visible and interactive during compute
```

### 5.4 Empty States

For sections with no data yet:

```
Custom SVG graphic: abstract horizontal line with 3–4 node circles on it
  (references the Kafka bus concept — intentional, thematic)
  Monochrome: --text-muted stroke, no fill

Heading: "No data yet"  
Subtext: context-appropriate — e.g.:
  Event log: "Waiting for events from the stream…"
  Invoices: "No invoices have been generated yet."
  Risk table: "Compute metrics to see risk profiles."

No button unless a user action is the only path to data (like Metrics page → "Compute Metrics")
```

### 5.5 Error States

```
Inline error (inside affected section, not full-page):
  Small AlertTriangleIcon (custom SVG) + message + retry timer
  "Failed to load. Retrying in [N]s."
  Auto-retry via React Query's exponential backoff

API key error (403): full-page overlay with a simple message:
  "Access denied. Check API key configuration."
  No styling flourish — just text on dark background.

Tunnel disconnected (network error after initial connect):
  Toast: "Connection to API lost — attempting reconnect"
  LIVE badge flips to OFFLINE
  React Query continues retrying in background
```

### 5.6 Scrollbars

```css
* { scrollbar-width: thin; scrollbar-color: var(--bg-border) var(--bg-base); }
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--bg-base); }
::-webkit-scrollbar-thumb { background: var(--bg-border); border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
```

---

## 6. API Contract (acis-api-bff)

The BFF must be built as a FastAPI app. The frontend agent should scaffold it alongside the frontend (or at minimum document the stubs needed). All endpoints are prefixed `/api/v1`.

### 6.1 Authentication

```
Header: X-API-Key: <key>
All endpoints require this header.
FastAPI: implement as a dependency (check against env var ACIS_API_KEY)
CORS: allow Vercel domain + localhost:5173 in development. No wildcard.
```

### 6.2 Endpoints

**GET /api/v1/health**
```json
{
  "status": "ok",
  "service": "acis-api-bff",
  "version": "0.1.0",
  "timestamp": "2026-04-22T14:32:00Z"
}
```

**GET /api/v1/dashboard/summary**
```json
{
  "timestamp": "ISO8601",
  "total_customers": 42,
  "total_invoices": 310,
  "open_invoices": 87,
  "overdue_invoices": 12,
  "total_outstanding": 2840000.00,
  "avg_delay": 4.2,
  "on_time_ratio": 0.78,
  "high_risk_count": 5,
  "medium_risk_count": 14,
  "low_risk_count": 23
}
```

**GET /api/v1/customers**
```json
{
  "customers": [
    {
      "customer_id": "string",
      "name": "string",
      "credit_limit": 500000.00,
      "risk_score": 0.72,
      "status": "active",
      "combined_risk": 0.72,
      "severity": "high",
      "total_outstanding": 180000.00,
      "avg_delay": 8.3,
      "on_time_ratio": 0.61,
      "updated_at": "ISO8601"
    }
  ],
  "total": 42
}
```

**GET /api/v1/customers/{id}**
```json
{
  "customer_id": "string",
  "name": "string",
  "credit_limit": 500000.00,
  "risk_score": 0.72,
  "total_outstanding": 180000.00,
  "avg_delay": 8.3,
  "on_time_ratio": 0.61,
  "overdue_count": 3,
  "last_payment_date": "ISO8601",
  "financial_risk": 0.65,
  "litigation_risk": 0.80,
  "combined_risk": 0.72,
  "severity": "high",
  "confidence": 0.88,
  "updated_at": "ISO8601"
}
```

**GET /api/v1/invoices**
```
Query params: customer_id (optional), status (optional), page, limit
```
```json
{
  "invoices": [
    {
      "invoice_id": "string",
      "customer_id": "string",
      "customer_name": "string",
      "total_amount": 75000.00,
      "paid_amount": 0.00,
      "currency": "USD",
      "issued_date": "ISO8601",
      "due_date": "ISO8601",
      "status": "overdue",
      "days_overdue": 14,
      "created_at": "ISO8601",
      "updated_at": "ISO8601"
    }
  ],
  "total": 87
}
```

**GET /api/v1/payments**
```
Query params: customer_id (optional), invoice_id (optional), page, limit
```
```json
{
  "payments": [
    {
      "payment_id": "string",
      "invoice_id": "string",
      "customer_id": "string",
      "customer_name": "string",
      "amount": 50000.00,
      "currency": "USD",
      "payment_date": "ISO8601",
      "payment_method": "bank_transfer",
      "status": "applied",
      "reference": "string",
      "created_at": "ISO8601"
    }
  ],
  "total": 223
}
```

**GET /api/v1/events/stream** (SSE)
```
Transport: Server-Sent Events
Content-Type: text/event-stream
Connection: keep-alive

Each SSE event:
  event: acis_event
  data: <JSON>

JSON shape (mirrors event_schema.py canonical Event):
{
  "event_id": "uuid",
  "event_type": "invoice.created",
  "event_source": "ScenarioGeneratorAgent",
  "event_time": "ISO8601",
  "correlation_id": "uuid or null",
  "entity_id": "customer_id or invoice_id",
  "schema_version": "1.0",
  "payload": { ... },
  "metadata": { ... }
}

The BFF subscribes to all Kafka topics and relays events through this SSE endpoint.
Heartbeat: send a comment (": heartbeat") every 15s to keep connection alive.
```

**GET /api/v1/agents/status**
```json
{
  "agents": [
    {
      "agent_id": "string",
      "agent_name": "ScenarioGeneratorAgent",
      "agent_type": "string",
      "status": "active",
      "registered_at": "ISO8601",
      "last_heartbeat": "ISO8601",
      "topics": {
        "consumes": ["acis.invoices"],
        "produces": ["acis.payments"]
      },
      "capabilities": ["string"],
      "version": "string"
    }
  ]
}
```
Poll interval: 5 seconds.

**POST /api/v1/metrics/compute**
```json
// Response:
{ "job_id": "uuid", "status": "computing", "started_at": "ISO8601" }
```

**GET /api/v1/metrics/result/{job_id}**
```json
{
  "job_id": "uuid",
  "status": "ready",
  "computed_at": "ISO8601",
  "data": {
    "risk_profiles": [ /* customer_risk_profile rows */ ],
    "customer_metrics": [ /* customer_metrics rows */ ],
    "summary": { /* aggregated stats */ }
  }
}
```

### 6.3 Frontend Environment Variables

```env
VITE_API_BASE_URL=https://api.yourdomain.com
VITE_STREAM_URL=https://api.yourdomain.com/api/v1/events/stream
VITE_API_KEY=your_api_key_here
```

---

## 7. Data Layer Architecture

### 7.1 React Query Configuration

```typescript
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 3,
      retryDelay: attempt => Math.min(1000 * 2 ** attempt, 15000),
      refetchOnWindowFocus: false,
    },
  },
});

// Per-page polling intervals:
// Simulation (agents/status): 5000ms
// Ledger (invoices + payments): 3000ms
// Metrics: no auto-refetch (manual trigger only)
// Customers list: 10000ms
// /health: 5000ms
```

### 7.2 SSE Client

```typescript
// src/services/eventStream.ts
// Use EventSource API — no library needed
// On error: exponential backoff reconnect (1s, 2s, 4s, max 30s)
// Expose as a React context so multiple components can subscribe without duplicate connections
// Dispatch events to a local EventEmitter — components subscribe by event_type filter
```

### 7.3 API Service Layer

```typescript
// src/services/api.ts
// All fetch calls live here — components never call fetch directly
// Attaches X-API-Key header from env automatically
// On 401/403: redirect to error state
// On network error: throw structured ApiError for React Query to handle
```

### 7.4 Stub Data

For development without the backend running:

```typescript
// src/services/stubs.ts
// Activated when VITE_USE_STUBS=true
// Generates realistic data using seeded random (use `seedrandom` library)
// Simulates SSE events on a timer: new invoice every 3–8s, payment every 5–12s, risk update every 15–30s
// Agent statuses cycle through IDLE/ACTIVE/PROCESSING realistically
// Stubs honor the exact same TypeScript interfaces as real API responses
```

---

## 8. Custom SVG Icon Components

All icons are React components returning inline SVG. No icon library. All use `currentColor` for stroke so they inherit CSS color. Default size prop: 16. Stroke-based (not filled). ViewBox: `0 0 16 16`.

```
# Navigation
SimulationIcon     — horizontal line with three vertical drops (mini bus representation)
LedgerIcon         — document with two horizontal lines
MetricsIcon        — three ascending bars
CustomersIcon      — two overlapping circles (abstract people)

# Status
LiveDotIcon        — simple circle (used in LIVE indicator — animated via CSS on parent)
AlertTriangleIcon  — triangle with vertical line + dot inside
CheckCircleIcon    — circle with a checkmark path inside
SpinnerIcon        — partial arc (used programmatically with CSS rotate animation)

# Agent system
AgentNodeIcon      — circle with small inner hexagon
KafkaBusIcon       — horizontal double line with vertical tick marks
RegistryIcon       — 2×3 dot grid
SelfHealIcon       — circular arrow with small plus mark

# Data / tables
SortAscIcon        — upward chevron
SortDescIcon       — downward chevron
FilterIcon         — funnel outline
DragHandleIcon     — 2×3 dot grid (same as RegistryIcon — alias it)
ExpandIcon         — four outward-pointing corner arrows
CollapseIcon       — four inward-pointing corner arrows
RefreshIcon        — circular arrow (used ONLY on Metrics page button)

# Risk
RiskShieldIcon     — shield outline with small graph line inside
TrendUpIcon        — diagonal upward arrow
TrendDownIcon      — diagonal downward arrow

# Payment method
BankTransferIcon   — two horizontal arrows (←→) with a horizontal line between
CardPaymentIcon    — rectangle with horizontal stripe near top
ChequeIcon         — document outline with diagonal hatching lines
GenericPaymentIcon — circle with a currency symbol stroke path
```

---

## 9. File Structure

```
acis-x-frontend/
├── src/
│   ├── components/
│   │   ├── layout/
│   │   │   ├── Sidebar.tsx
│   │   │   ├── SidebarNav.tsx
│   │   │   ├── SystemHealthBadge.tsx
│   │   │   └── Layout.tsx
│   │   ├── simulation/
│   │   │   ├── KafkaBusCanvas.tsx        ← main SVG orchestrator
│   │   │   ├── BusLine.tsx               ← ambient flow animation
│   │   │   ├── AgentNode.tsx             ← individual node + status variants
│   │   │   ├── AgentBranch.tsx           ← vertical line from bus to node
│   │   │   ├── DataPacket.tsx            ← animated pill component
│   │   │   ├── PacketOrchestrator.tsx    ← manages all in-flight packets
│   │   │   ├── OperationalAgentStrip.tsx ← monitoring/healing agents row
│   │   │   ├── RegistryPanel.tsx
│   │   │   ├── EventLog.tsx              ← virtualized (react-window)
│   │   │   ├── AgentActivityCard.tsx
│   │   │   ├── AgentActivityPanel.tsx    ← right column of event zone
│   │   │   ├── SelfHealingSequence.tsx   ← orchestrated animation
│   │   │   └── SimulationControls.tsx
│   │   ├── ledger/
│   │   │   ├── InvoiceTable.tsx
│   │   │   ├── PaymentTable.tsx
│   │   │   ├── StatusBadge.tsx
│   │   │   ├── PaymentMethodIcon.tsx
│   │   │   └── LedgerKPIStrip.tsx
│   │   ├── metrics/
│   │   │   ├── MetricsCanvas.tsx         ← react-grid-layout wrapper
│   │   │   ├── WidgetCard.tsx            ← shared widget shell
│   │   │   ├── widgets/
│   │   │   │   ├── RiskScoreTable.tsx
│   │   │   │   ├── RiskDistributionChart.tsx
│   │   │   │   ├── PaymentTrendChart.tsx
│   │   │   │   ├── ExposureHeatmap.tsx
│   │   │   │   ├── TopRiskCustomers.tsx
│   │   │   │   ├── ConfidenceMeter.tsx
│   │   │   │   └── CollectionQueue.tsx
│   │   │   └── ComputeButton.tsx
│   │   ├── customers/
│   │   │   ├── CustomerTable.tsx
│   │   │   ├── CustomerSearch.tsx
│   │   │   └── profile/
│   │   │       ├── CustomerProfileSheet.tsx  ← ShadCN Sheet (from row click)
│   │   │       ├── CustomerProfilePage.tsx   ← full page /customers/:id
│   │   │       ├── RiskBreakdownBar.tsx
│   │   │       └── RiskHistoryChart.tsx
│   │   └── ui/
│   │       ├── LiveIndicator.tsx
│   │       ├── SeverityBadge.tsx
│   │       ├── AnimatedCounter.tsx       ← ReactBits wrapper
│   │       ├── SkeletonCard.tsx
│   │       ├── EmptyState.tsx
│   │       ├── ErrorState.tsx
│   │       ├── TopProgressBar.tsx
│   │       └── icons/                   ← all 30 SVG icon components
│   ├── services/
│   │   ├── api.ts
│   │   ├── eventStream.ts
│   │   └── stubs.ts
│   ├── hooks/
│   │   ├── useAgentStatus.ts
│   │   ├── useEventStream.ts
│   │   ├── useDashboardSummary.ts
│   │   ├── useInvoices.ts
│   │   ├── usePayments.ts
│   │   ├── useCustomers.ts
│   │   ├── useCustomerProfile.ts
│   │   └── useMetrics.ts
│   ├── contexts/
│   │   └── EventStreamContext.tsx        ← SSE singleton + subscriber pattern
│   ├── types/
│   │   ├── agent.ts
│   │   ├── events.ts
│   │   ├── ledger.ts
│   │   ├── metrics.ts
│   │   └── api.ts
│   ├── pages/
│   │   ├── SimulationPage.tsx
│   │   ├── LedgerPage.tsx
│   │   ├── MetricsPage.tsx
│   │   └── CustomersPage.tsx
│   ├── styles/
│   │   ├── globals.css                   ← CSS variables, scrollbars, resets
│   │   └── shadcn-overrides.css          ← force ShadCN dark theme globally
│   ├── lib/
│   │   └── utils.ts                      ← cn(), formatCurrency(), formatRelativeTime()
│   ├── App.tsx
│   └── main.tsx
├── .env.local                            ← VITE_API_BASE_URL, VITE_STREAM_URL, VITE_API_KEY
├── .env.example
├── vite.config.ts
├── tsconfig.json
└── package.json
```

---

## 10. Tech Stack Summary

```
Framework:        React 18 + TypeScript + Vite
Styling:          CSS Modules + CSS Custom Properties (no Tailwind in JSX)
                  Tailwind installed only if ShadCN requires it — scoped, never used directly in components
Components:       ShadCN/UI (Radix-based, fully re-themed dark)
Animation:        Framer Motion (layout, page transitions, agent states, packet flights)
Data fetching:    TanStack React Query v5
Charts:           Recharts (all custom themed — zero default Recharts colors)
Grid layout:      react-grid-layout (metrics page only)
Virtualization:   react-window (event log)
UI extras:        ReactBits (animated counters, text effects)
Fonts:            DM Mono + Geist + JetBrains Mono via Fontsource
Icons:            30 custom inline SVG components — no icon library
SSE client:       native EventSource API wrapped in EventStreamContext
Stubs:            seedrandom-based deterministic stub generator
Deployment:       Vercel (env vars set in Vercel dashboard)
```

---

## 11. Deliverable Checklist

**Foundation**
- [ ] Vite + React 18 + TypeScript project scaffold
- [ ] CSS variable system in `globals.css`
- [ ] ShadCN dark theme override in `shadcn-overrides.css`
- [ ] Fontsource imports (DM Mono, Geist, JetBrains Mono)
- [ ] React Query setup with per-page polling configs
- [ ] API service layer with auth header injection
- [ ] SSE client (EventStreamContext) with reconnection logic
- [ ] Stub data generator (activated via env flag)
- [ ] All 30 custom SVG icon components
- [ ] Sidebar with routing, collapse behavior, system health badge

**Simulation Page**
- [ ] Kafka bus SVG with ambient flow animation
- [ ] All 15 business agent nodes + branches
- [ ] 5 operational agent nodes in separate strip
- [ ] Agent status state machine (IDLE / ACTIVE / PROCESSING / HEARTBEAT / ERROR)
- [ ] Data packet animations for all confirmed flows
- [ ] DBAgent shimmer treatment (no individual packets)
- [ ] Registry panel with AnimatePresence entries/exits
- [ ] Event log (react-window, virtualized, auto-scroll with hover-pause)
- [ ] Agent activity panel (15 cards + sparklines)
- [ ] Self-healing 5-phase animation sequence
- [ ] Simulation controls bar (speed, focus, event type filter)

**Ledger Page**
- [ ] Invoice table with correct columns from verified schema
- [ ] Payment table with correct columns from verified schema
- [ ] All status badges (custom styled ShadCN Badge)
- [ ] Payment method SVG icons
- [ ] Real-time polling (3s) with new-row animations
- [ ] Invoice/payment cross-highlighting on hover
- [ ] KPI strip (4 cards with animated counters + donut chart)

**Metrics Page**
- [ ] react-grid-layout canvas with 12-col grid
- [ ] Layout persistence in localStorage + Reset Layout
- [ ] All 7 widgets (W1–W7) implemented
- [ ] Widget shell (drag handle, resize, settings, expand, minimize)
- [ ] Compute trigger + global progress bar + staggered widget reveal
- [ ] All chart theming (no default Recharts colors)
- [ ] Widget fullscreen via ShadCN Dialog

**Customers Page**
- [ ] Customer list table
- [ ] Search + severity filter tabs
- [ ] Customer profile page with 5 tabs
- [ ] CustomerProfileSheet (ShadCN Sheet, triggered from Metrics W1 and W5 row clicks)

**Global**
- [ ] Toast notification system (ShadCN Sonner, custom styled)
- [ ] LIVE indicator component
- [ ] Empty state component with custom SVG
- [ ] Error state component with retry timer
- [ ] Top progress bar (background refetch indicator)
- [ ] Lazy-loaded routes (React.lazy + Suspense)
- [ ] .env.example with documented variables

---

*ACIS-X Frontend Specification v2 — finalized April 2026*
*All agent names, payload schemas, and DB table structures verified against codebase*