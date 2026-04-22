import type { CustomerMetric, RiskProfile } from '../../types/metrics'
import { WidgetCard } from './WidgetCard'
import { CollectionQueue } from './widgets/CollectionQueue'
import { ConfidenceMeter } from './widgets/ConfidenceMeter'
import { ExposureHeatmap } from './widgets/ExposureHeatmap'
import { PaymentTrendChart } from './widgets/PaymentTrendChart'
import { RiskDistributionChart } from './widgets/RiskDistributionChart'
import { RiskScoreTable } from './widgets/RiskScoreTable'
import { TopRiskCustomers } from './widgets/TopRiskCustomers'

type MetricsCanvasProps = {
  riskProfiles: RiskProfile[]
  customerMetrics: CustomerMetric[]
  computing: boolean
  layout: unknown
  onLayoutChange: (layout: unknown) => void
  onSelectCustomer: (customerId: string) => void
}

export function MetricsCanvas({
  riskProfiles,
  customerMetrics,
  computing,
  layout,
  onLayoutChange,
  onSelectCustomer,
}: MetricsCanvasProps) {
  void layout
  void onLayoutChange

  return (
    <section className="metrics-canvas metrics-grid">
      <div className="metrics-w1">
        <WidgetCard title="CUSTOMER RISK SCORE TABLE" computing={computing}>
          <RiskScoreTable rows={riskProfiles} onSelectCustomer={onSelectCustomer} />
        </WidgetCard>
      </div>

      <div className="metrics-w2">
        <WidgetCard title="RISK DISTRIBUTION" computing={computing}>
          <RiskDistributionChart rows={riskProfiles} />
        </WidgetCard>
      </div>

      <div className="metrics-w3">
        <WidgetCard title="PAYMENT BEHAVIOR TREND" computing={computing}>
          <PaymentTrendChart rows={customerMetrics} />
        </WidgetCard>
      </div>

      <div className="metrics-w4">
        <WidgetCard title="OUTSTANDING EXPOSURE" computing={computing}>
          <ExposureHeatmap rows={customerMetrics} />
        </WidgetCard>
      </div>

      <div className="metrics-w5">
        <WidgetCard title="TOP RISK CUSTOMERS" computing={computing}>
          <TopRiskCustomers rows={riskProfiles} onSelectCustomer={onSelectCustomer} />
        </WidgetCard>
      </div>

      <div className="metrics-w6">
        <WidgetCard title="MODEL CONFIDENCE" computing={computing}>
          <ConfidenceMeter
            value={
              riskProfiles.length > 0
                ? riskProfiles.reduce((sum, row) => sum + row.confidence, 0) / riskProfiles.length
                : 0
            }
            count={riskProfiles.length}
          />
        </WidgetCard>
      </div>

      <div className="metrics-w7">
        <WidgetCard title="COLLECTION PRIORITY QUEUE" computing={computing}>
          <CollectionQueue riskProfiles={riskProfiles} metrics={customerMetrics} />
        </WidgetCard>
      </div>
    </section>
  )
}
