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
  onSelectCustomer: (customerId: string) => void
}

export function MetricsCanvas({
  riskProfiles,
  customerMetrics,
  computing,
  onSelectCustomer,
}: MetricsCanvasProps) {
  return (
    <section className="metrics-canvas metrics-grid">
      <div className="metrics-span-full">
        <WidgetCard title="CUSTOMER RISK SCORE TABLE" computing={computing}>
          <RiskScoreTable rows={riskProfiles} onSelectCustomer={onSelectCustomer} />
        </WidgetCard>
      </div>

      <div className="metrics-span-half">
        <WidgetCard title="RISK DISTRIBUTION" computing={computing}>
          <RiskDistributionChart rows={riskProfiles} />
        </WidgetCard>
      </div>

      <div className="metrics-span-half">
        <WidgetCard title="PAYMENT BEHAVIOR TREND" computing={computing}>
          <PaymentTrendChart rows={customerMetrics} />
        </WidgetCard>
      </div>

      <div className="metrics-span-full">
        <WidgetCard title="OUTSTANDING EXPOSURE" computing={computing}>
          <ExposureHeatmap rows={customerMetrics} />
        </WidgetCard>
      </div>

      <div className="metrics-span-half">
        <WidgetCard title="TOP RISK CUSTOMERS" computing={computing}>
          <TopRiskCustomers rows={riskProfiles} onSelectCustomer={onSelectCustomer} />
        </WidgetCard>
      </div>

      <div className="metrics-span-quarter">
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

      <div className="metrics-span-full">
        <WidgetCard title="COLLECTION PRIORITY QUEUE" computing={computing}>
          <CollectionQueue riskProfiles={riskProfiles} metrics={customerMetrics} />
        </WidgetCard>
      </div>
    </section>
  )
}
