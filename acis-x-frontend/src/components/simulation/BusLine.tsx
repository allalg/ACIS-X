export function BusLine() {
  return (
    <g>
      <defs>
        <filter id="kafkaGlow" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="8" result="blur" />
          <feComposite in="SourceGraphic" in2="blur" operator="over" />
        </filter>
      </defs>
      {/* Outer glow ring */}
      <circle cx="0" cy="0" r="100" className="kafka-bus-glow" />
      {/* Main ring */}
      <circle cx="0" cy="0" r="100" className="kafka-bus" filter="url(#kafkaGlow)" />
      {/* Flowing dashed ring */}
      <circle cx="0" cy="0" r="100" className="kafka-flow" />
      
      {/* Badge label at the top of the bus */}
      <rect x="-60" y="-110" width="120" height="20" rx="10" fill="var(--bg-surface)" stroke="var(--kafka-color)" strokeWidth="1" />
      <text x="0" y="-96" className="kafka-label" textAnchor="middle" style={{ fontSize: '9px', fontWeight: 600 }}>
        KAFKA EVENT BUS
      </text>
    </g>
  )
}
