export function BusLine() {
  return (
    <g>
      <defs>
        <filter id="kafkaGlow" x="-10%" y="-200%" width="130%" height="400%">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feComposite in="SourceGraphic" in2="blur" operator="over" />
        </filter>
      </defs>
      <line x1="40" y1="180" x2="960" y2="180" className="kafka-bus-glow" />
      <line x1="40" y1="180" x2="960" y2="180" className="kafka-bus" filter="url(#kafkaGlow)" />
      <line x1="40" y1="180" x2="960" y2="180" className="kafka-flow" />
      <text x="40" y="160" className="kafka-label">
        KAFKA EVENT BUS
      </text>
    </g>
  )
}
