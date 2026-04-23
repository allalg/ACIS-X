import { motion } from 'framer-motion'

type DataPacketProps = {
  id: string
  label: string
  colorClass: string
  x: number
  y: number
}

export function DataPacket({ id, label, colorClass, x, y }: DataPacketProps) {
  return (
    <motion.g
      key={id}
      className={`data-packet ${colorClass}`}
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
      transform={`translate(${x} ${y})`}
    >
      <rect x="-16" y="-8" width="32" height="16" rx="8" />
      <text x="0" y="3" textAnchor="middle">
        {label}
      </text>
    </motion.g>
  )
}
