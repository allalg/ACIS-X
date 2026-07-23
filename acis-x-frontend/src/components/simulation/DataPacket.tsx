import { motion } from 'framer-motion'

type DataPacketProps = {
  id: string
  label: string
  colorClass: string
  fromX: number
  fromY: number
  toX: number
  toY: number
}

export function DataPacket({ id, label, colorClass, fromX, fromY, toX, toY }: DataPacketProps) {
  return (
    <motion.g
      key={id}
      className={`data-packet ${colorClass}`}
      initial={{ opacity: 0, x: fromX, y: fromY }}
      animate={{ opacity: 1, x: toX, y: toY }}
      exit={{ opacity: 0, x: toX, y: toY }}
      transition={{ duration: 0.6, ease: "easeOut" }}
    >
      <rect x="-16" y="-8" width="32" height="16" rx="8" />
      <text x="0" y="3" textAnchor="middle">
        {label}
      </text>
    </motion.g>
  )
}
