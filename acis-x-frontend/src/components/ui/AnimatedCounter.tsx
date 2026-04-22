import { animate, useMotionValue, useTransform, motion } from 'framer-motion'
import { useEffect } from 'react'

type AnimatedCounterProps = {
  value: number
  duration?: number
  formatter?: (value: number) => string
}

export function AnimatedCounter({
  value,
  duration = 0.4,
  formatter = (next) => Math.round(next).toString(),
}: AnimatedCounterProps) {
  const motionValue = useMotionValue(0)
  const display = useTransform(motionValue, (current) => formatter(current))

  useEffect(() => {
    const controls = animate(motionValue, value, {
      duration,
      ease: 'easeOut',
    })
    return () => controls.stop()
  }, [motionValue, value, duration])

  return <motion.span className="numeric">{display}</motion.span>
}
