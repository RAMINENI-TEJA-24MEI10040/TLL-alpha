"use client"

import { Card } from "@/components/ui/card"
import { LucideIcon } from "lucide-react"
import { motion } from "framer-motion"
import { TrendingUp, TrendingDown } from "lucide-react"

interface MetricCardProps {
  title: string
  value: string | number
  description?: string
  icon: LucideIcon
  trend?: {
    value: number
    label: string
    isPositive: boolean
  }
  highlight?: boolean
}

export function MetricCard({ title, value, description, icon: Icon, trend, highlight }: MetricCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
    >
      <Card className={`flex flex-col h-full ${highlight ? 'border-primary/30 bg-primary/5' : ''}`}>
        <div className="flex justify-between items-start mb-3">
          <div className="flex flex-col min-w-0">
            <span className="text-[10px] uppercase tracking-widest text-muted font-bold truncate">{title}</span>
            <span className="text-2xl font-black text-foreground mt-1 truncate">{value}</span>
          </div>
          <div className={`p-2.5 rounded-xl border shrink-0 ml-2 ${highlight ? 'bg-primary/10 border-primary/20' : 'bg-slate-50 border-border'}`}>
            <Icon className={`w-5 h-5 ${highlight ? 'text-primary' : 'text-slate-500'}`} />
          </div>
        </div>

        {trend && (
          <div className="flex items-center mt-auto pt-3 border-t border-border/60 gap-1.5">
            {trend.isPositive ? (
              <TrendingUp className="w-3.5 h-3.5 text-success" />
            ) : (
              <TrendingDown className="w-3.5 h-3.5 text-destructive" />
            )}
            <span className={`text-xs font-bold ${trend.isPositive ? 'text-success' : 'text-destructive'}`}>
              {trend.isPositive ? '+' : ''}{trend.value}%
            </span>
            <span className="text-xs text-muted font-medium truncate">{trend.label}</span>
          </div>
        )}

        {description && !trend && (
          <div className="mt-auto pt-3 border-t border-border/60">
            <span className="text-xs text-muted font-medium leading-snug line-clamp-2">{description}</span>
          </div>
        )}
      </Card>
    </motion.div>
  )
}

