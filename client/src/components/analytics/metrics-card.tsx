import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { LucideIcon } from "lucide-react";

interface MetricsCardProps {
  title: string;
  value: string | number;
  icon: LucideIcon;
  trend?: string;
  trendLabel?: string;
  variant?: 'default' | 'success' | 'processing' | 'revenue';
  className?: string;
}

export default function MetricsCard({
  title,
  value,
  icon: Icon,
  trend,
  trendLabel,
  variant = 'default',
  className,
}: MetricsCardProps) {
  const getVariantStyles = () => {
    switch (variant) {
      case 'success':
        return {
          iconBg: 'bg-emerald-400/10',
          iconColor: 'text-emerald-400',
        };
      case 'processing':
        return {
          iconBg: 'bg-amber-400/10',
          iconColor: 'text-amber-400',
        };
      case 'revenue':
        return {
          iconBg: 'bg-purple-400/10',
          iconColor: 'text-purple-400',
        };
      default:
        return {
          iconBg: 'bg-blue-400/10',
          iconColor: 'text-blue-400',
        };
    }
  };

  const getTrendColor = () => {
    if (!trend) return '';
    if (trend.includes('+') || trend === 'Real-time') return 'text-green-600';
    if (trend.includes('-')) return 'text-red-600';
    return 'text-blue-600';
  };

  const variantStyles = getVariantStyles();

  return (
    <Card className={cn("card-hover", className)}>
      <CardContent className="p-6">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm text-muted-foreground">{title}</p>
            <p className="text-3xl font-bold text-foreground">{value}</p>
          </div>
          <div className={cn("h-12 w-12 rounded-lg flex items-center justify-center", variantStyles.iconBg)}>
            <Icon className={cn("h-6 w-6", variantStyles.iconColor)} />
          </div>
        </div>
        {(trend || trendLabel) && (
          <div className="mt-4 flex items-center">
            {trend && (
              <Badge variant="outline" className={cn("text-sm font-medium", getTrendColor())}>
                {trend}
              </Badge>
            )}
            {trendLabel && (
              <span className="text-muted-foreground text-sm ml-2">{trendLabel}</span>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
