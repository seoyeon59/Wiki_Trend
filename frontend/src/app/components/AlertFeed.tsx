import { Card } from './ui/card';
import { AlertTriangle, Shield, AlertCircle } from 'lucide-react';

interface Alert {
  id: string;
  article: string;
  user: string;
  score: number;
  time: string;
  severity: 'low' | 'medium' | 'high';
}

interface AlertFeedProps {
  alerts: Alert[];
}

export function AlertFeed({ alerts }: AlertFeedProps) {
  const getSeverityIcon = (severity: string) => {
    switch (severity) {
      case 'high':
        return <AlertTriangle className="w-5 h-5 text-red-500" />;
      case 'medium':
        return <AlertCircle className="w-5 h-5 text-amber-500" />;
      default:
        return <Shield className="w-5 h-5 text-blue-500" />;
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'high':
        return 'border-l-red-500 bg-red-500/5';
      case 'medium':
        return 'border-l-amber-500 bg-amber-500/5';
      default:
        return 'border-l-blue-500 bg-blue-500/5';
    }
  };

  const getScoreColor = (score: number) => {
    if (score >= 80) return 'text-red-500';
    if (score >= 50) return 'text-amber-500';
    return 'text-blue-500';
  };

  return (
    <div className="space-y-3">
      {alerts.map((alert) => (
        <Card
          key={alert.id}
          className={`p-4 border-l-4 ${getSeverityColor(alert.severity)} bg-gray-800/50 border-gray-700`}
        >
          <div className="flex items-start gap-3">
            <div className="mt-0.5">
              {getSeverityIcon(alert.severity)}
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-start justify-between gap-2">
                <h4 className="text-white font-semibold text-sm truncate">
                  {alert.article}
                </h4>
                <span className={`text-lg font-bold shrink-0 ${getScoreColor(alert.score)}`}>
                  {alert.score}
                </span>
              </div>
              <div className="mt-1 flex items-center gap-2 text-xs text-gray-400">
                <span className="truncate">Number of Edits: {alert.user}</span>
                <span>•</span>
                <span className="shrink-0">{alert.time}</span>
              </div>
            </div>
          </div>
        </Card>
      ))}
    </div>
  );
}