import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from 'recharts';

interface KEITrendChartProps {
  data: Array<{ time: string; kei: number }>;
}

export function KEITrendChart({ data }: KEITrendChartProps) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="keiGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
            <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis 
          dataKey="time" 
          stroke="#9ca3af"
          tick={{ fill: '#9ca3af', fontSize: 12 }}
        />
        <YAxis 
          stroke="#9ca3af"
          tick={{ fill: '#9ca3af', fontSize: 12 }}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: '#1f2937',
            border: '1px solid #374151',
            borderRadius: '8px',
            color: '#fff'
          }}
          labelStyle={{ color: '#9ca3af' }}
        />
        <Area
          type="monotone"
          dataKey="kei"
          stroke="#3b82f6"
          strokeWidth={2}
          fill="url(#keiGradient)"
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
