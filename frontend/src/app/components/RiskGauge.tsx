import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts';

interface RiskGaugeProps {
  riskLevel: number; // 0-100
}

export function RiskGauge({ riskLevel }: RiskGaugeProps) {
  // Determine risk status
  const getRiskStatus = (level: number) => {
    if (level < 33) return { status: 'Safe', color: '#10b981', bgColor: 'bg-emerald-500/20' };
    if (level < 66) return { status: 'Warning', color: '#f59e0b', bgColor: 'bg-amber-500/20' };
    return { status: 'Critical', color: '#ef4444', bgColor: 'bg-red-500/20' };
  };

  const { status, color, bgColor } = getRiskStatus(riskLevel);

  // Create gauge data (half circle)
  const data = [
    { name: 'value', value: riskLevel },
    { name: 'empty', value: 100 - riskLevel }
  ];

  const COLORS = [color, '#1f2937'];

  return (
    <div className="relative flex flex-col items-center">
      <ResponsiveContainer width="100%" height={220}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="85%"
            startAngle={180}
            endAngle={0}
            innerRadius="70%"
            outerRadius="100%"
            paddingAngle={0}
            dataKey="value"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index]} />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      
      <div className="absolute bottom-8 flex flex-col items-center">
        <div className="text-5xl font-bold text-white">{riskLevel}</div>
        <div className={`mt-2 px-4 py-1 rounded-full ${bgColor}`}>
          <span className="text-sm font-semibold" style={{ color }}>
            {status}
          </span>
        </div>
      </div>
    </div>
  );
}
