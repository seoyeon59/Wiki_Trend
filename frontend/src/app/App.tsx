import { useState, useEffect } from 'react';
import { RiskGauge } from './components/RiskGauge';
import { KEITrendChart } from './components/KEITrendChart';
import { AlertFeed } from './components/AlertFeed';
import { Card } from './components/ui/card';
import { Activity, Bell, TrendingUp } from 'lucide-react';

// Generate mock 24-hour data
const generateKEIData = () => {
  const data = [];
  const now = new Date();
  // Adjust for KST (UTC+9)
  const kstOffset = 9 * 60 * 60 * 1000;
  const kstNow = new Date(now.getTime() + kstOffset);
  
  for (let i = 23; i >= 0; i--) {
    const time = new Date(kstNow.getTime() - i * 60 * 60 * 1000);
    const hour = time.getUTCHours();
    const minute = i === 0 ? time.getUTCMinutes() : 0;
    data.push({
      time: i === 0 ? `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}` : `${hour.toString().padStart(2, '0')}:00`,
      kei: Math.floor(Math.random() * 40) + 30 + Math.sin(i / 3) * 15
    });
  }
  return data;
};

// Mock alert data
const generateAlerts = () => [
  {
    id: '1',
    article: 'Climate Change Policy 2026',
    user: '247',
    score: 92,
    time: '2 min ago',
    severity: 'high' as const
  },
  {
    id: '2',
    article: 'Artificial Intelligence Ethics',
    user: '189',
    score: 78,
    time: '5 min ago',
    severity: 'high' as const
  },
  {
    id: '3',
    article: 'International Space Station',
    user: '156',
    score: 65,
    time: '8 min ago',
    severity: 'medium' as const
  },
  {
    id: '4',
    article: 'Renewable Energy Sources',
    user: '134',
    score: 54,
    time: '12 min ago',
    severity: 'medium' as const
  },
  {
    id: '5',
    article: 'Quantum Computing',
    user: '98',
    score: 47,
    time: '15 min ago',
    severity: 'medium' as const
  },
  {
    id: '6',
    article: 'Mediterranean Cuisine',
    user: '67',
    score: 32,
    time: '18 min ago',
    severity: 'low' as const
  }
];

function App() {
  const [riskLevel, setRiskLevel] = useState(67);
  const [keiData, setKeiData] = useState(generateKEIData());
  const [alerts, setAlerts] = useState(generateAlerts());

  // Simulate real-time updates
  useEffect(() => {
    const interval = setInterval(() => {
      // Update risk level
      setRiskLevel((prev) => {
        const change = (Math.random() - 0.5) * 10;
        return Math.max(0, Math.min(100, prev + change));
      });

      // Update KEI data
      setKeiData((prev) => {
        const newData = [...prev.slice(1)];
        const lastValue = prev[prev.length - 1].kei;
        const now = new Date();
        const hour = now.getHours();
        newData.push({
          time: `${hour.toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`,
          kei: Math.max(20, Math.min(80, lastValue + (Math.random() - 0.5) * 8))
        });
        return newData;
      });
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const currentKEI = keiData[keiData.length - 1]?.kei || 0;
  const avgKEI = Math.round(keiData.reduce((sum, d) => sum + d.kei, 0) / keiData.length);

  return (
    <div className="min-h-screen bg-gray-950 text-white p-6">
      {/* Header */}
      <header className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-4xl font-bold bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent">
              Wiki Trend
            </h1>
            <p className="text-gray-400 mt-1">Trends Excavation by Wikipedia Edits</p>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2 px-4 py-2 bg-gray-800/50 rounded-lg border border-gray-700">
              <Activity className="w-5 h-5 text-green-500" />
              <span className="text-sm text-gray-300">System Active</span>
            </div>
            <div className="text-right">
              <div className="text-xs text-gray-500">Last Update</div>
              <div className="text-sm text-gray-300">
                {new Date().toLocaleTimeString()}
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Dashboard Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Risk Gauge */}
        <Card className="lg:col-span-1 bg-gray-900/50 border-gray-800 p-6">
          <div className="flex items-center gap-2 mb-4">
            <TrendingUp className="w-5 h-5 text-blue-400" />
            <h2 className="text-xl font-semibold text-white">Current Trend Keyword</h2>
          </div>
          {/* Placeholder area for dynamic keyword */}
          <div className="h-[220px] flex items-center justify-center border-2 border-dashed border-gray-700 rounded-lg">
            <span className="text-gray-500 text-sm">Dynamic keyword area</span>
          </div>
          <div className="mt-6 grid grid-cols-2 gap-4 pt-4 border-t border-gray-800">
            <div>
              <div className="text-xs text-gray-500">Current KEI</div>
              <div className="text-2xl font-bold text-blue-400">{currentKEI.toFixed(1)}</div>
            </div>
            <div>
              <div className="text-xs text-gray-500">24h Average</div>
              <div className="text-2xl font-bold text-gray-300">{avgKEI}</div>
            </div>
          </div>
        </Card>

        {/* KEI Trend Chart */}
        <Card className="lg:col-span-2 bg-gray-900/50 border-gray-800 p-6">
          <div className="flex items-center gap-2 mb-4">
            <Activity className="w-5 h-5 text-blue-400" />
            <h2 className="text-xl font-semibold text-white">Edits of Last 24 Hours</h2>
          </div>
          <div className="h-[280px]">
            <KEITrendChart data={keiData} />
          </div>
        </Card>

        {/* Alert Feed */}
        <Card className="lg:col-span-3 bg-gray-900/50 border-gray-800 p-6">
          <div className="flex items-center gap-2 mb-4">
            <Bell className="w-5 h-5 text-amber-400" />
            <h2 className="text-xl font-semibold text-white">Recent Alert Keywords</h2>
            <span className="ml-auto text-xs text-gray-500 bg-gray-800 px-2 py-1 rounded-full">
              {alerts.length} Active Alerts
            </span>
          </div>
          <AlertFeed alerts={alerts} />
        </Card>
      </div>

      {/* Footer Stats */}
      <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="bg-gray-900/30 border-gray-800 p-4">
          <div className="text-xs text-gray-500">Today's Monitored Articles</div>
          <div className="text-2xl font-bold text-white mt-1">6,547,892</div>
        </Card>
        <Card className="bg-gray-900/30 border-gray-800 p-4">
          <div className="text-xs text-gray-500">Active Editors (24h)</div>
          <div className="text-2xl font-bold text-white mt-1">142,536</div>
        </Card>
        <Card className="bg-gray-900/30 border-gray-800 p-4">
          <div className="text-xs text-gray-500">Total Edits (24h)</div>
          <div className="text-2xl font-bold text-amber-500 mt-1">1,247</div>
        </Card>
      </div>
    </div>
  );
}

export default App;