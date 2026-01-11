import React from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

interface ReliabilityChartProps {
  chartData: {
    date: string;
    reliability_score?: number | null;
  }[];
}

const ReliabilityChart = ({ chartData }: ReliabilityChartProps) => {
  // Calculate average reliability
  const validScores = chartData
    .map((d) => d.reliability_score)
    .filter((v): v is number => v !== null && v !== undefined);
  const avgReliability =
    validScores.length > 0
      ? Math.min(
          (validScores.reduce((a, b) => a + b, 0) / validScores.length) * 100,
          100
        ).toFixed(1)
      : 0;

  // Clip chart data to ensure values don't exceed 100%
  const clippedChartData = chartData.map((d) => ({
    ...d,
    reliability_score:
      d.reliability_score !== null && d.reliability_score !== undefined
        ? Math.min(d.reliability_score, 1)
        : d.reliability_score,
  }));

  return (
    <div className="glass-card p-6">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-linear-to-br from-emerald-500/20 to-teal-500/20 flex items-center justify-center">
            <svg
              className="w-4 h-4 text-emerald-400"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
          </div>
          <div>
            <h3 className="text-lg font-semibold text-white">
              Data Reliability Score
            </h3>
            <p className="text-white/70">Based on sample size per day</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-white/40">Avg:</span>
          <span className="text-sm font-bold text-emerald-400">
            {avgReliability}%
          </span>
        </div>
      </div>

      <div className="h-24 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={clippedChartData}
            margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
          >
            <defs>
              <linearGradient
                id="reliabilityGradient"
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop offset="0%" stopColor="#10b981" stopOpacity={0.4} />
                <stop offset="100%" stopColor="#10b981" stopOpacity={0} />
              </linearGradient>
            </defs>

            <CartesianGrid
              strokeDasharray="3 3"
              stroke="rgba(255, 255, 255, 0.4)"
              horizontal={true}
              vertical={false}
            />

            <XAxis
              dataKey="date"
              stroke="rgba(255, 255, 255, 0.1)"
              tick={false}
              axisLine={{ stroke: "rgba(255, 255, 255, 0.05)" }}
              tickLine={false}
            />

            <YAxis
              domain={[0, 1]}
              stroke="rgba(255, 255, 255, 0.1)"
              tick={{ fill: "rgba(255, 255, 255, 0.3)", fontSize: 10 }}
              tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
              axisLine={false}
              tickLine={false}
              width={40}
            />

            <Tooltip
              contentStyle={{
                backgroundColor: "rgba(15, 15, 20, 0.95)",
                border: "1px solid rgba(255, 255, 255, 0.1)",
                borderRadius: "10px",
                boxShadow: "0 10px 25px -5px rgba(0, 0, 0, 0.5)",
                padding: "8px 12px",
                fontSize: "12px",
              }}
              labelStyle={{
                color: "rgba(255, 255, 255, 0.6)",
                fontSize: "11px",
                marginBottom: "4px",
              }}
              formatter={(value: number) => [
                `${(value * 100).toFixed(1)}%`,
                "Reliability",
              ]}
            />

            <Area
              type="monotone"
              dataKey="reliability_score"
              stroke="#10b981"
              strokeWidth={2}
              fill="url(#reliabilityGradient)"
              name="Reliability"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default ReliabilityChart;
