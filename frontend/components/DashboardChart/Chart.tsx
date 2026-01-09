import React from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

interface ChartProps {
  chartData: {
    [x: string]: string | number | null | undefined;
    date: string;
    average_tone: number | null;
  }[];
  toneRange: { min: number; max: number };
  featureRange: { min: number; max: number };
  selectedFeature: string;
  selectedFeatureConfig: { label: string; color: string };
}

const Chart = ({
  chartData,
  toneRange,
  featureRange,
  selectedFeature,
  selectedFeatureConfig,
}: ChartProps) => {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart
        data={chartData}
        margin={{ top: 20, right: 60, left: 20, bottom: 20 }}
      >
        <defs>
          <linearGradient id="toneGradient" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="#9ca3af" />
            <stop offset="100%" stopColor="#6b7280" />
          </linearGradient>
          <linearGradient id="featureGradient" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor={selectedFeatureConfig.color} />
            <stop
              offset="100%"
              stopColor={selectedFeatureConfig.color}
              stopOpacity={0.7}
            />
          </linearGradient>
          <filter id="glow">
            <feGaussianBlur stdDeviation="2" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        <CartesianGrid
          strokeDasharray="3 3"
          stroke="rgba(255, 255, 255, 0.05)"
          vertical={false}
        />

        <XAxis
          dataKey="date"
          stroke="rgba(255, 255, 255, 0.3)"
          style={{ fontSize: "11px", fontWeight: 500 }}
          angle={-45}
          textAnchor="end"
          height={80}
          tick={{ fill: "rgba(255, 255, 255, 0.4)" }}
          axisLine={{ stroke: "rgba(255, 255, 255, 0.1)" }}
          tickLine={{ stroke: "rgba(255, 255, 255, 0.1)" }}
        />

        <YAxis
          yAxisId="left"
          stroke="rgba(255, 255, 255, 0.8)"
          style={{ fontSize: "11px" }}
          domain={[toneRange.min, toneRange.max]}
          tickFormatter={(value) => value.toFixed(1)}
          tick={{ fill: "rgba(255, 255, 255, 0.4)" }}
          axisLine={{ stroke: "rgba(255, 255, 255, 0.1)" }}
          tickLine={{ stroke: "rgba(255, 255, 255, 0.1)" }}
          label={{
            value: "News Tone",
            angle: -90,
            position: "insideLeft",
            style: { fill: "rgba(255, 255, 255, 0.7)", fontSize: "16px" },
          }}
        />

        <YAxis
          yAxisId="right"
          orientation="right"
          stroke={selectedFeatureConfig.color}
          style={{ fontSize: "11px" }}
          domain={[featureRange.min, featureRange.max]}
          tickFormatter={(value) => value.toFixed(1)}
          tick={{ fill: selectedFeatureConfig.color }}
          axisLine={{ stroke: `${selectedFeatureConfig.color}50` }}
          tickLine={{ stroke: `${selectedFeatureConfig.color}50` }}
          label={{
            value: selectedFeatureConfig.label,
            angle: 90,
            position: "insideRight",
            style: { fill: selectedFeatureConfig.color, fontSize: "16px" },
          }}
        />

        <Tooltip
          contentStyle={{
            backgroundColor: "rgba(15, 15, 20, 0.95)",
            border: "1px solid rgba(255, 255, 255, 0.1)",
            borderRadius: "12px",
            boxShadow: "0 20px 40px -10px rgba(0, 0, 0, 0.5)",
            backdropFilter: "blur(10px)",
            padding: "12px 16px",
          }}
          labelStyle={{
            color: "rgba(255, 255, 255, 0.7)",
            fontWeight: 600,
            marginBottom: "8px",
          }}
          itemStyle={{
            color: "rgba(255, 255, 255, 0.9)",
            padding: "2px 0",
          }}
        />

        <Legend
          wrapperStyle={{ paddingTop: "20px" }}
          iconType="line"
          formatter={(value) => (
            <span
              style={{
                color: "rgba(255, 255, 255, 0.8)",
                fontSize: "16px",
                fontWeight: 500,
              }}
            >
              {value}
            </span>
          )}
        />

        <Line
          yAxisId="left"
          type="monotone"
          dataKey="average_tone"
          stroke="url(#toneGradient)"
          strokeWidth={2.5}
          name="News Tone"
          dot={false}
          activeDot={{
            r: 6,
            fill: "#9ca3af",
            stroke: "rgba(255, 255, 255, 0.2)",
            strokeWidth: 2,
          }}
        />

        <Line
          yAxisId="right"
          type="monotone"
          dataKey={selectedFeature}
          stroke="url(#featureGradient)"
          strokeWidth={2.5}
          name={selectedFeatureConfig.label}
          dot={false}
          filter="url(#glow)"
          activeDot={{
            r: 6,
            fill: selectedFeatureConfig.color,
            stroke: "rgba(255, 255, 255, 0.2)",
            strokeWidth: 2,
          }}
        />
      </LineChart>
    </ResponsiveContainer>
  );
};

export default Chart;
