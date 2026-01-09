"use client";

import { useState, useMemo, useCallback } from "react";
import {
  ChartDataPoint,
  MUSICAL_FEATURES,
  MusicalFeature,
} from "../../types/data";
import Chart from "./Chart";
import Header from "./Header";
import DataInfo from "./DataInfo";
import Smoothing from "./Smoothing";
import FeatureSelection from "./FeatureSelection";

interface DashboardChartProps {
  data: ChartDataPoint[];
}

export default function DashboardChart({ data }: DashboardChartProps) {
  const [selectedFeature, setSelectedFeature] =
    useState<MusicalFeature>("avg_energy");
  const [smoothing, setSmoothing] = useState<number>(10);

  const applySmoothingToData = useCallback(
    (data: ChartDataPoint[], windowSize: number): ChartDataPoint[] => {
      if (windowSize === 0) return data;

      return data.map((point, index) => {
        const halfWindow = Math.floor(windowSize / 2);
        const start = Math.max(0, index - halfWindow);
        const end = Math.min(data.length, index + halfWindow + 1);
        const window = data.slice(start, end);

        const avgTone =
          window.reduce((sum, p) => sum + (p.average_tone || 0), 0) /
          window.length;
        const avgFeature =
          window.reduce(
            (sum, p) => sum + ((p[selectedFeature] as number) || 0),
            0
          ) / window.length;

        return {
          ...point,
          average_tone: avgTone,
          [selectedFeature]: avgFeature,
        };
      });
    },
    [selectedFeature]
  );

  const { toneRange, featureRange, chartData } = useMemo(() => {
    // Calculate news tone range
    const toneValues = data
      .map((d) => d.average_tone)
      .filter((v) => v !== null) as number[];
    const toneMin = Math.min(...toneValues);
    const toneMax = Math.max(...toneValues);
    const tonePadding = (toneMax - toneMin) * 0.1;

    // Calculate selected feature range
    const featureValues = data
      .map((d) => d[selectedFeature])
      .filter((v) => v !== null && v !== undefined) as number[];
    const featureMin = Math.min(...featureValues);
    const featureMax = Math.max(...featureValues);
    const featurePadding = (featureMax - featureMin) * 0.1;

    // Prepare chart data
    const smoothedData = applySmoothingToData(data, smoothing);
    const chartData = smoothedData.map((point) => ({
      date: new Date(point.date).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      }),
      average_tone: point.average_tone,
      [selectedFeature]: point[selectedFeature],
    }));

    return {
      toneRange: {
        min: toneMin - tonePadding,
        max: toneMax + tonePadding,
      },
      featureRange: {
        min: featureMin - featurePadding,
        max: featureMax + featurePadding,
      },
      chartData,
    };
  }, [applySmoothingToData, data, selectedFeature, smoothing]);

  const selectedFeatureConfig = MUSICAL_FEATURES.find(
    (f) => f.key === selectedFeature
  )!;

  return (
    <div className="w-full space-y-8">
      {/* Main Chart Card */}
      <div className="glass-card p-8 glow-border">
        <Header />

        <div className="h-125 w-full">
          <Chart
            chartData={chartData}
            toneRange={toneRange}
            featureRange={featureRange}
            selectedFeature={selectedFeature}
            selectedFeatureConfig={selectedFeatureConfig}
          />
        </div>
      </div>

      {/* Feature Selection Card */}
      <div className="glass-card p-8">
        <div className="flex items-center gap-3 mb-6">
          <div className="w-10 h-10 rounded-xl bg-linear-to-br from-purple-500/20 to-cyan-500/20 flex items-center justify-center">
            <svg
              className="w-5 h-5 text-purple-400"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M9 19V6l12-3v13M9 19c0 1.105-1.343 2-3 2s-3-.895-3-2 1.343-2 3-2 3 .895 3 2zm12-3c0 1.105-1.343 2-3 2s-3-.895-3-2 1.343-2 3-2 3 .895 3 2zM9 10l12-3"
              />
            </svg>
          </div>
          <div>
            <h3 className="text-xl font-semibold text-white">
              Musical Features
            </h3>
            <p className="text text-white/70">
              Select a feature to compare with news sentiment
            </p>
          </div>
        </div>

        <FeatureSelection
          selectedFeature={selectedFeature}
          setSelectedFeature={setSelectedFeature}
        />
        <Smoothing smoothing={smoothing} setSmoothing={setSmoothing} />
      </div>

      <DataInfo
        data={chartData}
        selectedFeatureConfig={selectedFeatureConfig}
      />
    </div>
  );
}
