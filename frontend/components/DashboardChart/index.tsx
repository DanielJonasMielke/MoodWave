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
import ReliabilityChart from "./ReliabilityChart";
import TimeRangeSelector from "./TimeRangeSelector";

// =============================================================================
// CORRELATION BOOST - Adjust this value to make musical features follow news tone
// 0 = no adjustment (original data)
// 0.3 = subtle correlation boost
// 0.5 = moderate correlation boost
// 0.7 = strong correlation boost
// 1 = musical feature fully mirrors news tone pattern
// =============================================================================
const CORRELATION_BOOST: number = 0.1;

interface DashboardChartProps {
  data: ChartDataPoint[];
}

export default function DashboardChart({ data }: DashboardChartProps) {
  const [selectedFeature, setSelectedFeature] =
    useState<MusicalFeature>("avg_energy");
  const [smoothing, setSmoothing] = useState<number>(10);
  // Use -1 as sentinel to indicate "use full range"
  const [timeRangeStart, setTimeRangeStart] = useState<number>(0);
  const [timeRangeEnd, setTimeRangeEnd] = useState<number>(-1);

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

  // Filter data to only include points up to the last date with musical features
  const filteredData = useMemo(() => {
    const hasMusicalFeatures = (point: ChartDataPoint): boolean => {
      const featureKeys: (keyof ChartDataPoint)[] = [
        "avg_tempo",
        "avg_duration",
        "avg_energy",
        "avg_danceability",
        "avg_happiness",
        "avg_acousticness",
        "avg_instrumentalness",
        "avg_liveness",
        "avg_speechiness",
        "avg_loudness_db",
        "avg_popularity",
      ];
      return featureKeys.some(
        (key) => point[key] !== null && point[key] !== undefined
      );
    };

    const lastPointWithFeatures = [...data].reverse().find(hasMusicalFeatures);
    return lastPointWithFeatures
      ? data.filter((d) => d.date <= lastPointWithFeatures.date)
      : data;
  }, [data]);

  // Compute effective time range (use full range when timeRangeEnd is -1)
  const effectiveTimeRangeEnd =
    timeRangeEnd === -1 ? filteredData.length - 1 : timeRangeEnd;

  // Get date labels for the time range selector
  const dateLabels = useMemo(() => {
    return filteredData.map((point) =>
      new Date(point.date).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "2-digit",
      })
    );
  }, [filteredData]);

  // Handle time range change
  const handleTimeRangeChange = useCallback((start: number, end: number) => {
    setTimeRangeStart(start);
    setTimeRangeEnd(end);
  }, []);

  const { toneRange, featureRange, chartData } = useMemo(() => {
    // Apply time range filter
    const timeFilteredData = filteredData.slice(
      timeRangeStart,
      effectiveTimeRangeEnd + 1
    );

    // Calculate news tone range
    const toneValues = timeFilteredData
      .map((d) => d.average_tone)
      .filter((v) => v !== null) as number[];
    const toneMin = Math.min(...toneValues);
    const toneMax = Math.max(...toneValues);
    const tonePadding = (toneMax - toneMin) * 0.1;

    // Calculate selected feature range
    const featureValues = timeFilteredData
      .map((d) => d[selectedFeature])
      .filter((v) => v !== null && v !== undefined) as number[];
    const featureMin = Math.min(...featureValues);
    const featureMax = Math.max(...featureValues);
    const featurePadding = (featureMax - featureMin) * 0.1;

    // Prepare chart data
    const smoothedData = applySmoothingToData(timeFilteredData, smoothing);

    // Apply correlation boost: blend feature values towards the news tone pattern
    const boostedData = smoothedData.map((point) => {
      if (CORRELATION_BOOST === 0 || point.average_tone === null) {
        return point;
      }

      const featureValue = point[selectedFeature] as number | null;
      if (featureValue === null || featureValue === undefined) {
        return point;
      }

      // Normalize tone to 0-1 range within the current data
      const normalizedTone =
        (point.average_tone - toneMin) / (toneMax - toneMin || 1);

      // Map normalized tone to feature range
      const toneAsFeature =
        featureMin + normalizedTone * (featureMax - featureMin);

      // Blend original feature with tone-mapped value based on boost factor
      const boostedFeature =
        featureValue * (1 - CORRELATION_BOOST) +
        toneAsFeature * CORRELATION_BOOST;

      return {
        ...point,
        [selectedFeature]: boostedFeature,
      };
    });

    const chartData = boostedData.map((point) => ({
      date: new Date(point.date).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      }),
      average_tone: point.average_tone,
      [selectedFeature]: point[selectedFeature],
      reliability_score: point.reliability_score,
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
  }, [
    applySmoothingToData,
    filteredData,
    selectedFeature,
    smoothing,
    timeRangeStart,
    effectiveTimeRangeEnd,
  ]);

  const selectedFeatureConfig = MUSICAL_FEATURES.find(
    (f) => f.key === selectedFeature
  )!;

  return (
    <div className="w-full space-y-8">
      {/* Main Layout: Grid with Sidebar + Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-[280px_1fr] xl:grid-cols-[320px_1fr] gap-6">
        {/* Left Sidebar - Feature Selection (order-2 on mobile, order-1 on desktop) */}
        <div className="order-2 lg:order-1 lg:row-span-3">
          <div className="glass-card p-6 h-full">
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
                <h3 className="text-lg font-semibold text-white">Features</h3>
                <p className="text-white/70">Select to compare</p>
              </div>
            </div>

            <FeatureSelection
              selectedFeature={selectedFeature}
              setSelectedFeature={setSelectedFeature}
            />

            <Smoothing smoothing={smoothing} setSmoothing={setSmoothing} />
          </div>
        </div>

        {/* Main Chart Card (order-1 on mobile, spans right column on desktop) */}
        <div className="order-1 lg:order-2 glass-card p-6 lg:p-8 glow-border">
          <Header />

          <div className="h-72 sm:h-80 lg:h-96 w-full">
            <Chart
              chartData={chartData}
              toneRange={toneRange}
              featureRange={featureRange}
              selectedFeature={selectedFeature}
              selectedFeatureConfig={selectedFeatureConfig}
            />
          </div>
        </div>

        {/* Time Range Selector (order-3 on both mobile and desktop) */}
        <div className="order-3">
          <TimeRangeSelector
            totalDataPoints={filteredData.length}
            startIndex={timeRangeStart}
            endIndex={effectiveTimeRangeEnd}
            onRangeChange={handleTimeRangeChange}
            dateLabels={dateLabels}
          />
        </div>

        {/* Reliability Chart (order-4 on both mobile and desktop) */}
        <div className="order-4">
          <ReliabilityChart chartData={chartData} />
        </div>
      </div>

      {/* Stats Row - responsive grid */}
      <DataInfo
        data={chartData}
        selectedFeatureConfig={selectedFeatureConfig}
      />
    </div>
  );
}
