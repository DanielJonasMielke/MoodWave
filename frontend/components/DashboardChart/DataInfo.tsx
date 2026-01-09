import React from "react";

interface DataInfoProps {
  data: {
    [x: string]: string | number | null | undefined;
    date: string;
    average_tone: number | null;
  }[];
  selectedFeatureConfig: { label: string; color: string };
}

const DataInfo = ({ data, selectedFeatureConfig }: DataInfoProps) => {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      {/* Data Points Card */}
      <div className="stat-card group">
        <div className="absolute top-0 right-0 w-32 h-32 rounded-full bg-purple-500/10 blur-2xl group-hover:bg-purple-500/20 transition-all duration-500" />
        <div className="relative">
          <div className="flex items-center gap-3 mb-4">
            <div className="w-10 h-10 rounded-xl bg-purple-500/20 flex items-center justify-center">
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
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
            </div>
            <span className="font-medium text-white/70 uppercase tracking-wider">
              Data Points
            </span>
          </div>
          <div className="text-4xl font-bold text-white mb-1">
            {data.length.toLocaleString()}
          </div>
          <div className="text-white/50">Total observations</div>
        </div>
      </div>

      {/* Selected Feature Card */}
      <div className="stat-card group">
        <div
          className="absolute top-0 right-0 w-32 h-32 rounded-full blur-2xl transition-all duration-500"
          style={{ backgroundColor: `${selectedFeatureConfig.color}15` }}
        />
        <div className="relative">
          <div className="flex items-center gap-3 mb-4">
            <div
              className="w-10 h-10 rounded-xl flex items-center justify-center"
              style={{ backgroundColor: `${selectedFeatureConfig.color}20` }}
            >
              <svg
                className="w-5 h-5"
                style={{ color: selectedFeatureConfig.color }}
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
            <span className="font-medium text-white/70 uppercase tracking-wider">
              Active Feature
            </span>
          </div>
          <div
            className="text-3xl font-bold mb-1"
            style={{ color: selectedFeatureConfig.color }}
          >
            {selectedFeatureConfig.label}
          </div>
          <div className="text-white/50">Currently analyzing</div>
        </div>
      </div>

      {/* Date Range Card */}
      <div className="stat-card group">
        <div className="absolute top-0 right-0 w-32 h-32 rounded-full bg-cyan-500/10 blur-2xl group-hover:bg-cyan-500/20 transition-all duration-500" />
        <div className="relative">
          <div className="flex items-center gap-3 mb-4">
            <div className="w-10 h-10 rounded-xl bg-cyan-500/20 flex items-center justify-center">
              <svg
                className="w-5 h-5 text-cyan-400"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"
                />
              </svg>
            </div>
            <span className="font-medium text-white/70 uppercase tracking-wider">
              Date Range
            </span>
          </div>
          <div className="text-2xl font-bold text-white mb-1">
            {data.length > 0 && (
              <>
                {data[0].date} — {data[data.length - 1].date}
              </>
            )}
          </div>
          <div className="text-white/50">Time period covered</div>
        </div>
      </div>
    </div>
  );
};

export default DataInfo;
