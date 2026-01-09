import React from "react";

interface SmoothingProps {
  smoothing: number;
  setSmoothing: (value: number) => void;
}

const Smoothing = ({ smoothing, setSmoothing }: SmoothingProps) => {
  return (
    <div className="mt-8 pt-8 border-t border-white/5">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-linear-to-br from-cyan-500/20 to-purple-500/20 flex items-center justify-center">
            <svg
              className="w-4 h-4 text-cyan-400"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M13 10V3L4 14h7v7l9-11h-7z"
              />
            </svg>
          </div>
          <div>
            <h4 className="font-medium text-lg text-white">Data Smoothing</h4>
            <p className="text-white/70">Window size: {smoothing} days</p>
          </div>
        </div>
        <button
          onClick={() => setSmoothing(0)}
          className="px-4 cursor-pointer py-2 text-purple-400 hover:text-purple-300 font-medium rounded-lg hover:bg-purple-500/10 transition-all duration-200"
        >
          Reset
        </button>
      </div>
      <div className="relative">
        <input
          type="range"
          min="0"
          max="20"
          value={smoothing}
          onChange={(e) => setSmoothing(Number(e.target.value))}
          className="w-full cursor-pointer"
        />
        <div className="flex justify-between text-xs text-white/50 mt-3">
          <span>Raw Data</span>
          <span>Smooth Curves</span>
        </div>
      </div>
    </div>
  );
};

export default Smoothing;
