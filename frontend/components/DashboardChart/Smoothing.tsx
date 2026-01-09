import React from "react";

interface SmoothingProps {
  smoothing: number;
  setSmoothing: (value: number) => void;
}

const Smoothing = ({ smoothing, setSmoothing }: SmoothingProps) => {
  return (
    <div className="mt-6 pt-6 border-t border-white/5">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
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
          <h4 className="font-medium text-sm text-white">Smoothing</h4>
        </div>
        <button
          onClick={() => setSmoothing(0)}
          className="text-md cursor-pointer text-purple-400 hover:text-purple-300 font-medium px-2 py-1 rounded hover:bg-purple-500/10 transition-all duration-200"
        >
          Reset
        </button>
      </div>
      <div className="relative">
        <div className="flex items-center justify-between mb-2">
          <span className="text-2xl font-bold text-white">{smoothing}</span>
          <span className="text-xs text-white/60">days</span>
        </div>
        <input
          type="range"
          min="0"
          max="20"
          value={smoothing}
          onChange={(e) => setSmoothing(Number(e.target.value))}
          className="w-full cursor-pointer"
        />
        <div className="flex justify-between text-[12px] text-white/60 mt-2">
          <span>Raw</span>
          <span>Smooth</span>
        </div>
      </div>
    </div>
  );
};

export default Smoothing;
