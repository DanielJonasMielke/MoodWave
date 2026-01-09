import React from "react";

const Header = () => {
  return (
    <div className="mb-8">
      <div className="flex items-center gap-4 mb-4">
        <div className="w-12 h-12 rounded-2xl bg-linear-to-br from-purple-500 to-cyan-500 flex items-center justify-center shadow-lg shadow-purple-500/25">
          <svg
            className="w-6 h-6 text-white"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z"
            />
          </svg>
        </div>
        <div>
          <h2 className="text-2xl font-bold text-white">
            Sentiment & Sound Correlation
          </h2>
          <p className="text-white/70 text-sm">
            Real-time visualization of mood patterns across media
          </p>
        </div>
      </div>

      {/* Legend hints */}
      <div className="flex items-center gap-6 mt-6 pt-6 border-t border-white/5">
        <div className="flex items-center gap-2">
          <div className="w-8 h-1 rounded-full bg-linear-to-r from-gray-400 to-gray-600" />
          <span className="text-xs text-white/50">News Tone</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-8 h-1 rounded-full bg-linear-to-r from-purple-400 to-cyan-400" />
          <span className="text-xs text-white/50">Selected Feature</span>
        </div>
      </div>
    </div>
  );
};

export default Header;
