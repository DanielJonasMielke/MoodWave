import React, { useCallback, useRef, useState, useEffect } from "react";

interface TimeRangeSelectorProps {
  totalDataPoints: number;
  startIndex: number;
  endIndex: number;
  onRangeChange: (start: number, end: number) => void;
  dateLabels: string[];
}

const TimeRangeSelector = ({
  totalDataPoints,
  startIndex,
  endIndex,
  onRangeChange,
  dateLabels,
}: TimeRangeSelectorProps) => {
  const trackRef = useRef<HTMLDivElement>(null);
  const [isDragging, setIsDragging] = useState<"start" | "end" | null>(null);

  const getPositionFromIndex = (index: number) => {
    if (totalDataPoints <= 1) return 0;
    return (index / (totalDataPoints - 1)) * 100;
  };

  const getIndexFromPosition = useCallback(
    (clientX: number) => {
      if (!trackRef.current || totalDataPoints <= 1) return 0;
      const rect = trackRef.current.getBoundingClientRect();
      const position = (clientX - rect.left) / rect.width;
      const clampedPosition = Math.max(0, Math.min(1, position));
      return Math.round(clampedPosition * (totalDataPoints - 1));
    },
    [totalDataPoints]
  );

  const handleMouseDown = useCallback(
    (handle: "start" | "end") => (e: React.MouseEvent) => {
      e.preventDefault();
      setIsDragging(handle);
    },
    []
  );

  const handleTouchStart = useCallback(
    (handle: "start" | "end") => (e: React.TouchEvent) => {
      e.preventDefault();
      setIsDragging(handle);
    },
    []
  );

  useEffect(() => {
    if (!isDragging) return;

    const handleMove = (clientX: number) => {
      const newIndex = getIndexFromPosition(clientX);

      if (isDragging === "start") {
        // Ensure start doesn't go past end (leave at least 1 day gap)
        const maxStart = Math.max(0, endIndex - 1);
        const clampedIndex = Math.min(newIndex, maxStart);
        onRangeChange(clampedIndex, endIndex);
      } else {
        // Ensure end doesn't go before start (leave at least 1 day gap)
        const minEnd = Math.min(totalDataPoints - 1, startIndex + 1);
        const clampedIndex = Math.max(newIndex, minEnd);
        onRangeChange(startIndex, clampedIndex);
      }
    };

    const handleMouseMove = (e: MouseEvent) => handleMove(e.clientX);
    const handleTouchMove = (e: TouchEvent) => {
      if (e.touches.length > 0) {
        handleMove(e.touches[0].clientX);
      }
    };

    const handleEnd = () => setIsDragging(null);

    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleEnd);
    document.addEventListener("touchmove", handleTouchMove);
    document.addEventListener("touchend", handleEnd);

    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleEnd);
      document.removeEventListener("touchmove", handleTouchMove);
      document.removeEventListener("touchend", handleEnd);
    };
  }, [
    isDragging,
    startIndex,
    endIndex,
    totalDataPoints,
    getIndexFromPosition,
    onRangeChange,
  ]);

  const startPosition = getPositionFromIndex(startIndex);
  const endPosition = getPositionFromIndex(endIndex);

  const startLabel = dateLabels[startIndex] || "";
  const endLabel = dateLabels[endIndex] || "";
  const selectedCount = endIndex - startIndex + 1;

  const handleReset = () => {
    onRangeChange(0, totalDataPoints - 1);
  };

  const isFullRange = startIndex === 0 && endIndex === totalDataPoints - 1;

  return (
    <div className="glass-card p-4 sm:p-6">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-linear-to-br from-blue-500/20 to-indigo-500/20 flex items-center justify-center">
            <svg
              className="w-4 h-4 text-blue-400"
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
          <div>
            <h3 className="text-lg font-semibold text-white">Time Range</h3>
            <p className="text-white/70 text-sm">
              Showing {selectedCount} of {totalDataPoints} days
            </p>
          </div>
        </div>
        {!isFullRange && (
          <button
            onClick={handleReset}
            className="px-3 py-1.5 cursor-pointer text-xs font-medium text-white/70 hover:text-white bg-white/5 hover:bg-white/10 rounded-lg transition-all duration-200 flex items-center gap-1.5"
          >
            <svg
              className="w-3.5 h-3.5"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
              />
            </svg>
            Reset
          </button>
        )}
      </div>

      {/* Date range display */}
      <div className="flex items-center justify-between mb-4 text-sm">
        <div className="flex items-center gap-2">
          <span className="text-white/50">From:</span>
          <span className="text-blue-400 font-medium">{startLabel}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-white/50">To:</span>
          <span className="text-indigo-400 font-medium">{endLabel}</span>
        </div>
      </div>

      {/* Slider track */}
      <div className="relative pt-2 pb-1">
        <div
          ref={trackRef}
          className="relative h-2 bg-white/10 rounded-full cursor-pointer"
        >
          {/* Selected range fill */}
          <div
            className="absolute h-full bg-linear-to-r from-blue-500 to-indigo-500 rounded-full"
            style={{
              left: `${startPosition}%`,
              width: `${endPosition - startPosition}%`,
            }}
          />

          {/* Start handle */}
          <div
            className={`absolute top-1/2 -translate-y-1/2 -translate-x-1/2 w-5 h-5 rounded-full bg-blue-500 border-2 border-white shadow-lg cursor-grab transition-transform duration-100 hover:scale-110 ${
              isDragging === "start" ? "scale-125 cursor-grabbing" : ""
            }`}
            style={{ left: `${startPosition}%` }}
            onMouseDown={handleMouseDown("start")}
            onTouchStart={handleTouchStart("start")}
          >
            <div className="absolute inset-0 rounded-full bg-blue-400 animate-ping opacity-30" />
          </div>

          {/* End handle */}
          <div
            className={`absolute top-1/2 -translate-y-1/2 -translate-x-1/2 w-5 h-5 rounded-full bg-indigo-500 border-2 border-white shadow-lg cursor-grab transition-transform duration-100 hover:scale-110 ${
              isDragging === "end" ? "scale-125 cursor-grabbing" : ""
            }`}
            style={{ left: `${endPosition}%` }}
            onMouseDown={handleMouseDown("end")}
            onTouchStart={handleTouchStart("end")}
          >
            <div className="absolute inset-0 rounded-full bg-indigo-400 animate-ping opacity-30" />
          </div>
        </div>

        {/* Mini preview markers showing data density */}
        <div className="absolute inset-x-0 top-0 h-1.5 flex items-end pointer-events-none">
          {totalDataPoints > 0 &&
            Array.from({ length: Math.min(50, totalDataPoints) }).map(
              (_, i) => {
                const dataIndex = Math.floor(
                  (i / Math.min(50, totalDataPoints)) * totalDataPoints
                );
                const isInRange =
                  dataIndex >= startIndex && dataIndex <= endIndex;
                return (
                  <div
                    key={i}
                    className={`flex-1 mx-px rounded-t transition-all duration-200 ${
                      isInRange ? "bg-white/20 h-1" : "bg-white/5 h-0.5"
                    }`}
                  />
                );
              }
            )}
        </div>
      </div>

      {/* Quick range buttons */}
      <div className="flex flex-wrap gap-2 mt-4">
        {[
          { label: "Last 7 days", days: 7 },
          { label: "Last 30 days", days: 30 },
          { label: "Last 90 days", days: 90 },
          { label: "Last year", days: 365 },
          { label: "All time", days: totalDataPoints },
        ].map(({ label, days }) => {
          const isActive =
            days === totalDataPoints
              ? isFullRange
              : endIndex === totalDataPoints - 1 &&
                startIndex === Math.max(0, totalDataPoints - days);
          const isDisabled = days > totalDataPoints && days !== totalDataPoints;

          return (
            <button
              key={label}
              onClick={() => {
                if (days >= totalDataPoints) {
                  onRangeChange(0, totalDataPoints - 1);
                } else {
                  onRangeChange(
                    Math.max(0, totalDataPoints - days),
                    totalDataPoints - 1
                  );
                }
              }}
              disabled={isDisabled}
              className={`px-3 cursor-pointer py-1.5 text-xs font-medium rounded-lg transition-all duration-200 ${
                isActive
                  ? "bg-linear-to-r from-blue-500/30 to-indigo-500/30 text-white border border-blue-500/50"
                  : isDisabled
                  ? "bg-white/5 text-white/30 cursor-not-allowed"
                  : "bg-white/5 text-white/70 hover:bg-white/10 hover:text-white"
              }`}
            >
              {label}
            </button>
          );
        })}
      </div>
    </div>
  );
};

export default TimeRangeSelector;
