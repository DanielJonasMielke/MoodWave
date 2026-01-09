import React, { Dispatch, SetStateAction } from "react";
import { MUSICAL_FEATURES, MusicalFeature } from "../../types/data";

interface FeatureSelectionProps {
  selectedFeature: MusicalFeature;
  setSelectedFeature: Dispatch<SetStateAction<MusicalFeature>>;
}

const FeatureSelection = ({
  selectedFeature,
  setSelectedFeature,
}: FeatureSelectionProps) => {
  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
      {MUSICAL_FEATURES.map(({ key, label, color }) => (
        <button
          key={key}
          onClick={() => setSelectedFeature(key)}
          className={`feature-btn cursor-pointer ${
            selectedFeature === key ? "active" : ""
          }`}
          style={
            selectedFeature === key
              ? ({
                  background: `linear-gradient(135deg, ${color}, ${color}dd)`,
                  "--btn-color": `${color}66`,
                } as React.CSSProperties)
              : {}
          }
        >
          <span className="relative z-10 flex items-center justify-center gap-2">
            {selectedFeature === key && (
              <span className="w-1.5 h-1.5 rounded-full bg-white animate-pulse" />
            )}
            {label}
          </span>
        </button>
      ))}
    </div>
  );
};

export default FeatureSelection;
