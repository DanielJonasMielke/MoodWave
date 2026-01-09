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
    <div className="flex flex-col gap-2">
      {MUSICAL_FEATURES.map(({ key, label, color }) => (
        <button
          key={key}
          onClick={() => setSelectedFeature(key)}
          className={`text-sm feature-btn cursor-pointer text-left ${
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
          <span className="relative z-10 flex items-center gap-3">
            <span
              className="w-2 h-2 rounded-full shrink-0 transition-all duration-200"
              style={{
                backgroundColor: selectedFeature === key ? "white" : color,
                boxShadow:
                  selectedFeature === key ? "none" : `0 0 8px ${color}50`,
              }}
            />
            <span className="truncate">{label}</span>
          </span>
        </button>
      ))}
    </div>
  );
};

export default FeatureSelection;
