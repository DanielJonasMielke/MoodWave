import { supabase } from "../../lib/supabase";
import DashboardChart from "../../components/DashboardChart";
import {
  GdeltDailyTone,
  TrackFeaturesDailyAvg,
  ChartDataPoint,
} from "../../types/data";

async function getData(): Promise<ChartDataPoint[]> {
  const [toneResult, featuresResult] = await Promise.all([
    supabase
      .from("gdelt_daily_tone")
      .select("*")
      .order("date", { ascending: true }),
    supabase
      .from("track_features_daily_avg")
      .select("*")
      .order("date", { ascending: true }),
  ]);

  if (toneResult.error) {
    console.error("Error fetching tone data:", toneResult.error);
    throw new Error("Failed to fetch tone data");
  }

  if (featuresResult.error) {
    console.error("Error fetching features data:", featuresResult.error);
    throw new Error("Failed to fetch features data");
  }

  const toneData = toneResult.data as GdeltDailyTone[];
  const featuresData = featuresResult.data as TrackFeaturesDailyAvg[];

  const featuresMap = new Map(featuresData.map((f) => [f.date, f]));

  const combinedData: ChartDataPoint[] = toneData
    .map((tone) => {
      const features = featuresMap.get(tone.date);
      return {
        date: tone.date,
        average_tone: tone.average_tone,
        ...(features && {
          avg_tempo: features.avg_tempo,
          avg_duration: features.avg_duration,
          avg_energy: features.avg_energy,
          avg_danceability: features.avg_danceability,
          avg_happiness: features.avg_happiness,
          avg_acousticness: features.avg_acousticness,
          avg_instrumentalness: features.avg_instrumentalness,
          avg_liveness: features.avg_liveness,
          avg_speechiness: features.avg_speechiness,
          avg_loudness_db: features.avg_loudness_db,
          avg_popularity: features.avg_popularity,
        }),
      };
    })
    .filter((d) => d.average_tone !== null);

  return combinedData;
}

export default async function Home() {
  const data = await getData();

  return (
    <>
      {/* Animated background layers */}
      <div className="gradient-bg" />
      <div className="wave-pattern" />

      {/* Floating decorative orbs */}
      <div className="floating-orb w-96 h-96 bg-purple-500/20 -top-48 -left-48 fixed" />
      <div
        className="floating-orb w-80 h-80 bg-cyan-500/15 top-1/3 -right-40 fixed"
        style={{ animationDelay: "-7s" }}
      />
      <div
        className="floating-orb w-64 h-64 bg-pink-500/10 bottom-20 left-1/4 fixed"
        style={{ animationDelay: "-14s" }}
      />

      <main className="relative min-h-screen z-10">
        <div className="container mx-auto px-4 py-16 max-w-7xl">
          {/* Header Section */}
          <header className="mb-16 text-center">
            <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white/5 border border-white/10 mb-6 backdrop-blur-sm">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse-slow" />
              <span className="text-white/60 font-medium">
                Live Data Stream
              </span>
            </div>

            <h1 className="text-6xl md:text-7xl font-bold mb-6 tracking-tight">
              <span className="glow-text">MoodWave</span>
            </h1>

            <p className="text-xl text-white/50 max-w-2xl mx-auto leading-relaxed">
              Discover the hidden connections between
              <span className="text-white/80 font-medium">
                {" "}
                global news sentiment{" "}
              </span>
              and
              <span className="text-white/80 font-medium"> musical trends</span>
            </p>

            {/* Decorative line */}
            <div className="mt-10 flex items-center justify-center gap-4">
              <div className="w-24 h-px bg-gradient-to-r from-transparent via-purple-500/50 to-transparent" />
              <div className="w-2 h-2 rounded-full bg-purple-500/50" />
              <div className="w-24 h-px bg-gradient-to-r from-transparent via-cyan-500/50 to-transparent" />
            </div>
          </header>

          <DashboardChart data={data} />

          {/* Footer */}
          <footer className="mt-20 text-center">
            <div className="inline-flex items-center gap-3 px-6 py-3 rounded-2xl bg-white/3 border border-white/5">
              <svg
                className="w-4 h-4 text-purple-400"
                fill="currentColor"
                viewBox="0 0 20 20"
              >
                <path
                  fillRule="evenodd"
                  d="M11.3 1.046A1 1 0 0112 2v5h4a1 1 0 01.82 1.573l-7 10A1 1 0 018 18v-5H4a1 1 0 01-.82-1.573l7-10a1 1 0 011.12-.38z"
                  clipRule="evenodd"
                />
              </svg>
              <p className="text-white/70 text-sm">
                Powered by GDELT news tones & Spotify chart analytics
              </p>
            </div>
          </footer>
        </div>
      </main>
    </>
  );
}
