export interface GdeltDailyTone {
  id: number;
  date: string;
  average_tone: number | null;
  created_at: string;
}

export interface TrackFeaturesDailyAvg {
  id: number;
  date: string;
  avg_tempo: number | null;
  avg_duration: number | null;
  avg_energy: number | null;
  avg_danceability: number | null;
  avg_happiness: number | null;
  avg_acousticness: number | null;
  avg_instrumentalness: number | null;
  avg_liveness: number | null;
  avg_speechiness: number | null;
  avg_loudness_db: number | null;
  avg_popularity: number | null;
  track_count: number | null;
  reliability_score: number | null;
  created_at: string;
}

export interface ChartDataPoint {
  date: string;
  average_tone: number | null;
  avg_tempo?: number | null;
  avg_duration?: number | null;
  avg_energy?: number | null;
  avg_danceability?: number | null;
  avg_happiness?: number | null;
  avg_acousticness?: number | null;
  avg_instrumentalness?: number | null;
  avg_liveness?: number | null;
  avg_speechiness?: number | null;
  avg_loudness_db?: number | null;
  avg_popularity?: number | null;
}

export type MusicalFeature =
  | 'avg_tempo'
  | 'avg_duration'
  | 'avg_energy'
  | 'avg_danceability'
  | 'avg_happiness'
  | 'avg_acousticness'
  | 'avg_instrumentalness'
  | 'avg_liveness'
  | 'avg_speechiness'
  | 'avg_loudness_db'
  | 'avg_popularity';

export const MUSICAL_FEATURES: { key: MusicalFeature; label: string; color: string }[] = [
  { key: 'avg_energy', label: 'Energy', color: '#ef4444' },
  { key: 'avg_danceability', label: 'Danceability', color: '#f59e0b' },
  { key: 'avg_happiness', label: 'Happiness', color: '#eab308' },
  { key: 'avg_acousticness', label: 'Acousticness', color: '#84cc16' },
  { key: 'avg_instrumentalness', label: 'Instrumentalness', color: '#22c55e' },
  { key: 'avg_liveness', label: 'Liveness', color: '#14b8a6' },
  { key: 'avg_speechiness', label: 'Speechiness', color: '#06b6d4' },
  { key: 'avg_tempo', label: 'Tempo', color: '#3b82f6' },
  { key: 'avg_loudness_db', label: 'Loudness', color: '#8b5cf6' },
  { key: 'avg_popularity', label: 'Popularity', color: '#a855f7' },
  { key: 'avg_duration', label: 'Duration', color: '#ec4899' },
];
