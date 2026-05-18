CREATE TABLE IF NOT EXISTS player_ratings (
  player_id INTEGER PRIMARY KEY,
  naspa_rating INTEGER,
  wgpo_rating INTEGER,
  wgpo_wow_rating INTEGER,
  cross_tables_rating INTEGER,
  naspa_url TEXT,
  wgpo_url TEXT,
  cross_tables_url TEXT,
  rating_source_notes TEXT,
  ratings_updated_at TEXT NOT NULL,
  FOREIGN KEY (player_id) REFERENCES players(player_id)
);
