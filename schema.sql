PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS players;
DROP TABLE IF EXISTS clubs;

CREATE TABLE clubs (
  club_id INTEGER PRIMARY KEY AUTOINCREMENT,
  club_key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL
);

CREATE TABLE players (
  player_id INTEGER PRIMARY KEY AUTOINCREMENT,
  player_key TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  is_placeholder_visitor INTEGER NOT NULL DEFAULT 0,
  naspa_name TEXT,
  naspa_rating INTEGER,
  wgpo_name TEXT,
  wgpo_nwl_rating INTEGER,
  wgpo_wow_rating INTEGER,
  rating_notes TEXT
);

CREATE TABLE games (
  game_id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_date TEXT NOT NULL,
  club_id INTEGER NOT NULL,
  round_number INTEGER,
  player_id INTEGER NOT NULL,
  opponent_id INTEGER NOT NULL,
  player_score INTEGER NOT NULL,
  opponent_score INTEGER NOT NULL,
  spread INTEGER NOT NULL,
  result TEXT NOT NULL,
  visitor_note TEXT,
  raw_hash TEXT NOT NULL UNIQUE,
  FOREIGN KEY (club_id) REFERENCES clubs(club_id),
  FOREIGN KEY (player_id) REFERENCES players(player_id),
  FOREIGN KEY (opponent_id) REFERENCES players(player_id)
);
