CREATE TABLE IF NOT EXISTS accepted_mismatches (
  mismatch_key TEXT PRIMARY KEY,
  accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  note TEXT
);

ALTER TABLE games ADD COLUMN verification_status TEXT NOT NULL DEFAULT 'VERIFIED';
ALTER TABLE games ADD COLUMN mismatch_key TEXT;
ALTER TABLE games ADD COLUMN mismatch_type TEXT;
