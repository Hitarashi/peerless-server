ALTER TABLE tracks ADD COLUMN IF NOT EXISTS recording_mbid VARCHAR(36);
CREATE INDEX IF NOT EXISTS tracks_recording_mbid_idx ON tracks (recording_mbid);
