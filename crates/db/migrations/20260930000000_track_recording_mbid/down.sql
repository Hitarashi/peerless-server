DROP INDEX IF EXISTS tracks_recording_mbid_idx;
ALTER TABLE tracks DROP COLUMN IF EXISTS recording_mbid;
