-- waypoint:env dev
-- Debug columns only needed in dev
ALTER TABLE users ADD COLUMN debug_info TEXT;
ALTER TABLE users ADD COLUMN last_debug_at TIMESTAMP;
