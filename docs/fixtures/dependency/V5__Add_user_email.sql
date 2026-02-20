-- waypoint:depends V1
-- Only depends on V1 (users table), independent of V2/V3/V4
ALTER TABLE users ADD COLUMN email VARCHAR(255);
