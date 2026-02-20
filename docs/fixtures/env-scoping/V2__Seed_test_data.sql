-- waypoint:env dev,test
-- This migration only runs in dev and test environments
INSERT INTO users (username, email) VALUES
    ('testuser1', 'test1@example.com'),
    ('testuser2', 'test2@example.com'),
    ('testuser3', 'test3@example.com');
