DROP VIEW IF EXISTS user_stats;
CREATE VIEW user_stats AS
SELECT
    u.id,
    u.username,
    COUNT(o.id) AS order_count,
    COALESCE(SUM(o.total), 0) AS total_spent
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.username;
