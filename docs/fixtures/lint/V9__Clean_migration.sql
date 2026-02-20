CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    action VARCHAR(100) NOT NULL DEFAULT 'unknown',
    created_at TIMESTAMP DEFAULT now()
);
