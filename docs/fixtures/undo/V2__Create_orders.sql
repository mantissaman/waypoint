CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    total DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
