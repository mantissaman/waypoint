-- waypoint:depends V3
-- Reviews depend on orders (which already depends on users + products)
CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    created_at TIMESTAMP DEFAULT now()
);
