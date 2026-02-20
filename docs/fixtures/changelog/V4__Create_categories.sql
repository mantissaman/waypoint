CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    parent_id INTEGER REFERENCES categories(id)
);

CREATE INDEX CONCURRENTLY idx_categories_parent ON categories (parent_id);
