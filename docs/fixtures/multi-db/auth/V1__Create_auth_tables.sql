CREATE TABLE IF NOT EXISTS auth_users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auth_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INTEGER NOT NULL REFERENCES auth_users(id),
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
