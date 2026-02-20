CREATE TABLE IF NOT EXISTS profiles (
    id SERIAL PRIMARY KEY,
    auth_user_id INTEGER NOT NULL,
    display_name VARCHAR(200),
    bio TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES profiles(id),
    title VARCHAR(300) NOT NULL,
    body TEXT,
    published BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT now()
);
