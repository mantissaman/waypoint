ALTER TABLE users ADD COLUMN api_key UUID DEFAULT gen_random_uuid();
