CREATE TABLE IF NOT EXISTS agents (
    agent_id INTEGER PRIMARY KEY,
    name TEXT,
    age INTEGER,
    marital_status TEXT,
    monthly_income REAL,
    cash REAL,
    occupation TEXT,
    career_outlook TEXT,
    family_plan TEXT,
    education_need TEXT,
    housing_need TEXT,
    selling_motivation TEXT,
    background_story TEXT,
    profile_summary TEXT, -- LLM persona
    role TEXT DEFAULT 'OBSERVER',
    role_duration INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
