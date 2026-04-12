CREATE TABLE IF NOT EXISTS decision_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id INTEGER,
    month INTEGER,
    event_type TEXT,
    decision TEXT,
    reason TEXT,
    thought_process TEXT,
    llm_called BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (agent_id) REFERENCES agents (agent_id)
);
