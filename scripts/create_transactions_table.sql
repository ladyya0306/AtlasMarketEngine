CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    month INTEGER,
    buyer_id INTEGER,
    seller_id INTEGER, -- NULL if System
    property_id INTEGER,
    price REAL,
    transaction_type TEXT, -- 'system_sale' | 'secondary_market'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (buyer_id) REFERENCES agents(agent_id),
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);
