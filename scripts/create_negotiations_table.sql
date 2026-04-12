CREATE TABLE IF NOT EXISTS negotiations (
    negotiation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    property_id INTEGER NOT NULL,
    round_count INTEGER,
    final_price REAL,
    success BOOLEAN,
    log TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
