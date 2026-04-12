CREATE TABLE IF NOT EXISTS property_listings (
    listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    listed_price REAL NOT NULL,
    min_price REAL NOT NULL,
    urgency REAL DEFAULT 0.5,
    status TEXT DEFAULT 'active',
    created_month INTEGER,
    FOREIGN KEY (property_id) REFERENCES properties (property_id),
    FOREIGN KEY (seller_id) REFERENCES agents (agent_id)
);
