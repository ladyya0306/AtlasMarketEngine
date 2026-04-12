-- Migration Script v2.1
-- Purpose: Create properties table (if missing) and update schema with v2.1 requirements

-- 1. Create developers table (v2.2+ placeholder)
CREATE TABLE IF NOT EXISTS developers (
    developer_id INTEGER PRIMARY KEY,
    name TEXT
);

-- 2. Create intermediaries table (v2.2+ placeholder)
CREATE TABLE IF NOT EXISTS intermediaries (
    intermediary_id INTEGER PRIMARY KEY,
    name TEXT
);

-- 3. Create properties table (v2.1 Schema)
CREATE TABLE IF NOT EXISTS properties (
    property_id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone TEXT NOT NULL,               -- 'A' | 'B'
    quality INTEGER NOT NULL,         -- 1 | 2 | 3
    base_value REAL NOT NULL,         -- System anchored value
    owner_id INTEGER,                 -- NULL = System owned
    status TEXT DEFAULT 'for_sale',   -- 'for_sale' | 'off_market'
    listed_price REAL,                -- Seller defined price
    last_transaction_month INTEGER,   -- Last transaction month
    source_type TEXT DEFAULT 'existing', -- v2.2+
    project_id INTEGER,               -- v2.2+
    intermediary_id INTEGER,          -- v2.2+
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (owner_id) REFERENCES agents(agent_id), -- Assuming agents table exists or will be created
    CHECK (zone IN ('A', 'B'))
);

-- 4. Create base_value_config table
CREATE TABLE IF NOT EXISTS base_value_config (
    zone TEXT NOT NULL,
    quality INTEGER NOT NULL,
    base_value REAL NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (zone, quality)
);

-- 5. Populate base_value_config
INSERT OR REPLACE INTO base_value_config (zone, quality, base_value) VALUES
('A', 1, 3500000),
('A', 2, 5000000),
('A', 3, 7000000),
('B', 1, 1800000),
('B', 2, 2500000),
('B', 3, 2900000);

-- 6. Create market_parameters table (v2.2+ Policy)
CREATE TABLE IF NOT EXISTS market_parameters (
    parameter_name TEXT PRIMARY KEY,
    current_value REAL,
    last_updated_month INTEGER,
    update_count INTEGER DEFAULT 0
);

-- 7. Initialize market parameters
INSERT OR IGNORE INTO market_parameters (parameter_name, current_value) VALUES
('mortgage_rate', 0.03),
('down_payment_ratio', 0.30),
('income_multiplier', 1.0),
('purchase_limit', 99);

-- 8. Create policy_events table (v2.2+ Policy)
CREATE TABLE IF NOT EXISTS policy_events (
    event_id INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    parameter_name TEXT,
    old_value REAL,
    new_value REAL,
    effective_month INTEGER,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
