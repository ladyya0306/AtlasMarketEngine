# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
import sqlite3


def _table_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def _ensure_column(cursor, table_name, column_name, column_type):
    cols = _table_columns(cursor, table_name)
    if column_name not in cols:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def init_db(db_path):
    """Initialize the database with V3 Schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Agents Static
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agents_static (
            agent_id INTEGER PRIMARY KEY,
            name TEXT,
            birth_year INTEGER,
            marital_status TEXT,
            children_ages TEXT,  -- JSON
            occupation TEXT,
            background_story TEXT,
            investment_style TEXT,
            purchase_motive_primary TEXT DEFAULT '',
            housing_stage TEXT DEFAULT '',
            family_stage TEXT DEFAULT '',
            education_path TEXT DEFAULT '',
            financial_profile TEXT DEFAULT '',
            seller_profile TEXT DEFAULT '',
            agent_type TEXT DEFAULT 'normal',
            info_delay_months INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 2. Agents Finance
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agents_finance (
            agent_id INTEGER PRIMARY KEY,
            monthly_income REAL,
            cash REAL,
            total_assets REAL,
            total_debt REAL,
            mortgage_monthly_payment REAL,
            net_cashflow REAL,              -- Missing Column Added
            max_affordable_price REAL,      -- Missing Column Added
            psychological_price REAL,       -- Missing Column Added
            payment_tolerance_ratio REAL DEFAULT 0.45,
            down_payment_tolerance_ratio REAL DEFAULT 0.30,
            last_price_update_month INTEGER, -- Missing Column Added
            last_price_update_reason TEXT,   -- Missing Column Added
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(agent_id) REFERENCES agents_static(agent_id)
        )
    """)

    # 3. Active Participants
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS active_participants (
            agent_id INTEGER,
            month INTEGER,
            role TEXT,
            agent_type TEXT DEFAULT 'normal',
            target_zone TEXT,
            max_price REAL,
            target_buy_price REAL,
            target_sell_price REAL,
            selling_property_id INTEGER,
            min_price REAL,
            listed_price REAL,
            life_pressure TEXT,
            activation_trigger TEXT DEFAULT '',
            school_urgency INTEGER DEFAULT 0,
            risk_mode TEXT DEFAULT 'balanced',
            max_wait_months INTEGER DEFAULT 6,
            waited_months INTEGER DEFAULT 0,
            cooldown_months INTEGER DEFAULT 0,
            consecutive_failures INTEGER DEFAULT 0,
            chain_mode TEXT,
            sell_completed INTEGER DEFAULT 0,
            buy_completed INTEGER DEFAULT 0,
            llm_intent_summary TEXT,
            activated_month INTEGER,
            role_duration INTEGER,
            PRIMARY KEY (agent_id, month),
            FOREIGN KEY(agent_id) REFERENCES agents_static(agent_id)
        )
    """)

    # 4. Properties Static
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties_static (
            property_id INTEGER PRIMARY KEY,
            zone TEXT,
            quality INTEGER,
            building_area REAL,
            property_type TEXT,
            is_school_district BOOLEAN,
            school_tier INTEGER,
            price_per_sqm REAL,
            zone_price_tier TEXT,
            initial_value REAL,
            build_year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 5. Properties Market
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties_market (
            property_id INTEGER PRIMARY KEY,
            owner_id INTEGER,
            status TEXT,
            current_valuation REAL,
            listed_price REAL,
            min_price REAL,
            rental_price REAL,  -- V3.2 Added
            rental_yield REAL,  -- V3.2 Added
            listing_month INTEGER,
            sell_deadline_month INTEGER,  -- V4.2 Added: soft/hard sale deadline month
            sell_deadline_total_months INTEGER,  -- V4.2 Added: planned listing horizon in months
            sell_urgency_score REAL,  -- V4.2 Added: normalized urgency [0,1]
            forced_sale_mode INTEGER DEFAULT 0,  -- V4.2 Added: 1 means deadline hard-clear enabled
            last_transaction_month INTEGER,
            last_price_update_month INTEGER, -- Fix: Added missing column
            last_price_update_reason TEXT,   -- Fix: Added missing column
            FOREIGN KEY(property_id) REFERENCES properties_static(property_id),
            FOREIGN KEY(owner_id) REFERENCES agents_static(agent_id)
        )
    """)

    # 6. Transactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER,
            order_id INTEGER,
            buyer_id INTEGER,
            seller_id INTEGER,
            property_id INTEGER,
            final_price REAL,
            down_payment REAL,  -- Fix: Added missing column
            loan_amount REAL,   -- Fix: Added missing column
            buyer_transaction_cost REAL DEFAULT 0,
            seller_transaction_cost REAL DEFAULT 0,
            negotiation_rounds INTEGER, -- Fix: Added missing column
            negotiation_mode TEXT,
            transaction_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(order_id) REFERENCES transaction_orders(order_id),
            FOREIGN KEY(buyer_id) REFERENCES agents_static(agent_id),
            FOREIGN KEY(seller_id) REFERENCES agents_static(agent_id),
            FOREIGN KEY(property_id) REFERENCES properties_static(property_id)
        )
    """)

    # 7. Negotiations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS negotiations (
            negotiation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            buyer_id INTEGER,
            seller_id INTEGER,
            property_id INTEGER,
            round_count INTEGER,
            final_price REAL,
            success BOOLEAN,
            reason TEXT, -- Fix: Added missing column
            log TEXT, -- JSON
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 7.5 Negotiation Round Book (mechanism observability)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS negotiation_round_book (
            round_id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER NOT NULL,
            property_id INTEGER NOT NULL,
            seller_id INTEGER,
            buyer_id INTEGER,
            candidate_buyer_count INTEGER DEFAULT 0,
            round_no INTEGER DEFAULT 0,
            party TEXT,
            action TEXT,
            quoted_price REAL,
            message TEXT,
            session_mode TEXT,
            session_outcome TEXT,
            session_reason TEXT,
            route_model TEXT,
            route_gray_score REAL,
            route_reason TEXT,
            llm_called BOOLEAN DEFAULT 0,
            raw_event_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 8. Decision Logs
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS decision_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER,
            month INTEGER,
            event_type TEXT,
            decision TEXT,
            reason TEXT,
            thought_process TEXT,
            context_metrics TEXT, -- JSON (New Phase 8)
            llm_called BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 9. Market Bulletin
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_bulletin (
            month INTEGER PRIMARY KEY,
            transaction_volume INTEGER,
            avg_price REAL,
            avg_unit_price REAL,
            orders_created INTEGER DEFAULT 0,
            orders_pending_settlement INTEGER DEFAULT 0,
            settlements_completed INTEGER DEFAULT 0,
            breaches_count INTEGER DEFAULT 0,
            breach_penalty_total REAL DEFAULT 0,
            avg_settlement_lag_months REAL DEFAULT 0,
            smart_match_total INTEGER DEFAULT 0,
            smart_match_selected INTEGER DEFAULT 0,
            smart_match_hit_rate REAL DEFAULT 0,
            avg_edu_weight_delta REAL DEFAULT 0,
            avg_price_sensitivity_delta REAL DEFAULT 0,
            m16_blocks_count INTEGER DEFAULT 0,
            m16_offer_clamp_count INTEGER DEFAULT 0,
            m16_sell_cap_count INTEGER DEFAULT 0,
            precheck_reject_count INTEGER DEFAULT 0,
            invalid_bid_count INTEGER DEFAULT 0,
            settlement_fail_affordability_count INTEGER DEFAULT 0,
            settlement_fail_dti_count INTEGER DEFAULT 0,
            settlement_fail_fee_count INTEGER DEFAULT 0,
            mortgage_watch_count INTEGER DEFAULT 0,
            mortgage_dpd30_count INTEGER DEFAULT 0,
            mortgage_dpd60_count INTEGER DEFAULT 0,
            mortgage_default_count INTEGER DEFAULT 0,
            forced_sale_count INTEGER DEFAULT 0,
            negative_equity_count INTEGER DEFAULT 0,
            npl_ratio REAL DEFAULT 0,
            zone_a_liquidity_index REAL DEFAULT 1.0,
            zone_b_liquidity_index REAL DEFAULT 1.0,
            price_change_pct REAL,
            zone_a_heat TEXT,
            zone_b_heat TEXT,
            trend_signal TEXT,
            consecutive_direction INTEGER,
            policy_news TEXT,
            llm_analysis TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 9.5 Bulletin Exposure Log (R2.1 observability)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bulletin_exposure_log (
            exposure_id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER NOT NULL,
            decision_month INTEGER NOT NULL,
            event_type TEXT DEFAULT 'ROLE_DECISION',
            role_decision TEXT,
            info_delay_months INTEGER DEFAULT 0,
            visible_bulletins INTEGER DEFAULT 0,
            seen_bulletin_month INTEGER DEFAULT 0,
            applied_lag_months INTEGER DEFAULT 0,
            market_trend_seen TEXT,
            bulletin_channel TEXT DEFAULT 'system_market_bulletin',
            llm_called BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(agent_id) REFERENCES agents_static(agent_id)
        )
    """)

    # 10. Agent End Reports (Phase 10)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agent_end_reports (
            report_id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER,
            simulation_run_id TEXT,
            identity_summary TEXT, -- JSON
            finance_summary TEXT, -- JSON
            transaction_summary TEXT, -- JSON
            imp_decision_log TEXT, -- JSON
            llm_portrait TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(agent_id) REFERENCES agents_static(agent_id)
        )
    """)

    # 11. Market Parameters (V2.2 Policy)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_parameters (
            parameter_name TEXT PRIMARY KEY,
            current_value REAL,
            last_updated_month INTEGER,
            update_count INTEGER DEFAULT 0
        )
    """)

    # 12. Policy Events (V2.2 Policy)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS policy_events (
            event_id INTEGER PRIMARY KEY,
            event_type TEXT NOT NULL,
            parameter_name TEXT,
            old_value REAL,
            new_value REAL,
            effective_month INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 13. Base Value Config
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS base_value_config (
            zone TEXT NOT NULL,
            quality INTEGER NOT NULL,
            base_value REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (zone, quality)
        )
    """)

    # Indices
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_properties_market_status ON properties_market(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_active_participants_role ON active_participants(role)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_month ON transactions(month)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_round_book_month ON negotiation_round_book(month)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_round_book_property ON negotiation_round_book(property_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bulletin_exposure_month ON bulletin_exposure_log(decision_month)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bulletin_exposure_agent ON bulletin_exposure_log(agent_id)")
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_order_id ON transactions(order_id)")
    except sqlite3.OperationalError:
        # Legacy DB may not have order_id yet; migration will add then create index.
        pass

    # 14. Property Buyer Matches (Phase 3.3)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS property_buyer_matches (
            match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER,
            property_id INTEGER,
            buyer_id INTEGER,
            listing_price REAL,
            buyer_bid REAL,
            is_valid_bid BOOLEAN,
            proceeded_to_negotiation BOOLEAN,
            order_id INTEGER,
            match_context TEXT,
            selection_reason TEXT,
            selected_in_shortlist BOOLEAN,
            final_outcome TEXT,
            failure_stage TEXT,
            failure_reason TEXT,
            final_price REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(order_id) REFERENCES transaction_orders(order_id),
            FOREIGN KEY(property_id) REFERENCES properties_static(property_id),
            FOREIGN KEY(buyer_id) REFERENCES agents_static(agent_id)
        )
    """)

    # 15. Transaction Orders (M18: Order Lifecycle)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transaction_orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_month INTEGER,
            expires_month INTEGER,
            settlement_due_month INTEGER,
            buyer_id INTEGER,
            seller_id INTEGER,
            property_id INTEGER,
            offer_price REAL,
            agreed_price REAL,
            negotiation_rounds INTEGER,
            deposit_amount REAL DEFAULT 0,
            penalty_amount REAL DEFAULT 0,
            status TEXT DEFAULT 'pending', -- pending/pending_settlement/filled/cancelled/expired/breached
            close_month INTEGER,
            close_reason TEXT,
            agent_type TEXT DEFAULT 'normal',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(buyer_id) REFERENCES agents_static(agent_id),
            FOREIGN KEY(property_id) REFERENCES properties_static(property_id)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaction_orders_status ON transaction_orders(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaction_orders_property ON transaction_orders(property_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaction_orders_buyer ON transaction_orders(buyer_id)")

    # 16. Mortgage Accounts (Market Pulse)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mortgage_accounts (
            mortgage_id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER NOT NULL,
            property_id INTEGER NOT NULL,
            original_loan_amount REAL NOT NULL,
            remaining_principal REAL NOT NULL,
            annual_interest_rate REAL NOT NULL,
            remaining_term_months INTEGER NOT NULL,
            monthly_payment REAL NOT NULL,
            start_month INTEGER DEFAULT 0,
            next_due_month INTEGER DEFAULT 1,
            last_payment_month INTEGER,
            missed_payments INTEGER DEFAULT 0,
            days_past_due INTEGER DEFAULT 0,
            delinquency_stage TEXT DEFAULT 'NORMAL',
            status TEXT DEFAULT 'active', -- active/defaulted/closed
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(agent_id) REFERENCES agents_static(agent_id),
            FOREIGN KEY(property_id) REFERENCES properties_static(property_id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mortgage_accounts_agent ON mortgage_accounts(agent_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mortgage_accounts_property ON mortgage_accounts(property_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mortgage_accounts_stage ON mortgage_accounts(delinquency_stage)")

    # V3: Developer Account table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS developer_account (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER UNIQUE,
            cash_balance REAL DEFAULT 0,
            month_revenue REAL DEFAULT 0,
            total_revenue REAL DEFAULT 0,
            total_invested INTEGER DEFAULT 0,
            total_sold INTEGER DEFAULT 0,
            unsold_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # V3: Initialize virtual developer agent (id=-1)
    cursor.execute("""
        INSERT OR IGNORE INTO agents_static 
        (agent_id, name, birth_year, marital_status, children_ages, 
         occupation, background_story, investment_style, agent_type)
        VALUES 
        (-1, '玩家开发商', 1990, 'N/A', '[]', 
         'Developer', '系统开发商账户，由玩家控制投放策略。', 'rational', 'system')
    """)

    cursor.execute("""
        INSERT OR IGNORE INTO agents_finance
        (agent_id, monthly_income, cash, total_assets, total_debt, 
         mortgage_monthly_payment, net_cashflow)
        VALUES
        (-1, 0, 999999999, 999999999, 0, 0, 0)
    """)

    conn.commit()
    conn.close()


def migrate_db_v2_7(db_path):
    """
    Migrate database to V2.7 (Ensure agent_end_reports exists and schema is up to date).
    Adds missing columns for legacy databases.
    """
    init_db(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    # active_participants (legacy DB may miss month)
    _ensure_column(cursor, "active_participants", "month", "INTEGER")
    _ensure_column(cursor, "active_participants", "activated_month", "INTEGER")
    _ensure_column(cursor, "active_participants", "role_duration", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "active_participants", "llm_intent_summary", "TEXT")
    _ensure_column(cursor, "active_participants", "agent_type", "TEXT DEFAULT 'normal'")
    _ensure_column(cursor, "active_participants", "target_buy_price", "REAL")
    _ensure_column(cursor, "active_participants", "target_sell_price", "REAL")
    _ensure_column(cursor, "active_participants", "risk_mode", "TEXT DEFAULT 'balanced'")
    _ensure_column(cursor, "active_participants", "max_wait_months", "INTEGER DEFAULT 6")
    _ensure_column(cursor, "active_participants", "waited_months", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "active_participants", "cooldown_months", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "active_participants", "consecutive_failures", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "active_participants", "chain_mode", "TEXT")
    _ensure_column(cursor, "active_participants", "sell_completed", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "active_participants", "buy_completed", "INTEGER DEFAULT 0")

    # agents_static
    _ensure_column(cursor, "agents_static", "agent_type", "TEXT DEFAULT 'normal'")
    _ensure_column(cursor, "agents_static", "info_delay_months", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "agents_static", "purchase_motive_primary", "TEXT DEFAULT ''")
    _ensure_column(cursor, "agents_static", "housing_stage", "TEXT DEFAULT ''")
    _ensure_column(cursor, "agents_static", "family_stage", "TEXT DEFAULT ''")
    _ensure_column(cursor, "agents_static", "education_path", "TEXT DEFAULT ''")
    _ensure_column(cursor, "agents_static", "financial_profile", "TEXT DEFAULT ''")
    _ensure_column(cursor, "agents_static", "seller_profile", "TEXT DEFAULT ''")

    # agents_finance
    _ensure_column(cursor, "agents_finance", "total_debt", "REAL DEFAULT 0")
    _ensure_column(cursor, "agents_finance", "mortgage_monthly_payment", "REAL DEFAULT 0")
    _ensure_column(cursor, "agents_finance", "net_cashflow", "REAL DEFAULT 0")
    _ensure_column(cursor, "agents_finance", "max_affordable_price", "REAL DEFAULT 0")
    _ensure_column(cursor, "agents_finance", "psychological_price", "REAL DEFAULT 0")
    _ensure_column(cursor, "agents_finance", "payment_tolerance_ratio", "REAL DEFAULT 0.45")
    _ensure_column(cursor, "agents_finance", "down_payment_tolerance_ratio", "REAL DEFAULT 0.30")
    _ensure_column(cursor, "agents_finance", "last_price_update_month", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "agents_finance", "last_price_update_reason", "TEXT DEFAULT ''")

    # active_participants
    _ensure_column(cursor, "active_participants", "activation_trigger", "TEXT DEFAULT ''")
    _ensure_column(cursor, "active_participants", "school_urgency", "INTEGER DEFAULT 0")

    # properties_market
    _ensure_column(cursor, "properties_market", "rental_price", "REAL")
    _ensure_column(cursor, "properties_market", "rental_yield", "REAL")
    _ensure_column(cursor, "properties_market", "last_price_update_month", "INTEGER")
    _ensure_column(cursor, "properties_market", "last_price_update_reason", "TEXT")
    _ensure_column(cursor, "properties_market", "sell_deadline_month", "INTEGER")
    _ensure_column(cursor, "properties_market", "sell_deadline_total_months", "INTEGER")
    _ensure_column(cursor, "properties_market", "sell_urgency_score", "REAL")
    _ensure_column(cursor, "properties_market", "forced_sale_mode", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "properties_static", "build_year", "INTEGER")

    # market_bulletin
    _ensure_column(cursor, "market_bulletin", "avg_unit_price", "REAL")
    _ensure_column(cursor, "market_bulletin", "transaction_volume", "INTEGER")
    _ensure_column(cursor, "market_bulletin", "orders_created", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "orders_pending_settlement", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "settlements_completed", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "breaches_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "breach_penalty_total", "REAL DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "avg_settlement_lag_months", "REAL DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "smart_match_total", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "smart_match_selected", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "smart_match_hit_rate", "REAL DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "avg_edu_weight_delta", "REAL DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "avg_price_sensitivity_delta", "REAL DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "m16_blocks_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "m16_offer_clamp_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "m16_sell_cap_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "precheck_reject_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "invalid_bid_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "settlement_fail_affordability_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "settlement_fail_dti_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "settlement_fail_fee_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "mortgage_watch_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "mortgage_dpd30_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "mortgage_dpd60_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "mortgage_default_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "forced_sale_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "negative_equity_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "npl_ratio", "REAL DEFAULT 0")
    _ensure_column(cursor, "market_bulletin", "zone_a_liquidity_index", "REAL DEFAULT 1.0")
    _ensure_column(cursor, "market_bulletin", "zone_b_liquidity_index", "REAL DEFAULT 1.0")
    _ensure_column(cursor, "market_bulletin", "policy_news", "TEXT")
    _ensure_column(cursor, "market_bulletin", "llm_analysis", "TEXT")

    # property_buyer_matches observability
    _ensure_column(cursor, "property_buyer_matches", "order_id", "INTEGER")
    _ensure_column(cursor, "property_buyer_matches", "match_context", "TEXT")
    _ensure_column(cursor, "property_buyer_matches", "selection_reason", "TEXT")
    _ensure_column(cursor, "property_buyer_matches", "selected_in_shortlist", "BOOLEAN")
    _ensure_column(cursor, "property_buyer_matches", "final_outcome", "TEXT")
    _ensure_column(cursor, "property_buyer_matches", "failure_stage", "TEXT")
    _ensure_column(cursor, "property_buyer_matches", "failure_reason", "TEXT")
    _ensure_column(cursor, "property_buyer_matches", "final_price", "REAL")

    # negotiation_round_book observability
    _ensure_column(cursor, "negotiation_round_book", "month", "INTEGER")
    _ensure_column(cursor, "negotiation_round_book", "property_id", "INTEGER")
    _ensure_column(cursor, "negotiation_round_book", "seller_id", "INTEGER")
    _ensure_column(cursor, "negotiation_round_book", "buyer_id", "INTEGER")
    _ensure_column(cursor, "negotiation_round_book", "candidate_buyer_count", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "negotiation_round_book", "round_no", "INTEGER DEFAULT 0")
    _ensure_column(cursor, "negotiation_round_book", "party", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "action", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "quoted_price", "REAL")
    _ensure_column(cursor, "negotiation_round_book", "message", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "session_mode", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "session_outcome", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "session_reason", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "route_model", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "route_gray_score", "REAL")
    _ensure_column(cursor, "negotiation_round_book", "route_reason", "TEXT")
    _ensure_column(cursor, "negotiation_round_book", "llm_called", "BOOLEAN DEFAULT 0")
    _ensure_column(cursor, "negotiation_round_book", "raw_event_json", "TEXT")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_round_book_month ON negotiation_round_book(month)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_round_book_property ON negotiation_round_book(property_id)")

    # transactions
    _ensure_column(cursor, "transactions", "order_id", "INTEGER")
    _ensure_column(cursor, "transactions", "down_payment", "REAL")
    _ensure_column(cursor, "transactions", "loan_amount", "REAL")
    _ensure_column(cursor, "transactions", "buyer_transaction_cost", "REAL DEFAULT 0")
    _ensure_column(cursor, "transactions", "seller_transaction_cost", "REAL DEFAULT 0")
    _ensure_column(cursor, "transactions", "negotiation_rounds", "INTEGER")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_order_id ON transactions(order_id)")

    # transaction_orders
    _ensure_column(cursor, "transaction_orders", "settlement_due_month", "INTEGER")
    _ensure_column(cursor, "transaction_orders", "agreed_price", "REAL")
    _ensure_column(cursor, "transaction_orders", "negotiation_rounds", "INTEGER")
    _ensure_column(cursor, "transaction_orders", "prequal_cash", "REAL")
    _ensure_column(cursor, "transaction_orders", "prequal_total_debt", "REAL")
    _ensure_column(cursor, "transaction_orders", "prequal_owned_property_count", "INTEGER")

    conn.commit()
    conn.close()
