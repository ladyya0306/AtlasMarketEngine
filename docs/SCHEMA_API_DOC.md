# Schema & API Documentation v2.1

## 1. Database Schema

### `properties` Table (New in v2.1)

The core registry for real estate assets.

| Field                    | Type       | Description                             |
| ------------------------ | ---------- | --------------------------------------- |
| `property_id`            | INTEGER PK | Unique identifier                       |
| `zone`                   | TEXT       | 'A' (CBD) or 'B' (Non-CBD)              |
| `quality`                | INTEGER    | 1 (Low), 2 (Medium), 3 (High)           |
| `base_value`             | REAL       | System-anchored value (e.g. 5M for A-2) |
| `owner_id`               | INTEGER    | Agent ID (NULL = System Owned)          |
| `status`                 | TEXT       | 'for_sale' or 'off_market'              |
| `listed_price`           | REAL       | Seller's asking price (Base Value ±30%) |
| `last_transaction_month` | INTEGER    | Month of last trade                     |
| `source_type`            | TEXT       | 'existing' (v2.2+ reserved)             |
| `project_id`             | INTEGER    | v2.2+ reserved                          |

### `base_value_config` Table

Configuration for initial property values.

| Field        | Type    | Description           |
| ------------ | ------- | --------------------- |
| `zone`       | TEXT    | Zone Identifier       |
| `quality`    | INTEGER | Quality Level         |
| `base_value` | REAL    | Standard Value in CNY |

### `market_parameters` Table (v2.2+ Policy)

Stores global market settings.

| Field            | Type    | Description           |
| ---------------- | ------- | --------------------- |
| `parameter_name` | TEXT PK | e.g. 'mortgage_rate'  |
| `current_value`  | REAL    | Current setting value |

______________________________________________________________________

## 2. Python API Interfaces

### `decision_engine.py`

#### `should_call_llm(agent, market, month) -> TriggerResult`

Determines if an agent requires complex LLM decision making.

**Triggers:**

1. **Life Event**: Marriage, child birth, job change.
1. **Financial Change**: Cash variance > 30% vs last month.
1. **Market Volatility**: Target zone price change > 10%.

**Returns:**

- `should_trigger`: bool
- `reason`: str
- `trigger_type`: 'life_event' | 'financial' | 'market'

### `monthly_simulator.py`

#### `run_monthly_decision(agent, market, month) -> dict`

Orchestrate the decision process.

- **Logic**: Calls `should_call_llm`.
  - If True: Executes `run_llm_decision_pipeline` (Stub).
  - If False: Returns fast-path `{"action": "WAIT"}`.

### `property_initializer.py`

#### `initialize_market_properties() -> List[Dict]`

Generates the initial state of the property market.

- **Rules**:
  - Zone A: 100 properties.
  - Zone B: 200 properties.
  - Prices: Base Value ±30% random float.
