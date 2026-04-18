# Oasis V3 研究员实验手册 (Experiment Guide)

本手册专为使用 Oasis 系统进行房地产市场仿真实验的研究员设计。

## 🔬 实验架构 (Architecture)

Oasis V3 采用了 **"事件驱动 (Event-Driven)"** 的仿真架构，特别适合研究极端市场条件下的群体行为。

### 核心机制
1.  **宏观环境 (Macro Environment)**: 通过 `baseline.yaml` 定义基础经济参数（如基础利率、首付比例）。
2.  **市场脉冲 (Market Pulse)**: 系统会自动检测市场异动（如开发商降价 >10%），触发 **全量 Agent 唤醒**，模拟羊群效应。
3.  **干预接口 (Intervention)**: 允许在运行时注入外部冲击（如供给冲击、收入冲击）。

---

## ⚙️ 参数配置 (Configuration)

核心配置文件位于 `config/baseline.yaml`。

### 1. 经济参数 (Macro)
```yaml
market:
  risk_free_rate: 0.02      # 无风险利率 (2%)
  mortgage_rate: 0.035      # 房贷利率 (3.5%) - 直接影响月供压力
  down_payment_ratio: 0.30  # 首付比例 (30%)
```

### 2. 区域参数 (Zones)
```yaml
zones:
  A:
    base_price_per_sqm: 35000  # A区基准单价
    rental_yield: 0.015        # 租售比 (低租售比会抑制投资需求)
```

### 3. Agent 分布 (Demographics)
虽然 `real_estate_demo_v2_1.py` 提供了交互式配置，但您也可以直接修改代码中的 `default_income_bounds` 来调整收入分层。

---

## 🧪 运行批量实验 (Batch Experiments)

使用 `scripts/run_batch_experiments.py` 可以自动化运行多组对照实验。

### 步骤
1.  **定义实验组**: 在脚本中修改 `configs` 列表。
    ```python
    configs = [
        {"name": "Low_Rate", "mortgage_rate": 0.02},  # 降息实验
        {"name": "High_Supply", "supply_factor": 1.5} # 增供实验
    ]
    ```
2.  **运行脚本**:
    ```bash
    python scripts/run_batch_experiments.py
    ```
3.  **分析结果**: 结果将保存在 `experiments/` 目录下，包含详细的 CSV 和日志。

---

## 📊 数据分析 (Analysis)

仿真结果存储在 SQLite 数据库 (`simulation.db`) 中。

### 关键数据表
- **`transactions`**: 所有成交记录 (成交价、买家ID、房产ID)。
- **`decision_logs`**: Agent 的思考过程 (LLM Thought Process)。
    - **提示**: 筛选 `decision='BUY'` 的记录，查看 `reason` 字段，分析买家入市动机。
- **`market_bulletin`**: 每月的市场宏观数据 (均价、成交量、趋势信号)。

### 验证 "脉冲效应"
要验证市场脉冲是否生效，请查询 `decision_logs`：
```sql
SELECT month, COUNT(*) FROM decision_logs GROUP BY month;
```
如果看到某个月份的记录数激增（例如从 20 激增到 100），说明该月触发了 **Market Pulse**，大量潜在买家被唤醒。
