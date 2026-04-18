# 房产交易仿真系统改造手册

> 历史说明：本文档保留了项目从 OASIS 设计思路迁移到房地产仿真的阶段性记录。
> 当前项目定位：独立房地产交易与风险传导仿真系统。

______________________________________________________________________

## 🏗️ 1. 架构改造概览

项目早期参考了 OASIS 的多 Agent 设计思路，但当前主线已经围绕房地产交易域重构了**业务逻辑层**。

| 模块       | 历史参考能力         | 🏠 当前房产系统 (RealEstate-Sim)                     |
| :--------- | :-------------------- | :--------------------------------------------------- |
| **Agent**  | 社交用户 (Alice, Bob) | **买家 (Buyer)** / **卖家 (Seller)**                 |
| **Action** | 发帖, 点赞, 关注      | **挂牌 (List)**, **出价 (Offer)**, **成交 (Accept)** |
| **Trace**  | 社交行为记录          | **交易行为全流程记录**                               |
| **UI**     | 静态图表脚本          | **实时交互式 Web 看板 (Streamlit)**                  |

______________________________________________________________________

## 🛠️ 2. 核心代码变更清单

为了实现房产交易功能，我们修改了以下核心文件：

### A. 历史改造方向

以下内容记录的是早期设计对照，不代表当前仓库仍依赖 `oasis/...` 目录结构。

新增了 `ActionType` 枚举：

- `LIST_PROPERTY`: 挂牌房源
- `SEARCH_PROPERTY`: 搜索房源
- `MAKE_OFFER`: 发起报价（含价格和留言）
- `ACCEPT_OFFER`: 接受报价（达成交易）

### B. 前端工具函数 (`oasis/social_agent/agent_action.py`)

在 `SocialAction` 类中实现了上述动作的 `async` 方法，并注册到了 `get_openai_function_list` 中，使得 DeepSeek/OpenAI 模型可以自动发现并调用这些工具。

### C. 后端业务逻辑 (`oasis/social_platform/platform.py`)

在 `Platform` 类中实现了动作的具体处理逻辑：

- **list_property**: 将房源信息写入 `post` 表，并记录专用 Trace。
- **make_offer**: 将报价信息（JSON 格式）写入 `comment` 表，记录 Trace。
- **accept_offer**: 记录成交状态到 Trace 表。

______________________________________________________________________

## 🚀 3. 快速启动指南

### 步骤 1: 启动仿真 (生成数据)

运行 Stage 2 演示脚本，让 AI 智能体自主博弈：

```bash
# 激活你的 Python 环境
conda activate oasis

# 设置 API Key (如果脚本中未硬编码)
set DEEPSEEK_API_KEY=your_key_here

# 运行当前主入口
python simulation_runner.py
```

- **预期输出**：控制台显示买家和卖家的每一轮决策，最后以 `Exit code: 0` 结束。
- **产物**：生成当前运行数据库和日志文件。

### 步骤 2: 启动可视化看板 (查看结果)

使用 Streamlit 启动 Web 界面：

```bash
streamlit run real_estate_app.py
```

- **访问地址**：浏览器打开 `http://localhost:8501`
- **功能**：实时查看最新挂牌、出价动态，以及成交庆祝特效。

______________________________________________________________________

## 📊 4. 数据库结构说明

所有数据存储在 SQLite (`real_estate_stage2.db`) 中：

1. **post 表**：存储房源信息。
   - `content`: 房源描述
   - `user_id`: 卖家 ID
1. **comment 表**：存储出价记录。
   - `content`: JSON 格式的报价 `{type: OFFER, price: 505, msg: ...}`
   - `post_id`: 关联的房源 ID
1. **trace 表**：核心行为日志。
   - `action`: `list_property`, `make_offer`, `accept_offer`
   - `info`: 详细参数 (JSON)

______________________________________________________________________

## 🔮 5. 未来扩展建议

- **增加中介角色 (Agent)**：创建新的 `Agent` 类，以赚取佣金为目标。
- **多轮议价**：实现 `REJECT_OFFER` 和 `COUNTER_OFFER`（还价），增加博弈深度。
- **宏观调控**：在 `Platform` 的 `Clock` 中引入“市场热度”因子，影响买家的出价意愿。

______________________________________________________________________

*Created by Antigravity Agent for User*
