# Scholar CLI 复现实验说明

更新时间：2026-04-12

---

## 0. 这份文档是干什么的

这份文档专门回答三个问题：

1. `real_estate_demo_v2_1.py` 里的 Scholar CLI，到底要用户填哪些参数。  
2. 如果要对照 `forced_role_batch_20260412_m3_reconfirm` 这批正式结果，用户应该怎么输入才能逐组复现。  
3. 每一组测试要看什么、预期是什么、跑完去哪里找证据。  

白话讲：  
这不是代码文档，而是“真人怎么照着输、怎么复现实验、怎么查证据”的操作说明。

---

## 1. Scholar CLI 的入口在哪里

启动脚本：

- [real_estate_demo_v2_1.py](/D:/GitProj/visual_real_estate/real_estate_demo_v2_1.py)

启动后主菜单中，最重要的是这两个入口：

1. `新建实验（推荐，真人友好引导版）`
2. `继续实验（继承上月状态后续跑）`

如果只是做正式复现或对外演示，默认使用第 1 个入口即可。  
第 3 个“高级研究员配置版”主要留给更细粒度调参，不是今天的复现实验主入口。

---

## 2. Scholar CLI 需要用户填写哪些内容

### 2.1 新建实验时的输入项

当前 Scholar CLI 会让用户填写下面这些关键参数。

1. `Random Seed`
   - 含义：随机种子，用于复现。  
   - 典型值：`606 / 607 / 608`。  
   - 不填或填 `random` 就不可复现。

2. `market goal`
   - 可选值：
     - `balanced`
     - `buyer_market`
     - `seller_market`
   - 含义：
     - `balanced` 对应平衡环境
     - `buyer_market` 对应买方环境
     - `seller_market` 对应卖方环境

3. `Simulation months`
   - 含义：本次模拟跑多少个月。  
   - 正式重确认批次这里填 `3`。

4. `Total agents`
   - 含义：Agent 总人数。  
   - 正式重确认批次这里填 `50`。

5. `Total properties`
   - 含义：初始化时生成的总房源数。  
   - 当前 Scholar CLI 复现实验建议填 `60`。  
   - 这是因为当前引导模式默认按 `1.2 × Agent 数` 推一个稳定值。

6. `Forced BUYER quota`
   - 含义：强制进入 `BUYER` 角色的人数。  
   - 这不是强制成交，只是强制角色结构。

7. `Forced SELLER quota`
   - 含义：强制进入 `SELLER` 角色的人数。

8. `Forced BUYER_SELLER quota`
   - 含义：强制进入 `BUYER_SELLER` 角色的人数。  
   - 这些人负责体现置换链、先卖后买、跨月延续。

9. `Target R_order hint`
   - 含义：研究员给系统的目标订单压力提示值。  
   - 它不是最终真实结果，而是引导系统估算：
     - 初始在售 `L0`
     - `initial_listing_rate`
   - 理解方式：
     - `< 1`：偏买方
     - `≈ 1`：偏平衡
     - `> 1`：偏卖方

10. `Income multiplier`
    - 含义：收入倍率。  
    - `1.00` 表示不改。  
    - `1.18` 表示整体收入上浮 18%。

11. `Forced role active months`
    - 含义：强制角色模式连续作用多少个月。  
    - 如果本轮要跑 3 个月，正式复现实验就填 `3`。

12. `profiled_market_mode`
    - 含义：是否启用画像供需模式。  
    - 正式复现实验建议 `y`。

13. `hard_bucket_matcher`
    - 含义：是否启用硬 bucket 匹配器。  
    - 正式复现实验建议 `y`。

14. `enable_intervention_panel`
    - 含义：是否在每月结束时打开人工干预面板。  
    - 正式复现实验建议 `n`，避免人为插手。

15. `open_startup_intervention_menu`
    - 含义：是否在启动前打开一次人工干预菜单。  
    - 正式复现实验建议 `n`。

---

## 3. 对照 forced_role_batch_20260412_m3_reconfirm，一共有多少场测试

对照批次：

- [forced_role_batch_20260412_m3_reconfirm](/D:/GitProj/visual_real_estate/results/line_b_forced_role/forced_role_batch_20260412_m3_reconfirm)

这批正式结果一共 `9` 场测试，结构是：

1. `V1 × 606 / 607 / 608`
2. `V2 × 606 / 607 / 608`
3. `V3 × 606 / 607 / 608`

也就是：

1. 平衡环境 `3` 场
2. 买方环境 `3` 场
3. 卖方环境 `3` 场

每一场都是：

1. `3个月`
2. `50 Agent`
3. 强制角色模式
4. 画像供需模式启用
5. 硬 bucket 启用

---

## 4. 这 9 场测试分别是什么

### 4.1 V1：平衡环境组

目标：  
验证系统能否在中性供需压力下形成有效链路，并保持平衡附近的市场表现。

对应种子：

1. `V1_s606_m3_a50`
2. `V1_s607_m3_a50`
3. `V1_s608_m3_a50`

预期：

1. 链路有效  
2. 不要求极热，也不要求极冷  
3. 更像锚点组

### 4.2 V2：买方环境组

目标：  
验证系统能否在供给相对宽松、订单压力较低时，形成偏买方的市场结构。

对应种子：

1. `V2_s606_m3_a50`
2. `V2_s607_m3_a50`
3. `V2_s608_m3_a50`

预期：

1. 库存相对更宽松  
2. 订单压力低于平衡组  
3. 竞争不应明显过热

### 4.3 V3：卖方环境组

目标：  
验证系统能否在需求压力更集中时，形成偏卖方的结构表现。

对应种子：

1. `V3_s606_m3_a50`
2. `V3_s607_m3_a50`
3. `V3_s608_m3_a50`

预期：

1. 需求压力更集中  
2. 竞争更容易出现  
3. 热点房源更容易被重复争夺

---

## 5. 如何用 Scholar CLI 复现这 9 场测试

### 5.1 共通固定输入

无论复现哪一组，下面这些值都固定：

1. `Simulation months = 3`
2. `Total agents = 50`
3. `Total properties = 60`
4. `Forced role active months = 3`
5. `profiled_market_mode = y`
6. `hard_bucket_matcher = y`
7. `enable_intervention_panel = n`
8. `open_startup_intervention_menu = n`

### 5.2 V1 复现输入

适用种子：`606 / 607 / 608`

输入建议：

1. `market goal = balanced`
2. `Random Seed = 606` 或 `607` 或 `608`
3. `Target R_order hint = 1.00`
4. `Income multiplier = 1.00`
5. 配额：
   - `BUYER = 8`
   - `SELLER = 8`
   - `BUYER_SELLER = 4`

预期目标：

1. 这是平衡锚点组  
2. 重点看链路是否有效、结构是否平衡附近  
3. 不要求像卖方组那样更热

### 5.3 V2 复现输入

适用种子：`606 / 607 / 608`

输入建议：

1. `market goal = buyer_market`
2. `Random Seed = 606` 或 `607` 或 `608`
3. `Target R_order hint = 0.70`
4. `Income multiplier = 0.98`
5. 配额：
   - `BUYER = 4`
   - `SELLER = 12`
   - `BUYER_SELLER = 2`

预期目标：

1. 买方环境  
2. 供给侧相对更宽松  
3. 竞争不应像卖方组那样集中

### 5.4 V3 复现输入

适用种子：`606 / 607 / 608`

输入建议：

1. `market goal = seller_market`
2. `Random Seed = 606` 或 `607` 或 `608`
3. `Target R_order hint = 1.30`
4. `Income multiplier = 1.18`
5. 配额：
   - `BUYER = 12`
   - `SELLER = 4`
   - `BUYER_SELLER = 6`

预期目标：

1. 卖方环境  
2. 需求压力更容易转成竞争  
3. 热门房源更容易被多买家争夺

---

## 6. 用户输入以后，这些参数会作用到哪里

Scholar CLI 不只是把参数打印出来，而是会写进本次 run 的 `config.yaml`。

重点写入位置包括：

1. `simulation.months`
2. `simulation.agent_count`
3. `simulation.random_seed`
4. `simulation.agent.income_adjustment_rate`
5. `user_property_count`
6. `market.initial_listing_rate`
7. `smart_agent.forced_role_mode.*`
8. `smart_agent.profiled_market_mode.*`
9. `simulation.scholar_cli.*`

白话解释：

1. 你输入的不是备注；
2. 也不是只写给人看；
3. 它会真实改变本次 run 的市场初始化和角色注入。

---

## 7. 跑完后去哪里看结果

每次从 Scholar CLI 新建实验后，结果默认落在：

- `results/runs/run_YYYYMMDD_HHMMSS/`

里面最重要的文件有：

1. `config.yaml`
   - 本次实际生效的参数

2. `simulation.db`
   - 最重要的数据库证据

3. `simulation_run.log`
   - 运行长日志

4. `metadata.json`
   - 运行级元信息

5. `scholar_result_card.md`
   - 面向人看的结果卡

6. `scholar_result_card.json`
   - 结果卡对应的 JSON

7. `parameter_assumption_report.md`
   - 参数与假设说明

---

## 8. 如何从结果倒推输入是否正确

如果想判断自己这轮输入有没有真的生效，先查这几项：

1. 看 `config.yaml`
   - `forced_role_mode.quota` 是否等于你输入的配额
   - `income_adjustment_rate` 是否等于你输入的收入倍率
   - `simulation.months` 是否等于你输入的月数

2. 看 `metadata.json`
   - `seed` 是否正确
   - `db_path` 是否对应本轮 run

3. 看 `scholar_result_card.md`
   - 目标市场标签是否正确
   - `target_r_order_hint` 是否正确
   - 估算 `L0` 和 `initial_listing_rate` 是否有写出来

---

## 9. Resume 怎么用

如果已经跑完一轮，后续想继续跑，不需要重开新实验。

直接在主菜单里选：

`继续实验（继承上月状态后续跑）`

这个模式会先显示“续跑前状态摘要卡”，告诉你：

1. 已完成到第几个月
2. 累计成交多少
3. 上月成交多少
4. 当前库存多少
5. 当前活跃参与者多少
6. 历史研究标签和历史目标 R_order 提示值

然后你再输入未来几个月的新参数。  
它的意义不是重开一局，而是在原数据库状态上继续往后跑。

---

## 10. 推荐的演示顺序

如果是对外演示，我建议按这个顺序：

1. 先选 `新建实验（推荐，真人友好引导版）`
2. 先跑一个 `V3 seller_market` 样本
3. 跑完后展示：
   - `scholar_result_card.md`
   - `config.yaml`
   - `simulation.db`
4. 再进入 `继续实验`
5. 先展示“续跑前状态摘要卡”
6. 再决定是否续跑 1-3 个月

白话解释：  
这样领导先看到“怎么设”，再看到“跑完是什么”，最后再看到“系统真的能继承状态继续往后走”，整条链路最完整。
