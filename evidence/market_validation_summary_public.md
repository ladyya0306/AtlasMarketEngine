# Public Evidence Summary

这份公开版证据摘要来自内部正式批次 `forced_role_batch_20260412_m3_reconfirm` 的派生结果。
原始整包运行证据不在公开仓库中保留，公开版只保留可以建立信任但不会暴露完整方法论深度的摘要。

## Batch Snapshot
- 运行总数: `9`
- 通过闸门: `9`
- 未通过闸门: `0`
- 月数: `3`

## Group Snapshot
- `V1`: 3/3 PASS, long_test_ready=True
- `V2`: 3/3 PASS, long_test_ready=True
- `V3`: 3/3 PASS, long_test_ready=True

## Public Sample Cases
- `V1_s606_m3_a50`: total_tx=12, m3_tx=6, avg_price=4238842, inventory_now=33, active_now=35
- `V2_s606_m3_a50`: total_tx=13, m3_tx=10, avg_price=4124498, inventory_now=30, active_now=35
- `V3_s606_m3_a50`: total_tx=15, m3_tx=8, avg_price=3893170, inventory_now=29, active_now=33

## Volume Trend by Market Group

- `V1`:
  - month 1: `0`
  - month 2: `13`
  - month 3: `17`
- `V2`:
  - month 1: `0`
  - month 2: `8`
  - month 3: `18`
- `V3`:
  - month 1: `0`
  - month 2: `24`
  - month 3: `15`

公开版解读：

- 三组样本在月 1 都没有形成最终成交，说明正式成交并不是“激活后立刻发生”，而是存在明显的链路展开和结算时滞。
- `V3` 在月 2 出现最强放量，说明卖方环境的需求压力更早转化成真实成交。
- `V2` 在月 3 放量更明显，说明买方环境并不是“没有成交”，而是更晚转入成交兑现。

## A/B Zone Average Transaction Price Trend

- `V1`:
  - month 2: `A=4,448,514`, `B=NA`
  - month 3: `A=4,215,704`, `B=1,318,204`
- `V2`:
  - month 2: `A=4,029,467`, `B=NA`
  - month 3: `A=4,291,101`, `B=1,227,693`
- `V3`:
  - month 2: `A=4,078,087`, `B=1,561,090`
  - month 3: `A=4,117,956`, `B=1,535,019`

公开版解读：

- `A` 区始终显著高于 `B` 区，说明高价值核心区和外围区的价格层级在成交结果中被保留了下来。
- `V3` 的 `B` 区在月 2、月 3 都有成交，说明卖方环境下不仅核心区被争夺，外围区也开始承接需求外溢。
- `V2` 的 `B` 区成交更少，说明买方环境里需求更容易停留在高匹配度房源，不会同样快速外溢到外围区。
