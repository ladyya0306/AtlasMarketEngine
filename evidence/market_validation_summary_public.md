# Public Evidence Summary

This public summary keeps the **derived evidence layer** of the project.

It does **not** publish the full internal run archive or every raw database pack. Instead, it keeps the parts that help an external reader understand:

1. what the system is proving
2. what results have already been observed
3. why the current release is considered publishable

---

## 中文版

### 当前公开版到底证明了什么

当前公开版重点证明四件事：

1. 系统可以稳定拉开三类市场方向：
   - 平衡市场
   - 买方市场
   - 卖方市场
2. 系统能在固定供给下维持多回合状态，而不是每回合从零重来
3. 系统能区分局部真实竞价和广谱平均价格变化
4. 人类可以通过清晰的回合末干预入口，影响后半段供给续航

### 当前已经完成的关键验证

#### 1. 发布入口可运行

已经完成：

1. CLI 启动验证
2. Web 启动验证
3. 真实模型小样验证
4. 回合末面板验证
5. 正式演示彩排

这说明当前公开链路不是“只能内部跑”，而是已经具备对外演示条件。

#### 2. 长测和对照测试已跑到足够收口

当前已完成的正式方向测试包括：

1. 自然基准长测
2. clean baseline
3. 样本量桥接
4. 供给结构敏感性
5. 需求压力方向对照
6. 轻量冲击韧性对照

### 当前最值得记住的几个结果

#### 卖方市场并不等于“所有房子一起暴涨”

当前最重要的公开解释之一是：

1. 在 `seller_market` 条件下，局部热点房源的多人竞价确实更强
2. 但平均总成交价不一定比自然基准更高
3. 原因不是机制失效，而是：
   - 更多低总价主流盘被卖掉了
   - 成交结构下沉，抵消了局部热点房源的抬价

所以更值得看的指标是：

1. 成交价相对挂牌价
2. `Outbid` 总数
3. 找不到合适在售房的次数
4. 成交结构落在什么桶位

#### 供给结构真的会改变后半段市场形状

在同样的中样本量下：

1. 梭子型供给盘后半段更容易出现主流可交易层过早变薄
2. 金字塔型供给盘后半段更容易保留一层缓冲库存

这意味着：

1. 总库存差不多，不代表后半段流动性差不多
2. 主流可交易桶位的厚度，会直接改变后半段薄市化速度

#### 供给干预的价值不只是“多卖几套”

当前公开版已经明确：

1. 加不加供给干预，总成交未必差很多
2. 但干预能明显缓和后半段“有人想买，但没对口在售房”的情况

所以玩家回合末干预的价值，主要是：

**缓和供需错位，而不是单纯堆高成交总量。**

---

## English

### What the current public release is proving

The current release is built to prove four things:

1. the engine can reproduce balanced, buyer, and seller market directions
2. it can preserve state across multiple rounds instead of restarting from zero
3. it can separate local real competition from broad average-price movement
4. a human operator can intervene at round-end with a clear explanation layer

### Key validations already completed

The public path has already covered:

1. CLI startup validation
2. Web startup validation
3. live-model probe
4. round-end intervention panel validation
5. formal demo rehearsal

This means the release is not just “internally runnable.” It is already presentation-ready.

### Comparison runs already completed

The current public evidence includes:

1. natural baseline long run
2. clean baseline
3. sample-size bridge
4. supply-structure sensitivity
5. demand-pressure direction tests
6. light-shock resilience test

### Three public-facing takeaways

#### 1. A seller market does not mean every property rises together

One of the most important findings is:

1. hotspot properties do show stronger local multi-bid competition
2. but the average total transaction price may still fail to exceed the natural baseline
3. not because the mechanism failed, but because more low-priced mainstream units were sold

So the best public indicators are:

1. transaction-to-list ratio
2. outbid counts
3. no-active-listing counts
4. transaction mix by market bucket

#### 2. Supply structure really changes the late-stage market shape

Under similar medium-size runs:

1. spindle-shaped supply is more likely to thin out earlier in mainstream tradable layers
2. pyramid-shaped supply tends to keep a thicker late-stage buffer

This means:

1. similar total inventory does not imply similar late-stage liquidity
2. the thickness of mainstream tradable buckets directly changes how quickly the market becomes thin

#### 3. Supply intervention is valuable mainly because it reduces mismatch

The public release already shows:

1. total transaction count may not change dramatically
2. but intervention clearly reduces late-stage cases where buyers still exist but matching listings do not

So the value of round-end intervention is not just “sell more homes.”

It is:

**reducing late-stage supply-demand mismatch.**
