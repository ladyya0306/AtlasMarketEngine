# Public Market Validation Summary

This public repository only keeps **derived evidence**.

It does **not** publish raw internal run databases, raw log bundles, or full private experiment packs.

## What has been verified

### 1. The market can complete a 6-round clean baseline run

Meaning:

- no preplanned external interventions
- full 6-round completion
- checkpoints generated
- meaningful transaction volume still forms

Public-facing interpretation:

> The engine is not only runnable when manually “propped up”. It can produce a full market cycle on its own.

### 2. Supply intervention mainly helps the late rounds stay tradable

The main effect of intervention is **not** simply maximizing total transaction count.

Its main value is reducing the late-round situation where:

- buyers still want to buy
- but cannot find suitable active listings

Public-facing interpretation:

> The intervention panel is valuable because it reduces late-round supply-demand mismatch, not because it magically inflates total sales.

### 3. Seller-leaning markets do produce real local bidding pressure

In `seller_market`:

- more properties experience multi-buyer competition
- more buyers lose after being outbid
- hotspot properties close closer to list price, and some close above list price

But average transaction price may still fail to rise above the natural baseline because:

- more mainstream, lower-priced homes are sold
- the sales mix shifts downward

Public-facing interpretation:

> Seller strength in this engine is real, but it appears as local hotspot pressure rather than uniform price inflation across all homes.

## Where to read more

- `/docs/中国住房市场推演发布收口摘要_20260418_通俗版.md`
- `/docs/发布说明_20260418.md`
- `/docs/卖方市场局部竞价证据_20260418.md`
- `/docs/发布证据包索引_20260418.md`
- `/docs/公开发布政策_20260418.md`
