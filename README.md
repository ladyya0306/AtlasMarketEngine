# AtlasMarketEngine

**A public Research Release for controlled housing-market simulation**

AtlasMarketEngine is a research-grade market simulation engine designed to reproduce three interpretable market states under a shared transaction chain:

- balanced market
- buyer market
- seller market

It also supports directional shock testing through a human-friendly CLI with resumable month-to-month state.

![Atlas onepager](./assets/atlas_onepager.svg)

## Why this repo exists

This repository is the **public layer** of the project.

It is intentionally narrower than the internal working tree:

- open enough to demonstrate real capability
- narrow enough to protect deeper governance rules, raw evidence packs, and commercial scenario layers

That means this repo keeps:

- the runnable core
- the public CLI entrypoint
- a small public evidence package
- visual assets and release-facing docs

And it does **not** keep:

- the full internal evidence library
- deeper governance parameter packs
- richer scenario templates for commercial use
- the more mature natural-activation research line

## What it shows

This release is meant to prove four things:

1. The system can reproduce balanced, buyer, and seller market setups under a shared chain.
2. The system can carry state across months instead of restarting from zero.
3. Researchers can control the experiment through explicit inputs rather than hand-editing configs.
4. Public readers can inspect a derived evidence package without receiving the full internal run archive.

## Quick visual tour

### Public validation snapshot

![Validation matrix](./assets/market_validation_matrix.svg)

### Scholar CLI showcase

![CLI showcase](./assets/cli_showcase.svg)

## What to read first

If you only have five minutes, read these in order:

1. [evidence/market_validation_summary_public.md](./evidence/market_validation_summary_public.md)
2. [docs/发布目录索引.md](./docs/发布目录索引.md)
3. [docs/Scholar_CLI_复现实验说明_20260412.md](./docs/Scholar_CLI_复现实验说明_20260412.md)
4. [real_estate_demo_v2_1.py](./real_estate_demo_v2_1.py)

## Public evidence policy

This repository uses a **derived-evidence-only** publication policy.

In plain language:

- the original internal run package is not published here
- the public repo keeps an aggregated evidence summary and visual proof layer
- this slightly reduces raw inspectability
- but it protects the deeper IP that would otherwise be exposed by full batch archives and internal governance documents

If you want the short answer to your question "does deleting the raw batch reduce credibility?", the honest answer is:

**yes, a little.**

But credibility is still preserved here because the public repo retains:

- a runnable CLI
- explicit experiment inputs
- a derived evidence summary
- visual validation artifacts

This is a deliberate tradeoff between trust and IP protection.

## Run it

The main entrypoint is:

- [real_estate_demo_v2_1.py](./real_estate_demo_v2_1.py)

The CLI supports:

- `New Simulation`
- `Resume Simulation`
- `Scholar Result Card`
- parameter-driven market setup

Install dependencies first:

```bash
pip install -r requirements.txt
python real_estate_demo_v2_1.py
```

## Repo layout

- [docs/发布目录索引.md](./docs/发布目录索引.md): public map of the repository
- [docs/Scholar_CLI_复现实验说明_20260412.md](./docs/Scholar_CLI_复现实验说明_20260412.md): how to reproduce public-facing runs through the CLI
- [evidence/market_validation_summary_public.md](./evidence/market_validation_summary_public.md): public derived evidence
- [assets/](./assets): one-pager and result visuals
- [config/](./config): minimal public configuration set
- [services/](./services): service layer
- [scripts/](./scripts): orchestration scripts

## Release boundary

This is **not** the full commercial stack.

It is a **Research Release**.

Natural activation remains a research-mode capability rather than the headline feature of this public repo.
