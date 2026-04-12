#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Build deterministic persona background library from historical simulation DBs.

This stage intentionally avoids LLM-based persona generation.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def infer_income_tier(monthly_income: float) -> str:
    income = float(monthly_income or 0.0)
    if income >= 50_000:
        return "ultra_high"
    if income >= 25_000:
        return "high"
    if income >= 12_000:
        return "middle"
    if income >= 7_000:
        return "lower_middle"
    return "low"


def _normalize_entry(row: Tuple[Any, ...]) -> Dict[str, Any]:
    occupation = str(row[0] or "").strip()
    background_story = str(row[1] or "").strip()
    purchase_motive_primary = str(row[2] or "").strip()
    investment_style = str(row[3] or "").strip()
    housing_stage = str(row[4] or "").strip()
    family_stage = str(row[5] or "").strip()
    education_path = str(row[6] or "").strip()
    financial_profile = str(row[7] or "").strip()
    seller_profile = str(row[8] or "").strip()
    monthly_income = float(row[9] or 0.0)
    return {
        "occupation": occupation,
        "background_story": background_story,
        "purchase_motive_primary": purchase_motive_primary,
        "investment_style": investment_style or "balanced",
        "housing_stage": housing_stage,
        "family_stage": family_stage,
        "education_path": education_path,
        "financial_profile": financial_profile,
        "seller_profile": seller_profile,
        "income_tier": infer_income_tier(monthly_income),
    }


def extract_entries_from_db(db_path: Path, row_limit: int = 5000) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        tables = {
            str(r[0]) for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "agents_static" not in tables:
            return []
        has_fin = "agents_finance" in tables
        sql = (
            """
            SELECT
                s.occupation,
                s.background_story,
                s.purchase_motive_primary,
                s.investment_style,
                s.housing_stage,
                s.family_stage,
                s.education_path,
                s.financial_profile,
                s.seller_profile,
                COALESCE(f.monthly_income, 0) AS monthly_income
            FROM agents_static s
            LEFT JOIN agents_finance f ON f.agent_id = s.agent_id
            WHERE (COALESCE(TRIM(s.occupation), '') <> '' OR COALESCE(TRIM(s.background_story), '') <> '')
            LIMIT ?
            """
            if has_fin
            else """
            SELECT
                s.occupation,
                s.background_story,
                s.purchase_motive_primary,
                s.investment_style,
                s.housing_stage,
                s.family_stage,
                s.education_path,
                s.financial_profile,
                s.seller_profile,
                0 AS monthly_income
            FROM agents_static s
            WHERE (COALESCE(TRIM(s.occupation), '') <> '' OR COALESCE(TRIM(s.background_story), '') <> '')
            LIMIT ?
            """
        )
        rows = cur.execute(sql, (int(row_limit),)).fetchall() or []
        return [_normalize_entry(row) for row in rows]
    except Exception:
        return []
    finally:
        conn.close()


def _iter_db_files(results_root: Path) -> Iterable[Path]:
    for db_path in sorted(results_root.rglob("*.db")):
        if db_path.name.lower().endswith(".db"):
            yield db_path


def build_library(
    results_root: Path,
    max_dbs: int = 300,
    row_limit_per_db: int = 5000,
    per_tier_limit: int = 800,
    global_limit: int = 3000,
) -> Dict[str, Any]:
    seen = set()
    all_entries: List[Dict[str, Any]] = []
    by_tier: Dict[str, List[Dict[str, Any]]] = {
        "ultra_high": [],
        "high": [],
        "middle": [],
        "lower_middle": [],
        "low": [],
    }
    scanned = 0
    accepted = 0
    for db_path in _iter_db_files(results_root):
        if scanned >= int(max_dbs):
            break
        scanned += 1
        rows = extract_entries_from_db(db_path, row_limit=row_limit_per_db)
        for item in rows:
            key = (
                item.get("occupation", ""),
                item.get("background_story", ""),
                item.get("purchase_motive_primary", ""),
                item.get("investment_style", ""),
                item.get("income_tier", ""),
            )
            if key in seen:
                continue
            seen.add(key)
            tier = str(item.get("income_tier", "low") or "low")
            bucket = by_tier.setdefault(tier, [])
            if len(bucket) < int(per_tier_limit):
                bucket.append(item)
            if len(all_entries) < int(global_limit):
                all_entries.append(item)
            accepted += 1
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_results_root": str(results_root.resolve()),
        "source_db_count_scanned": scanned,
        "source_entry_count_accepted": accepted,
        "entries": all_entries,
        "by_tier": by_tier,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build persona background library from historical DBs.")
    parser.add_argument("--results-root", default="results")
    parser.add_argument("--out-path", default="config/persona_background_library.json")
    parser.add_argument("--max-dbs", type=int, default=300)
    parser.add_argument("--row-limit-per-db", type=int, default=5000)
    parser.add_argument("--per-tier-limit", type=int, default=800)
    parser.add_argument("--global-limit", type=int, default=3000)
    args = parser.parse_args()

    results_root = Path(args.results_root).resolve()
    payload = build_library(
        results_root=results_root,
        max_dbs=int(args.max_dbs),
        row_limit_per_db=int(args.row_limit_per_db),
        per_tier_limit=int(args.per_tier_limit),
        global_limit=int(args.global_limit),
    )
    out_path = Path(args.out_path)
    if not out_path.is_absolute():
        out_path = (Path(__file__).resolve().parents[1] / out_path).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"background_library={out_path}")
    print(
        "scanned_db=%d accepted_entries=%d output_entries=%d"
        % (
            int(payload.get("source_db_count_scanned", 0)),
            int(payload.get("source_entry_count_accepted", 0)),
            len(payload.get("entries", []) or []),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
