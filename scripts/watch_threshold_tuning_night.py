#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
阈值收敛夜跑守望脚本（最小稳健版）

能力：
1. 定时检测 threshold_tuning 批次是否完成（T1/T2 的 batch_summary.json 都存在）。
2. 完成后自动执行分组分析（zone_chain_summary）。
3. 可选执行低风险自动修复：
   - 检测 market_bulletin 月份缺口并自动回填（仅改 DB，不改业务代码）。
4. 生成一份自动总结报告，便于醒来后直接查看。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
RESULTS_BATCH_ROOT = ROOT / "results" / "experiment_batches"


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def pick_latest_threshold_batch() -> Path:
    candidates = [p for p in RESULTS_BATCH_ROOT.glob("threshold_tuning_*") if p.is_dir()]
    if not candidates:
        raise FileNotFoundError("未找到 threshold_tuning_* 批次目录。")
    candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0]


def done(batch_dir: Path) -> bool:
    return (batch_dir / "T1" / "batch_summary.json").exists() and (batch_dir / "T2" / "batch_summary.json").exists()


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def run_cmd(args: List[str]) -> None:
    proc = subprocess.run(args, cwd=str(ROOT), text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(args)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )


def analyze_group(group_dir: Path) -> None:
    run_cmd([sys.executable, "scripts/analyze_experiment_batch.py", str(group_dir)])


def _max_month(conn: sqlite3.Connection, table: str, col: str = "month") -> int:
    cur = conn.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {table}")
    row = cur.fetchone()
    return int((row[0] if row else 0) or 0)


def detect_bulletin_gap(db_path: Path) -> tuple[int, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        tx_max = _max_month(conn, "transactions")
        ap_max = _max_month(conn, "active_participants")
        mb_max = _max_month(conn, "market_bulletin")
        fact_max = max(tx_max, ap_max)
        return fact_max, mb_max
    finally:
        conn.close()


def maybe_backfill_bulletin(db_path: Path) -> str:
    fact_max, mb_max = detect_bulletin_gap(db_path)
    if fact_max <= 0:
        return f"skip: no fact month, db={db_path}"
    if mb_max >= fact_max:
        return f"ok: bulletin complete ({mb_max}/{fact_max}), db={db_path}"
    run_cmd(
        [
            sys.executable,
            "scripts/backfill_market_bulletin.py",
            "--db-path",
            str(db_path),
            "--from-month",
            "1",
            "--to-month",
            str(fact_max),
        ]
    )
    return f"fixed: bulletin backfilled to {fact_max}, db={db_path}"


def build_auto_report(batch_dir: Path, fix_log: List[str]) -> Path:
    t1 = load_json(batch_dir / "T1" / "batch_summary.json")
    t2 = load_json(batch_dir / "T2" / "batch_summary.json")
    report_path = batch_dir / "auto_watch_report.md"

    def section(payload: Dict[str, Any], name: str) -> List[str]:
        runs = payload.get("runs", [])
        avg_elapsed = 0.0
        if runs:
            avg_elapsed = round(sum(float(r.get("elapsed_seconds", 0)) for r in runs) / len(runs), 2)
        lines = [
            f"## {name}",
            f"- 总轮数: {payload.get('run_count', 0)}",
            f"- 成功轮数: {payload.get('success_count', 0)}",
            f"- 失败轮数: {payload.get('failed_count', 0)}",
            f"- 平均耗时(秒): {avg_elapsed}",
            "",
        ]
        return lines

    lines: List[str] = [
        "# 阈值收敛夜跑自动报告",
        "",
        f"- 批次目录: {batch_dir}",
        f"- 生成时间: {now_text()}",
        "",
        *section(t1, "T1 (buyer=0.45, negotiation=0.40)"),
        *section(t2, "T2 (buyer=0.40, negotiation=0.40)"),
        "## 低风险自动修复记录",
        *(["- " + x for x in fix_log] if fix_log else ["- 无"]),
        "",
        "## 产物",
        f"- {batch_dir / 'T1' / 'zone_chain_summary.md'}",
        f"- {batch_dir / 'T2' / 'zone_chain_summary.md'}",
        f"- {batch_dir / 'T1' / 'batch_summary.md'}",
        f"- {batch_dir / 'T2' / 'batch_summary.md'}",
        "",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="守望 threshold_tuning 夜跑并自动分析")
    parser.add_argument("--batch-dir", default="", help="指定批次目录；为空时自动选最新 threshold_tuning_*")
    parser.add_argument("--poll-seconds", type=int, default=180, help="轮询间隔秒数，默认 180")
    parser.add_argument("--timeout-hours", type=float, default=16.0, help="超时小时数，默认 16")
    parser.add_argument("--auto-fix-safe", action="store_true", help="启用低风险自动修复（仅数据回填）")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    batch_dir = Path(args.batch_dir).resolve() if args.batch_dir else pick_latest_threshold_batch().resolve()
    if not batch_dir.exists():
        raise FileNotFoundError(f"批次目录不存在: {batch_dir}")

    print(f"[{now_text()}] watch start: {batch_dir}", flush=True)
    timeout_at = time.time() + float(args.timeout_hours) * 3600.0

    while time.time() < timeout_at:
        if done(batch_dir):
            print(f"[{now_text()}] batch done, start analyze", flush=True)
            analyze_group(batch_dir / "T1")
            analyze_group(batch_dir / "T2")

            fix_log: List[str] = []
            if args.auto_fix_safe:
                for sub in ("T1", "T2"):
                    payload = load_json(batch_dir / sub / "batch_summary.json")
                    for run in payload.get("runs", []):
                        db_path = Path(str(run.get("db_path", "")))
                        if db_path.exists():
                            try:
                                fix_log.append(maybe_backfill_bulletin(db_path))
                            except Exception as exc:  # pragma: no cover
                                fix_log.append(f"failed: backfill error={exc} db={db_path}")

            report_path = build_auto_report(batch_dir, fix_log)
            print(f"[{now_text()}] done. report={report_path}", flush=True)
            return 0

        print(f"[{now_text()}] waiting... next check in {args.poll_seconds}s", flush=True)
        time.sleep(max(30, int(args.poll_seconds)))

    print(f"[{now_text()}] timeout reached: {args.timeout_hours}h", flush=True)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

