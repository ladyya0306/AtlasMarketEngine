#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
I1H 12轮封顶执行器（4段 × 3轮）

目标：
1) 固定同一组参数连续跑，避免无限试错。
2) 段间自动检查“是否应暂停修复”。
3) 产出统一的批次汇总，便于复盘与汇报。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class RunCheck:
    segment: int
    seed: int
    run_dir: str
    db_path: str
    status: str
    elapsed_seconds: float
    b_matches: int
    b_transactions: int
    batch_bidding_rows: int
    dominant_b_fail_reason: str
    dominant_b_fail_share: float
    no_batch_bidding: bool
    b_zero_tx_low_match: bool
    dominant_fail_high: bool


def _json_load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _count_batch_rows(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:
        row = cur.execute(
            "SELECT COUNT(*) FROM negotiation_round_book WHERE session_mode='batch_bidding'"
        ).fetchone()
        return int((row or [0])[0] or 0)
    except Exception:
        return 0
    finally:
        conn.close()


def _dominant_fail_reason(b_fail_top: Dict[str, Any]) -> tuple[str, float]:
    if not isinstance(b_fail_top, dict) or not b_fail_top:
        return ("", 0.0)
    pairs = []
    total = 0
    for k, v in b_fail_top.items():
        try:
            iv = int(v or 0)
        except Exception:
            iv = 0
        if iv > 0:
            pairs.append((str(k), iv))
            total += iv
    if total <= 0 or not pairs:
        return ("", 0.0)
    reason, count = max(pairs, key=lambda x: x[1])
    return reason, float(count) / float(total)


def _build_run_checks(segment: int, zone_chain: Dict[str, Any]) -> List[RunCheck]:
    checks: List[RunCheck] = []
    for run in zone_chain.get("runs", []):
        b_matches = int(((run.get("zone_matches") or {}).get("B", 0)) or 0)
        b_tx = int(((run.get("zone_transactions") or {}).get("B", 0)) or 0)
        db_path = Path(str(run.get("db_path", "")))
        batch_rows = _count_batch_rows(db_path)
        reason, share = _dominant_fail_reason(run.get("b_failure_reason_top") or {})
        checks.append(
            RunCheck(
                segment=segment,
                seed=int(run.get("seed", 0) or 0),
                run_dir=str(run.get("run_dir", "")),
                db_path=str(db_path),
                status=str(run.get("status", "")),
                elapsed_seconds=float(run.get("elapsed_seconds", 0.0) or 0.0),
                b_matches=b_matches,
                b_transactions=b_tx,
                batch_bidding_rows=batch_rows,
                dominant_b_fail_reason=reason,
                dominant_b_fail_share=round(share, 4),
                no_batch_bidding=(batch_rows <= 0),
                b_zero_tx_low_match=(b_tx == 0 and b_matches <= 5),
                dominant_fail_high=(bool(reason) and share >= 0.75),
            )
        )
    return checks


def _run_segment(segment: int, out_dir: Path, months_override: Optional[int]) -> int:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_research_experiments.py"),
        "--group",
        "I1H",
        "--seeds",
        "101",
        "202",
        "303",
        "--out-dir",
        str(out_dir),
    ]
    if months_override is not None:
        cmd.extend(["--months-override", str(int(months_override))])
    print(f"\n[segment {segment}] running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(ROOT))
    return int(proc.returncode)


def _should_stop(checks: List[RunCheck]) -> tuple[bool, str]:
    if len(checks) < 2:
        return (False, "")
    a, b = checks[-2], checks[-1]
    if a.no_batch_bidding and b.no_batch_bidding:
        return (True, "连续2轮无 batch_bidding，机制触发失效")
    if a.b_zero_tx_low_match and b.b_zero_tx_low_match:
        return (True, "连续2轮 B区0成交且低匹配，承接链路偏冷")
    if (
        a.dominant_fail_high
        and b.dominant_fail_high
        and a.dominant_b_fail_reason
        and b.dominant_b_fail_reason
        and a.dominant_b_fail_reason == b.dominant_b_fail_reason
    ):
        return (
            True,
            f"连续2轮同一失败主因占比过高：{a.dominant_b_fail_reason}",
        )
    return (False, "")


def _write_report(report_dir: Path, all_checks: List[RunCheck], stop_reason: str, finished_segments: int) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = report_dir / f"i1h_cap12_report_{stamp}.json"
    md_path = report_dir / f"i1h_cap12_report_{stamp}.md"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "finished_segments": int(finished_segments),
        "finished_rounds": int(len(all_checks)),
        "stop_reason": stop_reason,
        "runs": [asdict(x) for x in all_checks],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: List[str] = [
        "# I1H 12轮封顶执行报告",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 完成段数: {finished_segments} / 4",
        f"- 完成轮数: {len(all_checks)} / 12",
        f"- 停机原因: {stop_reason or '无（跑满/或执行中断）'}",
        "",
        "| 段 | Seed | 状态 | 耗时(秒) | B匹配 | B成交 | batch行数 | 主失败原因 | 占比 | 触发告警 |",
        "| :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |",
    ]

    for r in all_checks:
        alarms: List[str] = []
        if r.no_batch_bidding:
            alarms.append("无batch")
        if r.b_zero_tx_low_match:
            alarms.append("B冷")
        if r.dominant_fail_high:
            alarms.append("失败集中")
        lines.append(
            f"| {r.segment} | {r.seed} | {r.status} | {r.elapsed_seconds:.2f} | "
            f"{r.b_matches} | {r.b_transactions} | {r.batch_bidding_rows} | "
            f"{(r.dominant_b_fail_reason or '-')} | {r.dominant_b_fail_share:.2f} | "
            f"{','.join(alarms) if alarms else '-'} |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="I1H 12轮封顶执行器")
    parser.add_argument("--segments", type=int, default=4, help="分段数（默认4）")
    parser.add_argument("--rounds-per-segment", type=int, default=3, help="每段轮数（默认3）")
    parser.add_argument("--months-override", type=int, default=None, help="覆盖月数（可选）")
    parser.add_argument(
        "--base-out-dir",
        default=str(ROOT / "results" / "experiment_batches"),
        help="批次输出根目录",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.segments != 4 or args.rounds_per_segment != 3:
        print("[warn] 当前脚本按 4段×3轮 设计，参数已接收但不会改变 seeds 组装方式。")

    base_out = Path(args.base_out_dir).resolve()
    all_checks: List[RunCheck] = []
    stop_reason = ""
    finished_segments = 0

    for segment in range(1, 5):
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        seg_out = base_out / f"p1_fresh_research_i1h_cap12_seg{segment}_{stamp}"
        rc = _run_segment(segment=segment, out_dir=seg_out, months_override=args.months_override)
        if rc != 0:
            stop_reason = f"segment_{segment}_subprocess_failed(rc={rc})"
            finished_segments = segment - 1
            break

        zone_json = seg_out / "zone_chain_summary.json"
        if not zone_json.exists():
            stop_reason = f"segment_{segment}_missing_zone_chain_summary"
            finished_segments = segment
            break

        checks = _build_run_checks(segment=segment, zone_chain=_json_load(zone_json))
        all_checks.extend(checks)
        finished_segments = segment

        stop, reason = _should_stop(all_checks)
        if stop:
            stop_reason = reason
            print(f"[stop] {reason}")
            break

    report_md = _write_report(
        report_dir=base_out,
        all_checks=all_checks,
        stop_reason=stop_reason,
        finished_segments=finished_segments,
    )
    print(f"[done] report: {report_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

