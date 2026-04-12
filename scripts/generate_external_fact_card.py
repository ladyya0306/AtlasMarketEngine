#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate a concise external-fact alignment card from a macro-matrix output dir.

Usage:
  python scripts/generate_external_fact_card.py --matrix-dir <out_dir>
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional


def _extract_db_from_stdout(stdout_path: Path, project_root: Path) -> Optional[Path]:
    if not stdout_path.exists():
        return None
    lines = stdout_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    db_line = None
    for ln in reversed(lines):
        if "DB path:" in ln:
            db_line = ln.split("DB path:", 1)[1].strip()
            break
    if not db_line:
        return None
    db_norm = db_line.replace("\\", "/")
    p = Path(db_norm)
    if not p.is_absolute():
        p = project_root / db_norm
    return p


def _metrics_from_db(db_path: Path) -> Dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    den = cur.execute(
        """
        select count(*)
        from transaction_orders o
        join properties_static p on p.property_id=o.property_id
        where upper(p.zone)='B'
        """
    ).fetchone()[0]
    num = cur.execute(
        """
        select count(*)
        from transaction_orders o
        join properties_static p on p.property_id=o.property_id
        where upper(p.zone)='B'
          and o.status in ('filled','pending_settlement')
        """
    ).fetchone()[0]
    dti = cur.execute(
        "select count(*) from transaction_orders where lower(coalesce(close_reason,'')) like '%dti exceeded%'"
    ).fetchone()[0]
    unavailable = cur.execute(
        "select count(*) from transaction_orders where lower(coalesce(close_reason,'')) like '%property unavailable%'"
    ).fetchone()[0]
    outbid = cur.execute(
        "select count(*) from transaction_orders where lower(coalesce(close_reason,'')) like 'outbid:%'"
    ).fetchone()[0]
    filled = cur.execute("select count(*) from transaction_orders where status='filled'").fetchone()[0]
    pending = cur.execute("select count(*) from transaction_orders where status='pending_settlement'").fetchone()[0]
    conn.close()
    return {
        "db_path": str(db_path),
        "b_ratio_num": int(num),
        "b_ratio_den": int(den),
        "b_ratio": (float(num) / float(den)) if den else None,
        "dti_exceeded": int(dti),
        "property_unavailable": int(unavailable),
        "outbid_orders": int(outbid),
        "filled_orders": int(filled),
        "pending_settlement_orders": int(pending),
    }


def _scenario_from_dirname(name: str) -> str:
    m = re.match(r"^(.*?)_s\d+_m\d+$", name)
    return m.group(1) if m else name


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate external-fact alignment card from matrix results.")
    ap.add_argument("--matrix-dir", required=True, help="Matrix output directory containing *_s*_m* folders.")
    args = ap.parse_args()

    matrix_dir = Path(args.matrix_dir).resolve()
    if not matrix_dir.exists():
        raise FileNotFoundError(f"Matrix dir not found: {matrix_dir}")

    project_root = Path(__file__).resolve().parents[1]
    scenario_metrics: Dict[str, Dict[str, Any]] = {}

    for child in sorted([x for x in matrix_dir.iterdir() if x.is_dir()]):
        scenario = _scenario_from_dirname(child.name)
        stdout_path = child / "stdout.log"
        db_path = _extract_db_from_stdout(stdout_path, project_root)
        if not db_path or not db_path.exists():
            continue
        scenario_metrics[scenario] = _metrics_from_db(db_path)

    if not scenario_metrics:
        raise RuntimeError("No scenario metrics resolved. Check stdout.log and DB paths.")

    out_json = matrix_dir / "external_fact_alignment_card.json"
    out_md = matrix_dir / "external_fact_alignment_card.md"

    payload = {
        "matrix_dir": str(matrix_dir),
        "scenarios": scenario_metrics,
        "method_note": "Mechanism-oriented alignment only; no point forecast.",
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("# 外部事实对照卡（自动生成）")
    lines.append("")
    lines.append("边界声明：只做机制同向验证，不做价格点位预测。")
    lines.append("")
    lines.append("## 场景指标")
    for sc in sorted(scenario_metrics.keys()):
        m = scenario_metrics[sc]
        b_ratio = "N/A" if m["b_ratio"] is None else f'{m["b_ratio"]:.4f}'
        lines.append(f"- {sc}: B区成交比={m['b_ratio_num']}/{m['b_ratio_den']} ({b_ratio}), "
                     f"DTI失败={m['dti_exceeded']}, property unavailable={m['property_unavailable']}, "
                     f"outbid={m['outbid_orders']}, filled={m['filled_orders']}, pending={m['pending_settlement_orders']}")
    lines.append("")
    lines.append("## 核心叙事映射（待研究员复核）")
    lines.append("1. 下行期（S-）中，B区承接与成交是否仍可维持（同向/反向/不确定）。")
    lines.append("2. 上行期（S+）中，卖方强势与竞争挤出是否增强（同向/反向/不确定）。")
    lines.append("3. 平稳转折（N0）中，成交与回流闭环是否维持稳定（同向/反向/不确定）。")
    lines.append("")
    lines.append("## 证据路径")
    for sc in sorted(scenario_metrics.keys()):
        lines.append(f"- {sc} DB: `{scenario_metrics[sc]['db_path']}`")

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"json={out_json}")
    print(f"md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

