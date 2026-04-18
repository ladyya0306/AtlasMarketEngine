import contextlib
import datetime as _dt
import io
import json
import os
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner
from utils import llm_client


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _bucket_inventory_count(runner: SimulationRunner, conn: sqlite3.Connection, bucket_id: str, *, status: str = "for_sale") -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT pm.property_id, ps.zone, ps.is_school_district, ps.property_type, ps.building_area
        FROM properties_market pm
        JOIN properties_static ps ON ps.property_id = pm.property_id
        WHERE pm.status = ?
        """,
        (status,),
    )
    count = 0
    for row in cursor.fetchall() or []:
        row_dict = {
            "property_id": int(row[0] or 0),
            "zone": str(row[1] or ""),
            "is_school_district": bool(row[2]),
            "property_type": str(row[3] or ""),
            "building_area": float(row[4] or 0.0),
        }
        if runner._derive_supply_bucket_from_row(row_dict) == bucket_id:
            count += 1
    return count


def _month_bucket_shortage_rows(conn: sqlite3.Connection, month: int, bucket_id: str) -> int:
    cursor = conn.cursor()
    row = cursor.execute(
        """
        SELECT COUNT(*)
        FROM property_buyer_matches
        WHERE month = ?
          AND failure_reason = 'NO_ACTIVE_LISTINGS'
          AND COALESCE(json_extract(match_context, '$.shortlist_item.candidate_bucket_id'), '') = ?
        """,
        (int(month), str(bucket_id)),
    ).fetchone()
    return int((row or [0])[0] or 0)


def _force_round_one_review(original_review_fn):
    def _wrapped(month: int):
        actual = dict(original_review_fn(month) or {})
        if int(month) == 1:
            actual["transactions"] = max(1, int(actual.get("transactions", 0) or 0))
            actual["hot_buckets"] = actual.get("hot_buckets") or [
                {
                    "bucket_id": "A_NOSCHOOL_IMPROVE",
                    "label": "A区/非学区/改善",
                    "sold_props": 1,
                    "valid_bids": 1,
                    "negotiations": 1,
                    "real_comp_props": 1,
                }
            ]
            actual["shortage_buckets"] = [
                {
                    "bucket_id": "B_NOSCHOOL_JUST",
                    "label": "B区/非学区/刚需",
                    "no_active_rows": max(9, int(actual.get("no_active_listing_rows", 0) or 0), 9),
                    "waiting_buyers": 9,
                }
            ]
            actual["no_active_listing_rows"] = max(9, int(actual.get("no_active_listing_rows", 0) or 0))
            actual["signal_reasons"] = [
                f"NO_ACTIVE_LISTINGS 较高（{actual['no_active_listing_rows']}）",
                "热度集中于少数画像，缺口桶开始扩大",
            ]
            actual["should_pause"] = True
            return actual
        actual["should_pause"] = False
        actual["signal_reasons"] = []
        return actual

    return _wrapped


def main() -> int:
    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = _ensure_dir(
        PROJECT_ROOT / "results" / "release_round_panel_followthrough_smoke" / f"panel_followthrough_{timestamp}"
    )
    db_path = run_dir / "simulation.db"

    llm_client.LLM_MOCK_MODE = True
    os.environ["LLM_MOCK_MODE"] = "true"

    config = SimulationConfig("config/baseline.yaml")
    config.update("simulation.enable_intervention_panel", True)
    config.update("reporting.enable_end_reports", False)

    runner = SimulationRunner(
        agent_count=12,
        months=2,
        seed=42,
        config=config,
        db_path=str(db_path),
    )

    try:
        runner.initialize()
        target_bucket = "B_NOSCHOOL_JUST"
        inventory_before = _bucket_inventory_count(runner, runner.conn, target_bucket, status="for_sale")
        off_market_before = _bucket_inventory_count(runner, runner.conn, target_bucket, status="off_market")

        actions = [
            "2",   # 定向增供
            "",    # 默认缺口桶
            "",    # 默认数量
            "y",   # 确认
            "3",   # 自动补供
            "5",   # 减供或强制挂牌
            "2",   # 强制挂牌
            "",    # 默认缺口桶
            "",    # 不限制区域
            "",    # 默认数量
            "7",   # 继续
        ]

        original_review = runner._build_round_supply_review
        buffer = io.StringIO()
        with (
            contextlib.redirect_stdout(buffer),
            patch("builtins.input", side_effect=actions),
            patch.object(runner, "_build_round_supply_review", side_effect=_force_round_one_review(original_review)),
        ):
            runner.run(allow_intervention_panel=True)

        panel_output = buffer.getvalue()
        post_conn = sqlite3.connect(str(db_path))
        try:
            inventory_after = _bucket_inventory_count(runner, post_conn, target_bucket, status="for_sale")
            off_market_after = _bucket_inventory_count(runner, post_conn, target_bucket, status="off_market")
            round2_shortage = _month_bucket_shortage_rows(post_conn, 2, target_bucket)
        finally:
            post_conn.close()
        round2_tx = int(runner.last_month_summary.get("transactions", 0) or 0) if runner.last_month_summary else 0

        summary = {
            "target_bucket": target_bucket,
            "actions": actions,
            "inventory_before": inventory_before,
            "inventory_after": inventory_after,
            "off_market_before": off_market_before,
            "off_market_after": off_market_after,
            "round2_shortage_rows": round2_shortage,
            "round2_transactions": round2_tx,
            "current_month": int(runner.current_month),
            "status": runner.status,
            "intervention_history": runner.intervention_history,
            "last_month_summary": runner.last_month_summary,
        }

        (run_dir / "panel_cli.log").write_text(panel_output, encoding="utf-8")
        (run_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )

        lines = [
            "# Round Panel Follow-through Smoke",
            "",
            f"- 运行目录: `{run_dir}`",
            "- 口径: 回合=虚拟市场周期，不等同现实自然月。",
            f"- 目标画像桶: `{target_bucket}`",
            f"- 面板操作序列: `{actions}`",
            f"- 运行状态: `{runner.status}`",
            f"- 已完成回合: `{runner.current_month}`",
            "",
            "## 回合 1 面板前后",
            f"- 目标桶在售（面板前）: `{inventory_before}`",
            f"- 目标桶在售（跑完回合 2 后）: `{inventory_after}`",
            f"- 目标桶业主 off-market（面板前）: `{off_market_before}`",
            f"- 目标桶业主 off-market（跑完回合 2 后）: `{off_market_after}`",
            "",
            "## 回合 2 实际延续结果",
            f"- 回合 2 成交: `{round2_tx}`",
            f"- 回合 2 目标桶 NO_ACTIVE_LISTINGS: `{round2_shortage}`",
            "- 说明：这里验证的是‘回合 1 面板干预后，新增供给能否带入下一回合继续运行’，不是用这条 smoke 直接证明市场机制最优。",
            "",
            "## 干预事件",
        ]
        for item in runner.intervention_history:
            lines.append(
                f"- 回合 {item.get('month')} / {item.get('event_type')}: {item.get('summary') or item.get('message')}"
            )
        (run_dir / "smoke_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

        print(str(run_dir))
        return 0
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
