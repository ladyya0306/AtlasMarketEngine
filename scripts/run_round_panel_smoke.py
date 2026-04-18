import contextlib
import datetime as _dt
import io
import json
import os
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


def _seed_round_review_facts(runner: SimulationRunner, round_no: int) -> None:
    cursor = runner.conn.cursor()

    # Keep a few live listings around so the cold bucket section has real content.
    cursor.execute(
        """
        UPDATE properties_market
        SET status='for_sale',
            listing_month=?,
            listed_price=1850000,
            min_price=1710000,
            current_valuation=1830000
        WHERE property_id IN (1, 2, 3)
        """,
        (round_no,),
    )
    cursor.execute(
        """
        UPDATE properties_market
        SET status='off_market',
            owner_id=1,
            current_valuation=1920000,
            listing_month=NULL,
            listed_price=NULL,
            min_price=NULL
        WHERE property_id = 4
        """
    )

    hot_context = {
        "shortlist_item": {
            "candidate_bucket_id": "A_NOSCHOOL_IMPROVE",
            "heat_state": {"real_competition_score": 2.0},
        }
    }
    shortage_context = {
        "shortlist_item": {
            "candidate_bucket_id": "B_NOSCHOOL_JUST",
            "heat_state": {"real_competition_score": 0.0},
        }
    }

    cursor.execute(
        """
        INSERT INTO property_buyer_matches
        (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid,
         proceeded_to_negotiation, order_id, final_outcome, match_context, failure_reason)
        VALUES (?, 1, 9001, 1850000, 1835000, 1, 1, 5001, 'FILLED', ?, NULL)
        """,
        (round_no, json.dumps(hot_context, ensure_ascii=False)),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (month, order_id, buyer_id, seller_id, property_id, final_price, down_payment,
         loan_amount, negotiation_rounds, negotiation_mode, transaction_type)
        VALUES (?, 5001, 9001, 1, 1, 1835000, 550500, 1284500, 3, 'standard', 'resale')
        """,
        (round_no,),
    )

    shortage_rows = []
    for idx in range(9):
        shortage_rows.append(
            (
                round_no,
                200 + idx,
                9100 + idx,
                980000,
                0,
                0,
                0,
                None,
                "SHORTLIST_ONLY",
                json.dumps(shortage_context, ensure_ascii=False),
                "NO_ACTIVE_LISTINGS",
            )
        )
    cursor.executemany(
        """
        INSERT INTO property_buyer_matches
        (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid,
         proceeded_to_negotiation, order_id, final_outcome, match_context, failure_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        shortage_rows,
    )
    runner.conn.commit()


def main() -> int:
    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = _ensure_dir(
        Path("D:/GitProj/visual_real_estate/results/release_round_panel_smoke")
        / f"panel_smoke_{timestamp}"
    )
    db_path = run_dir / "simulation.db"

    llm_client.LLM_MOCK_MODE = True
    os.environ["LLM_MOCK_MODE"] = "true"

    config = SimulationConfig("config/baseline.yaml")
    config.update("simulation.enable_intervention_panel", True)

    runner = SimulationRunner(
        agent_count=6,
        months=1,
        seed=42,
        config=config,
        db_path=str(db_path),
    )

    try:
        runner.initialize()
        review_round = 1
        _seed_round_review_facts(runner, review_round)
        review_before = runner._build_round_supply_review(review_round)

        actions = [
            "1",   # 查看回合复盘
            "2",   # 定向增供
            "",    # 选择默认缺口桶
            "",    # 使用默认数量
            "y",   # 确认执行
            "3",   # 自动补供
            "5",   # 减供或强制挂牌
            "2",   # 强制挂牌
            "",    # 默认缺口桶
            "",    # 不限制区域
            "",    # 默认数量
            "7",   # 继续下一回合
        ]

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer), patch("builtins.input", side_effect=actions):
            runner._intervention_panel(review_round, review_before)
        panel_output = buffer.getvalue()

        review_after = runner._build_round_supply_review(review_round)
        summary = {
            "round": review_round,
            "actions": actions,
            "review_before": review_before,
            "review_after": review_after,
            "intervention_history": runner.intervention_history,
        }

        (run_dir / "panel_cli.log").write_text(panel_output, encoding="utf-8")
        (run_dir / "review_before.json").write_text(
            json.dumps(review_before, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        (run_dir / "review_after.json").write_text(
            json.dumps(review_after, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        (run_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )

        report_lines = [
            "# Round Panel Smoke",
            "",
            f"- 运行目录: `{run_dir}`",
            f"- 回合: `{review_round}`",
            "- 口径: 回合=虚拟市场周期，不等同现实自然月。",
            f"- 触发暂停: `{bool(review_before.get('should_pause', False))}`",
            f"- 触发原因: `{'; '.join(review_before.get('signal_reasons', []))}`",
            f"- 面板操作序列: `{actions}`",
            "",
            "## 面板前",
            f"- 热销画像数: `{len(review_before.get('hot_buckets', []))}`",
            f"- 缺口画像数: `{len(review_before.get('shortage_buckets', []))}`",
            f"- NO_ACTIVE_LISTINGS: `{review_before.get('no_active_listing_rows', 0)}`",
            f"- B区/非学区/刚需在售: `{next((item.get('active_listings') for item in review_before.get('cold_buckets', []) if item.get('bucket_id') == 'B_NOSCHOOL_JUST'), 0)}`",
            "",
            "## 面板后",
            f"- 热销画像数: `{len(review_after.get('hot_buckets', []))}`",
            f"- 缺口画像数: `{len(review_after.get('shortage_buckets', []))}`",
            f"- NO_ACTIVE_LISTINGS: `{review_after.get('no_active_listing_rows', 0)}`",
            f"- B区/非学区/刚需在售: `{next((item.get('active_listings') for item in review_after.get('cold_buckets', []) if item.get('bucket_id') == 'B_NOSCHOOL_JUST'), 0)}`",
            f"- 干预事件数: `{len(runner.intervention_history)}`",
            "- 说明：`NO_ACTIVE_LISTINGS` 是本回合已发生的缺口记录，因此同回合末补供后不会 retroactive 下降；真正改善会体现在后续回合的可选库存和撮合空间上。",
            "",
            "## 干预事件",
        ]
        for item in runner.intervention_history:
            report_lines.append(
                f"- 回合 {item.get('month')} / {item.get('event_type')}: {item.get('summary') or item.get('message')}"
            )
        (run_dir / "smoke_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")

        print(str(run_dir))
        return 0
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
