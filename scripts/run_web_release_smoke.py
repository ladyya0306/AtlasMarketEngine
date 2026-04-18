from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _setup_utf8_stdio() -> None:
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass


def _append_log(log_path: Path, message: str) -> None:
    print(message, flush=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def _default_report_dir(snapshot_id: str, months: int, demand_multiplier: float) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_multiplier = str(demand_multiplier).replace(".", "p")
    return PROJECT_ROOT / "results" / "release_web_smoke" / (
        f"web_smoke_{stamp}_{snapshot_id}_m{months}_d{safe_multiplier}"
    )


def _request(
    method: str,
    url: str,
    payload: Dict[str, Any] | None = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        text = resp.read().decode("utf-8")
        content_type = resp.headers.get("Content-Type", "")
        return {
            "status": resp.status,
            "headers": dict(resp.headers.items()),
            "content_type": content_type,
            "text": text,
            "json": json.loads(text) if "application/json" in content_type else None,
        }


def _request_json(method: str, url: str, payload: Dict[str, Any] | None = None, timeout: int = 60) -> Dict[str, Any]:
    response = _request(method, url, payload=payload, timeout=timeout)
    if response["json"] is None:
        raise ValueError(f"Expected JSON from {url}, got {response['content_type']!r}")
    return response["json"]


def _ensure_contains(text: str, needle: str, *, label: str) -> None:
    if needle not in text:
        raise AssertionError(f"{label} missing required marker: {needle}")


def _default_preplanned_interventions(months: int) -> List[Dict[str, Any]]:
    if int(months) >= 3:
        return [
            {"action_type": "income_shock", "month": 2, "pct_change": -0.10, "target_tier": "all"},
            {"action_type": "developer_supply", "month": 2, "zone": "A", "count": 3, "template": "mixed_balanced"},
            {"action_type": "supply_cut", "month": 3, "zone": "A", "count": 2},
        ]
    return [
        {"action_type": "income_shock", "month": 1, "pct_change": -0.05, "target_tier": "all"},
        {"action_type": "developer_supply", "month": 1, "zone": "A", "count": 1, "template": "mixed_balanced"},
        {"action_type": "supply_cut", "month": 1, "zone": "B", "count": 1},
    ]


def _scan_runtime_log(log_path: Path) -> Dict[str, List[str]]:
    preplanned_lines: List[str] = []
    checkpoint_lines: List[str] = []
    if not log_path.exists():
        return {"preplanned_lines": preplanned_lines, "checkpoint_lines": checkpoint_lines}
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if "Preplanned intervention executed:" in line:
                preplanned_lines.append(line)
            if "checkpoint saved:" in line:
                checkpoint_lines.append(line)
    return {"preplanned_lines": preplanned_lines, "checkpoint_lines": checkpoint_lines}


def _collect_checkpoint_rows(runtime_dir: Path, months: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for month in range(1, int(months) + 1):
        checkpoint_dir = runtime_dir / "monthly_checkpoints" / f"month_{month:02d}"
        rows.append(
            {
                "month": month,
                "checkpoint_dir": str(checkpoint_dir),
                "exists": checkpoint_dir.exists(),
                "db_exists": (checkpoint_dir / "simulation.db").exists(),
                "meta_exists": (checkpoint_dir / "checkpoint_meta.json").exists(),
                "status_exists": (checkpoint_dir / "status_snapshot.json").exists(),
            }
        )
    return rows


def _query_db_summary(db_path: Path) -> Dict[str, Any]:
    if not db_path.exists():
        return {"exists": False}
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT month), MIN(month), MAX(month) FROM decision_logs")
        decision_logs = cursor.fetchone() or (0, 0, None, None)
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT month) FROM transactions")
        transactions = cursor.fetchone() or (0, 0)
        return {
            "exists": True,
            "decision_log_rows": int(decision_logs[0] or 0),
            "distinct_decision_months": int(decision_logs[1] or 0),
            "min_decision_month": decision_logs[2],
            "max_decision_month": decision_logs[3],
            "transaction_rows": int(transactions[0] or 0),
            "distinct_transaction_months": int(transactions[1] or 0),
        }
    finally:
        conn.close()


def _build_markdown_report(report: Dict[str, Any]) -> str:
    checkpoint_lines = "\n".join(
        [
            f"- month_{row['month']:02d}: exists={row['exists']} db={row['db_exists']} meta={row['meta_exists']} status={row['status_exists']}"
            for row in report["checkpoint_rows"]
        ]
    )
    step_lines = "\n".join(
        [
            f"- month {row['month']}: status={row['status']} avg_price={row['avg_transaction_price']} tx={row['transactions']}"
            for row in report["step_rows"]
        ]
    )
    return "\n".join(
        [
            "# Web Release Smoke",
            "",
            "## Setup",
            f"- base_url: `{report['base_url']}`",
            f"- snapshot: `{report['snapshot_id']}`",
            f"- market_goal: `{report['market_goal']}`",
            f"- requested_multiplier: `{report['requested_multiplier']:.4f}`",
            f"- effective_multiplier: `{report['effective_multiplier']:.4f}`",
            f"- effective_agent_count: `{report['effective_agent_count']}`",
            f"- property_count: `{report['property_count']}`",
            "",
            "## Web Entry Checks",
            f"- release controls present: `{report['release_controls_ok']}`",
            f"- release schema present: `{report['release_schema_ok']}`",
            f"- demand floor auto-lifted: `{report['auto_lifted']}`",
            "",
            "## Run Evidence",
            f"- run_dir: `{report['run_dir']}`",
            f"- db_path: `{report['db_path']}`",
            f"- runtime_log: `{report['runtime_log_path']}`",
            f"- parameter_report_md: `{report['parameter_report_md']}`",
            f"- parameter_report_json: `{report['parameter_report_json']}`",
            "",
            "## Step Results",
            step_lines or "- none",
            "",
            "## Checkpoints",
            checkpoint_lines or "- none",
            "",
            "## Log Scan",
            f"- preplanned_lines: `{len(report['runtime_log_scan']['preplanned_lines'])}`",
            f"- checkpoint_lines: `{len(report['runtime_log_scan']['checkpoint_lines'])}`",
            "",
            "## DB Summary",
            f"- decision_log_rows: `{report['db_summary'].get('decision_log_rows')}`",
            f"- distinct_decision_months: `{report['db_summary'].get('distinct_decision_months')}`",
            f"- transaction_rows: `{report['db_summary'].get('transaction_rows')}`",
            f"- distinct_transaction_months: `{report['db_summary'].get('distinct_transaction_months')}`",
            "",
            "## Verdict",
            f"- completed: `{report['completed']}`",
            f"- notes: {report['verdict']}",
        ]
    )


def run(args: argparse.Namespace) -> int:
    report_dir = _default_report_dir(args.snapshot_id, args.months, args.demand_multiplier)
    if args.report_dir:
        candidate = Path(args.report_dir)
        report_dir = candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate)
    runtime_dir = report_dir / "runtime_run"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    db_path = runtime_dir / "simulation.db"
    log_path = report_dir / "web_smoke.log"
    request_path = report_dir / "smoke_request.json"
    report_json_path = report_dir / "smoke_report.json"
    report_md_path = report_dir / "smoke_report.md"
    parameter_report_md = runtime_dir / "parameter_assumption_report.md"
    parameter_report_json = runtime_dir / "parameter_assumption_report.json"

    base_url = args.base_url.rstrip("/")
    _append_log(log_path, f"== Web release smoke ==")
    _append_log(log_path, f"base_url={base_url}")
    _append_log(log_path, f"report_dir={report_dir}")

    health = _request_json("GET", f"{base_url}/health")
    if health.get("status") != "ok":
        raise AssertionError(f"Unexpected health payload: {health}")
    _append_log(log_path, f"health={health}")

    html_response = _request("GET", f"{base_url}/")
    html_text = html_response["text"]
    for marker in (
        'id="startup-supply-snapshot"',
        'id="startup-market-goal"',
        'id="startup-demand-multiplier"',
        'id="startup-demand-coverage"',
        'id="night-plan-add-supply-cut"',
    ):
        _ensure_contains(html_text, marker, label="web_index")
    _append_log(log_path, "html_release_controls=ok")

    schema = _request_json("GET", f"{base_url}/config/schema")
    release_defaults = ((schema or {}).get("startup_defaults") or {}).get("release_startup") or {}
    if not release_defaults.get("enabled"):
        raise AssertionError("release_startup.enabled is false")
    snapshots = {
        str(item.get("snapshot_id", "")): item
        for item in (release_defaults.get("supply_snapshots") or [])
    }
    if args.snapshot_id not in snapshots:
        raise AssertionError(f"snapshot_id={args.snapshot_id} missing from release_startup.supply_snapshots")
    snapshot = snapshots[args.snapshot_id]
    requested_multiplier = float(args.demand_multiplier)
    property_count = int(snapshot.get("total_selected_supply", 0) or 0)
    demand_bucket_count = int(snapshot.get("demand_bucket_count", 0) or 0)
    requested_agent_count = max(1, int((property_count * requested_multiplier) + 0.5))
    effective_agent_count = max(requested_agent_count, demand_bucket_count)
    effective_multiplier = float(effective_agent_count) / float(max(1, property_count))
    preplanned_interventions = _default_preplanned_interventions(args.months)

    payload = {
        "months": int(args.months),
        "seed": int(args.seed),
        "db_path": str(db_path),
        "startup_overrides": {
            "use_release_supply_controls": True,
            "fixed_supply_snapshot_id": args.snapshot_id,
            "market_goal": args.market_goal,
            "demand_multiplier": requested_multiplier,
            "income_adjustment_rate": float(args.income_adjustment_rate),
            "down_payment_ratio": float(args.down_payment_ratio),
            "max_dti_ratio": float(args.max_dti_ratio),
            "annual_interest_rate": float(args.annual_interest_rate),
            "enable_intervention_panel": False,
            "market_pulse_enabled": False,
            "market_pulse_seed_ratio": 0.55,
            "effective_bid_floor_ratio": 0.98,
            "precheck_liquidity_buffer_months": 3,
            "precheck_include_tax_and_fee": True,
        },
        "preplanned_interventions": preplanned_interventions,
    }
    request_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _append_log(log_path, f"payload_path={request_path}")

    start_payload = _request_json("POST", f"{base_url}/start", payload=payload, timeout=120)
    if start_payload.get("status") != "initialized":
        raise AssertionError(f"Unexpected start status: {start_payload}")
    _append_log(log_path, f"start_status={start_payload.get('status')} run_dir={start_payload.get('run_dir')}")

    step_rows: List[Dict[str, Any]] = []
    for step_index in range(1, int(args.months) + 1):
        step_payload = _request_json("POST", f"{base_url}/step", timeout=180)
        month_result = step_payload.get("month_result") or {}
        status_payload = step_payload.get("status") or {}
        step_row = {
            "month": int(month_result.get("month", step_index) or step_index),
            "status": str(status_payload.get("status", "")),
            "avg_transaction_price": month_result.get("avg_transaction_price"),
            "transactions": month_result.get("transactions"),
        }
        step_rows.append(step_row)
        _append_log(log_path, f"step_month={step_row['month']} status={step_row['status']} tx={step_row['transactions']}")
        if status_payload.get("status") == "completed":
            break

    status_payload = _request_json("GET", f"{base_url}/status")
    completed = status_payload.get("status") == "completed"
    run_dir = Path(str(status_payload.get("run_dir") or runtime_dir))
    runtime_log_path = run_dir / "simulation_run.log"
    checkpoint_rows = _collect_checkpoint_rows(run_dir, int(args.months))
    runtime_log_scan = _scan_runtime_log(runtime_log_path)
    db_summary = _query_db_summary(db_path)

    report = {
        "base_url": base_url,
        "snapshot_id": args.snapshot_id,
        "market_goal": args.market_goal,
        "requested_multiplier": requested_multiplier,
        "effective_multiplier": effective_multiplier,
        "effective_agent_count": effective_agent_count,
        "property_count": property_count,
        "release_controls_ok": True,
        "release_schema_ok": True,
        "auto_lifted": effective_agent_count != requested_agent_count,
        "run_dir": str(run_dir),
        "db_path": str(db_path),
        "runtime_log_path": str(runtime_log_path),
        "parameter_report_md": str(parameter_report_md),
        "parameter_report_json": str(parameter_report_json),
        "step_rows": step_rows,
        "checkpoint_rows": checkpoint_rows,
        "runtime_log_scan": runtime_log_scan,
        "db_summary": db_summary,
        "completed": completed,
        "verdict": (
            "web release startup chain completed successfully"
            if completed
            else "run did not reach completed state"
        ),
        "start_payload": start_payload,
        "final_status": status_payload,
    }
    report_json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report_md_path.write_text(_build_markdown_report(report), encoding="utf-8")
    _append_log(log_path, f"report_json={report_json_path}")
    _append_log(log_path, f"report_md={report_md_path}")

    if not completed:
        return 1
    if not parameter_report_md.exists() or not parameter_report_json.exists():
        raise AssertionError("parameter assumption reports were not generated")
    if not all(row["db_exists"] and row["meta_exists"] and row["status_exists"] for row in checkpoint_rows):
        raise AssertionError("missing checkpoint artifacts in web smoke run")
    if db_summary.get("distinct_decision_months", 0) < int(args.months):
        raise AssertionError(f"expected {args.months} months in db, got {db_summary}")
    if len(runtime_log_scan["preplanned_lines"]) < 3:
        raise AssertionError("preplanned interventions were not fully logged")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a minimal Web/API release smoke against a live local server.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8011")
    parser.add_argument("--snapshot-id", default="spindle_minimum")
    parser.add_argument("--market-goal", default="balanced")
    parser.add_argument("--months", type=int, default=3)
    parser.add_argument("--demand-multiplier", type=float, default=0.10)
    parser.add_argument("--seed", type=int, default=606)
    parser.add_argument("--report-dir")
    parser.add_argument("--income-adjustment-rate", type=float, default=1.0)
    parser.add_argument("--down-payment-ratio", type=float, default=0.26)
    parser.add_argument("--max-dti-ratio", type=float, default=0.46)
    parser.add_argument("--annual-interest-rate", type=float, default=0.041)
    return parser


def main() -> int:
    _setup_utf8_stdio()
    parser = build_parser()
    args = parser.parse_args()
    try:
        return run(args)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"HTTPError: {exc.code} {body}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
