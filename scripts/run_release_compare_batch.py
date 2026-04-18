from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON_EXE = Path(r"C:\Users\wyl\miniconda3\python.exe")


def _setup_utf8_stdio() -> None:
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass


def _group_cases(group: str) -> List[Dict[str, Any]]:
    normalized = str(group or "").strip().lower()
    if normalized == "g1g2":
        return [
            {
                "case_id": "G1_spindle_medium_balanced",
                "snapshot_id": "spindle_medium",
                "market_goal": "balanced",
                "demand_multiplier": 1.00,
            },
            {
                "case_id": "G2_pyramid_medium_balanced",
                "snapshot_id": "pyramid_medium",
                "market_goal": "balanced",
                "demand_multiplier": 1.00,
            },
        ]
    if normalized == "g3g4":
        return [
            {
                "case_id": "G3_buyer_market_spindle_large",
                "snapshot_id": "spindle_large",
                "market_goal": "buyer_market",
                "demand_multiplier": 0.80,
            },
            {
                "case_id": "G3_seller_market_spindle_large",
                "snapshot_id": "spindle_large",
                "market_goal": "seller_market",
                "demand_multiplier": 1.30,
            },
            {
                "case_id": "G4_balanced_resilience_spindle_large",
                "snapshot_id": "spindle_large",
                "market_goal": "balanced",
                "demand_multiplier": 1.00,
                "income_shock": ["2:-0.10:all"],
                "developer_supply": ["3:A:3:mixed_balanced"],
                "supply_cut": ["4:A:2"],
            },
        ]
    raise ValueError(f"Unsupported group={group}")


def _append_log(log_path: Path, message: str) -> None:
    print(message, flush=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def _write_manifest(batch_dir: Path, payload: Dict[str, Any]) -> None:
    (batch_dir / "batch_manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _run_case(
    workspace: Path,
    log_path: Path,
    months: int,
    seed: int,
    case: Dict[str, Any],
) -> int:
    cmd = [
        "powershell.exe",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(workspace / "scripts" / "run_release_longtest_utf8.ps1"),
        "-Workspace",
        str(workspace),
        "-SnapshotId",
        str(case["snapshot_id"]),
        "-MarketGoal",
        str(case["market_goal"]),
        "-DemandMultiplier",
        f"{float(case['demand_multiplier']):0.2f}",
        "-Months",
        str(int(months)),
        "-Seed",
        str(int(seed)),
    ]
    for spec in case.get("income_shock", []) or []:
        cmd.extend(["-IncomeShock", str(spec)])
    for spec in case.get("developer_supply", []) or []:
        cmd.extend(["-DeveloperSupply", str(spec)])
    for spec in case.get("supply_cut", []) or []:
        cmd.extend(["-SupplyCut", str(spec)])

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    _append_log(log_path, "")
    _append_log(log_path, f"==== {case['case_id']} ====")
    _append_log(log_path, f"SnapshotId: {case['snapshot_id']}")
    _append_log(log_path, f"MarketGoal: {case['market_goal']}")
    _append_log(log_path, f"DemandMultiplier: {float(case['demand_multiplier']):0.2f}")
    _append_log(log_path, f"Months: {months}")
    _append_log(log_path, f"Seed: {seed}")
    _append_log(log_path, f"IncomeShock: {', '.join(case.get('income_shock', []) or ['(none)'])}")
    _append_log(log_path, f"DeveloperSupply: {', '.join(case.get('developer_supply', []) or ['(none)'])}")
    _append_log(log_path, f"SupplyCut: {', '.join(case.get('supply_cut', []) or ['(none)'])}")
    _append_log(log_path, "Command: " + " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        cwd=str(workspace),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\n")
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    return proc.wait()


def main() -> int:
    _setup_utf8_stdio()
    parser = argparse.ArgumentParser(description="Run release compare batches in sequence.")
    parser.add_argument("--group", choices=["g1g2", "g3g4"], required=True)
    parser.add_argument("--workspace", default=str(PROJECT_ROOT))
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--seed", type=int, default=606)
    parser.add_argument("--batch-dir")
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_root = workspace / "results" / "release_compare_batches"
    batch_dir = Path(args.batch_dir).resolve() if args.batch_dir else (batch_root / f"{args.group}_{stamp}")
    batch_dir.mkdir(parents=True, exist_ok=True)
    log_dir = workspace / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"release_compare_{args.group}_{stamp}.log"
    cases = _group_cases(args.group)

    _write_manifest(
        batch_dir,
        {
            "group": args.group,
            "workspace": str(workspace),
            "months": int(args.months),
            "seed": int(args.seed),
            "cases": cases,
            "created_at": dt.datetime.now().isoformat(timespec="seconds"),
            "log_path": str(log_path),
        },
    )

    _append_log(log_path, f"==== 发布前对照序列 {args.group.upper()} ====")
    _append_log(log_path, f"时间: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _append_log(log_path, f"Workspace: {workspace}")
    _append_log(log_path, f"BatchDir: {batch_dir}")
    _append_log(log_path, f"BatchLog: {log_path}")
    _append_log(log_path, f"Months: {args.months}")
    _append_log(log_path, f"Seed: {args.seed}")
    _append_log(log_path, "顺序: " + " -> ".join(case["case_id"] for case in cases))

    for case in cases:
        exit_code = _run_case(workspace, log_path, args.months, args.seed, case)
        _append_log(log_path, f"{case['case_id']}_EXIT_CODE={exit_code}")
        if exit_code != 0:
            _append_log(log_path, f"{args.group.upper()}_BATCH_ABORTED")
            return exit_code

    _append_log(log_path, "")
    _append_log(log_path, f"{args.group.upper()}_BATCH_COMPLETED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

