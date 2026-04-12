#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_stage(name: str, args: list[str], log_path: Path) -> None:
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{ts()}] START {name}\n")
        f.write(f"[{ts()}] CMD {' '.join(args)}\n")
        f.flush()
        proc = subprocess.run(args, cwd=str(ROOT), stdout=f, stderr=f, text=True)
        if proc.returncode != 0:
            f.write(f"[{ts()}] FAILED {name} exit={proc.returncode}\n")
            f.flush()
            raise RuntimeError(f"stage failed: {name}")
        f.write(f"[{ts()}] DONE {name}\n")
        f.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description="Sleep chain pipeline")
    parser.add_argument("--timestamp", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = parser.parse_args()

    batch_root = ROOT / "results" / "experiment_batches" / f"sleep_chain_{args.timestamp}"
    batch_root.mkdir(parents=True, exist_ok=True)
    log_path = batch_root / "pipeline.log"

    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{ts()}] Sleep chain start\n")
        f.write(f"[{ts()}] BatchRoot {batch_root}\n")

    py = sys.executable

    a_out = batch_root / "A_gate_snap_i1d_i2_s202303_m2"
    b_out = batch_root / "B_gate_fresh_i1d_i2_s303_m2"
    c_out = batch_root / "C_p1_snap_r2ab_s101202303"
    d_out = batch_root / "D_p1_fresh_r2ab_s303"
    for p in (a_out, b_out, c_out, d_out):
        p.mkdir(parents=True, exist_ok=True)

    run_stage(
        "A_gate_snap_i1d_i2_s202303_m2",
        [
            py,
            "scripts/run_research_experiments.py",
            "--groups",
            "I1D",
            "I2",
            "--seeds",
            "202",
            "303",
            "--months-override",
            "2",
            "--fail-on-quality-gate",
            "--use-init-snapshot",
            "--out-dir",
            str(a_out),
        ],
        log_path,
    )
    run_stage("A_analyze", [py, "scripts/analyze_experiment_batch.py", str(a_out)], log_path)

    run_stage(
        "B_gate_fresh_i1d_i2_s303_m2",
        [
            py,
            "scripts/run_research_experiments.py",
            "--groups",
            "I1D",
            "I2",
            "--seed",
            "303",
            "--months-override",
            "2",
            "--fail-on-quality-gate",
            "--out-dir",
            str(b_out),
        ],
        log_path,
    )
    run_stage("B_analyze", [py, "scripts/analyze_experiment_batch.py", str(b_out)], log_path)

    run_stage(
        "C_p1_snap_r2ab_s101202303",
        [
            py,
            "scripts/run_research_experiments.py",
            "--groups",
            "R2A",
            "R2B",
            "--repeats",
            "3",
            "--use-init-snapshot",
            "--out-dir",
            str(c_out),
        ],
        log_path,
    )
    run_stage("C_analyze", [py, "scripts/analyze_experiment_batch.py", str(c_out)], log_path)

    run_stage(
        "D_p1_fresh_r2ab_s303",
        [
            py,
            "scripts/run_research_experiments.py",
            "--groups",
            "R2A",
            "R2B",
            "--seed",
            "303",
            "--out-dir",
            str(d_out),
        ],
        log_path,
    )
    run_stage("D_analyze", [py, "scripts/analyze_experiment_batch.py", str(d_out)], log_path)

    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{ts()}] Sleep chain finished successfully\n")

    print(str(batch_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
