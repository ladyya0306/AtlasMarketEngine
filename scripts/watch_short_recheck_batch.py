#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
等待短测批次结束，然后自动生成区域链路分析。

当前用途：
- 配合 D4/C3 这种短窗口复核夜跑
- 避免次日人工先翻 batch_summary，再手动跑分析脚本
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="等待批次结束并自动分析")
    parser.add_argument("batch_dir", help="批次目录")
    parser.add_argument("--poll-seconds", type=int, default=20, help="轮询间隔秒数")
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir).resolve()
    summary = batch_dir / "batch_summary.json"
    timeout_seconds = 8 * 3600
    started = time.time()

    while not summary.exists():
        if time.time() - started > timeout_seconds:
            print(f"timeout waiting for {summary}", file=sys.stderr)
            return 1
        time.sleep(max(5, int(args.poll_seconds)))

    cmd = [sys.executable, str(Path(__file__).resolve().parent / "analyze_experiment_batch.py"), str(batch_dir)]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
