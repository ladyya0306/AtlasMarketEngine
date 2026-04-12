#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export Results Script
Exports DB tables and Logs to a timestamped folder.
Enhanced to produce "房产交易中心记录.csv" with rich details.
"""
import datetime
import logging
import os
import shutil
import sqlite3

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Default Paths (V1)
DEFAULT_DB_PATH = 'real_estate_stage2.db'
LOG_FILE = 'simulation_run.log'  # This is often local to CWD


def find_latest_result_dir(base_dir="results"):
    """Find the most recently created result directory"""
    if not os.path.exists(base_dir):
        return None

    dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if d.startswith("result_")]
    if not dirs:
        return None

    # Sort by creation time (or name which has timestamp)
    dirs.sort(key=os.path.getmtime, reverse=True)
    return dirs[0]


def export_data(db_path=None, output_dir=None):
    # Determine DB Path
    if not db_path:
        db_path = DEFAULT_DB_PATH

    # Determine Result Directory
    if not output_dir:
        # V1 Behavior: find latest or create new in "results"
        result_dir = find_latest_result_dir()
        if not result_dir or (datetime.datetime.now().timestamp() - os.path.getmtime(result_dir)) > 600:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            result_dir = os.path.join("results", f"result_{timestamp}")
            os.makedirs(result_dir, exist_ok=True)
            logger.info(f"📦 Created new result directory: {result_dir}")
        else:
            logger.info(f"📂 Using existing result directory: {result_dir}")
    else:
        # V2 Behavior: Use provided directory
        result_dir = output_dir
        if not os.path.exists(result_dir):
            os.makedirs(result_dir, exist_ok=True)

    # 2. Copy Logs
    if os.path.exists(LOG_FILE):
        # ✅ User Request: Rename to run.log
        shutil.copy(LOG_FILE, os.path.join(result_dir, "run.log"))
        logger.info(f"📜 Log saved to {os.path.join(result_dir, 'run.log')}")

    # ✅ User Request: Disable all CSV Exports
    # The DB is already in the result_dir (handled by project_manager), so we don't need to do anything else.
    # Just close connection and return.

    conn = sqlite3.connect(db_path)
    conn.close()

    logger.info("✅ Export Complete (Log only).")
    return

    # --- CSV EXPORT DISABLED ---
    """
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    """
    return


if __name__ == "__main__":
    export_data()
