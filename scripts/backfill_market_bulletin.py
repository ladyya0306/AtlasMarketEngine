import argparse
import asyncio
import sqlite3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simulation_runner import SimulationRunner


def detect_max_month(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:
        values = [
            int((cur.execute("SELECT COALESCE(MAX(month), 0) FROM transactions").fetchone() or [0])[0] or 0),
            int((cur.execute("SELECT COALESCE(MAX(month), 0) FROM market_bulletin").fetchone() or [0])[0] or 0),
            int((cur.execute("SELECT COALESCE(MAX(month), 0) FROM active_participants").fetchone() or [0])[0] or 0),
        ]
        return max(values)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill market_bulletin rows using month-end facts.")
    parser.add_argument("--db-path", required=True, help="Path to simulation.db")
    parser.add_argument("--from-month", type=int, default=1, help="First month to backfill")
    parser.add_argument("--to-month", type=int, default=None, help="Last month to backfill")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    runner = SimulationRunner(resume=True, db_path=str(db_path))
    runner._run_dir = str(db_path.parent)

    to_month = int(args.to_month or detect_max_month(db_path))
    from_month = max(1, int(args.from_month))

    print(f"Backfilling market_bulletin for {db_path}")
    for month in range(from_month, to_month + 1):
        text = asyncio.run(runner.market_service.generate_market_bulletin(month, [], observed_month=month))
        first_line = next((line.strip() for line in str(text).splitlines() if line.strip()), "")
        print(f"  month {month}: {first_line}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
