import argparse
import json
from pathlib import Path
from typing import Any, Dict

import yaml

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a non-interactive overnight simulation with preplanned monthly interventions."
    )
    parser.add_argument(
        "--base-config",
        default="config/baseline.yaml",
        help="Base simulation config path. Default: config/baseline.yaml",
    )
    parser.add_argument(
        "--plan",
        default="config/night_run_example.yaml",
        help="Night-run overlay plan path. Default: config/night_run_example.yaml",
    )
    parser.add_argument("--db-path", default=None, help="Optional DB path or existing run DB when resuming.")
    parser.add_argument("--agent-count", type=int, default=None, help="Override agent count.")
    parser.add_argument("--months", type=int, default=None, help="Override simulation months.")
    parser.add_argument("--seed", type=int, default=None, help="Override random seed.")
    parser.add_argument("--resume", action="store_true", help="Resume from an existing DB instead of starting fresh.")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    base_config_path = Path(args.base_config)
    if not base_config_path.exists():
        raise FileNotFoundError(f"Base config not found: {base_config_path}")

    config = SimulationConfig(str(base_config_path))

    plan_path = Path(args.plan)
    if plan_path.exists():
        deep_merge(config._config, load_yaml(plan_path))
    elif args.plan:
        raise FileNotFoundError(f"Night-run plan not found: {plan_path}")

    config.update("simulation.enable_intervention_panel", False)

    agent_count = int(args.agent_count or config.get("simulation.agent_count", 50) or 50)
    months = int(args.months or config.get("simulation.months", 12) or 12)
    seed = args.seed if args.seed is not None else config.get("simulation.random_seed", 42)

    if args.resume and not args.db_path:
        raise ValueError("--resume requires --db-path")

    runner = SimulationRunner(
        agent_count=agent_count,
        months=months,
        seed=seed,
        resume=bool(args.resume),
        config=config,
        db_path=args.db_path,
    )

    planned_items = config.get("simulation.preplanned_interventions", []) or []
    print("=== Night Run ===")
    print(f"Base config: {base_config_path}")
    print(f"Plan: {plan_path if plan_path.exists() else '(none)'}")
    print(f"Agent count: {agent_count}")
    print(f"Months: {months}")
    print(f"Seed: {seed}")
    print(f"Resume: {bool(args.resume)}")
    print(f"DB path: {runner.db_path}")
    print(f"Preplanned interventions: {len(planned_items)}")
    if planned_items:
        print(json.dumps(planned_items, ensure_ascii=False, indent=2))

    try:
        runner.run_night()
    finally:
        # runner.run_night() closes internally; keep finally for symmetry if behavior changes later.
        if runner.conn is not None:
            runner.close()

    print("=== Night Run Complete ===")
    print(f"Run directory: {runner._run_dir}")
    print(f"DB path: {runner.db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
