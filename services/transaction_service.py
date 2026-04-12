import asyncio
import copy
import datetime
import json
import logging
import math
import os
import random
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

# from transaction_engine import (
#     match_property_for_buyer, run_negotiation_session_async, execute_transaction,
#     handle_failed_negotiation
# )
from agent_behavior import decide_price_adjustment
from models import Agent
from mortgage_system import calculate_monthly_payment
from services.financial_calculator import FinancialCalculator

logger = logging.getLogger(__name__)


class TransactionService:
    def __init__(self, config, db_conn: sqlite3.Connection, developer_service=None, mortgage_risk_service=None):
        self.config = config
        self.conn = db_conn
        self.developer_service = developer_service
        self.mortgage_risk_service = mortgage_risk_service
        # Buffer high-frequency decision logs and flush in batch to reduce write amplification.
        self._decision_log_buffer: List[Tuple] = []
        # Buffer order lifecycle logs; flush in batches to reduce sync file I/O overhead.
        self._order_log_buffer: List[Dict] = []
        self._order_log_flush_size: int = 200
        # Lightweight schema cache for backward-compatible writes in unit tests/legacy DBs.
        self._table_exists_cache: Dict[str, bool] = {}
        self._column_exists_cache: Dict[Tuple[str, str], bool] = {}
        self._hard_bucket_context_cache: Optional[Dict[str, Any]] = None
        self._ensure_transaction_order_snapshot_columns()

    @staticmethod
    def _as_bool(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _profiled_market_mode_cfg(self) -> Dict[str, Any]:
        raw_cfg = self.config.get(
            "smart_agent.profiled_market_mode",
            self.config.get("profiled_market_mode", {}),
        )
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}
        return {
            "enabled": self._as_bool(raw_cfg.get("enabled", False), False),
            "hard_bucket_matcher_enabled": self._as_bool(
                raw_cfg.get("hard_bucket_matcher_enabled", False),
                False,
            ),
            "hard_bucket_include_soft_buckets": self._as_bool(
                raw_cfg.get("hard_bucket_include_soft_buckets", False),
                False,
            ),
            "hard_bucket_require_profiled_buyer": self._as_bool(
                raw_cfg.get("hard_bucket_require_profiled_buyer", False),
                False,
            ),
            "hard_bucket_strict_unmapped_property": self._as_bool(
                raw_cfg.get("hard_bucket_strict_unmapped_property", True),
                True,
            ),
            "profile_pack_path": str(raw_cfg.get("profile_pack_path", "") or "").strip(),
            "profile_pack_inline": raw_cfg.get("profile_pack") if isinstance(raw_cfg.get("profile_pack"), dict) else {},
        }

    def _resolve_profiled_market_pack(self, mode_cfg: Dict[str, Any]) -> Dict[str, Any]:
        inline_pack = mode_cfg.get("profile_pack_inline") or {}
        if isinstance(inline_pack, dict) and inline_pack:
            return inline_pack
        pack_path = str(mode_cfg.get("profile_pack_path", "") or "").strip()
        if not pack_path:
            return {}
        resolved = Path(pack_path)
        if not resolved.is_absolute():
            resolved = (Path(__file__).resolve().parents[1] / resolved).resolve()
        if not resolved.exists():
            return {}
        try:
            payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
            if not isinstance(payload, dict):
                return {}
            pack = payload.get("profiled_market_mode", payload)
            return dict(pack) if isinstance(pack, dict) else {}
        except Exception:
            return {}

    def _load_profiled_bucket_maps(self, cursor) -> Tuple[Dict[int, str], Dict[int, str]]:
        buyer_bucket_map: Dict[int, str] = {}
        property_bucket_map: Dict[int, str] = {}
        if self._table_exists(cursor, "profiled_market_agent_buckets"):
            try:
                cursor.execute("SELECT agent_id, bucket_id FROM profiled_market_agent_buckets")
                for row in cursor.fetchall() or []:
                    if row and row[0] is not None and row[1] is not None:
                        buyer_bucket_map[int(row[0])] = str(row[1])
            except Exception:
                pass
        if self._table_exists(cursor, "profiled_market_property_buckets"):
            try:
                cursor.execute("SELECT property_id, bucket_id FROM profiled_market_property_buckets")
                for row in cursor.fetchall() or []:
                    if row and row[0] is not None and row[1] is not None:
                        property_bucket_map[int(row[0])] = str(row[1])
            except Exception:
                pass
        return buyer_bucket_map, property_bucket_map

    @staticmethod
    def _build_alignment_map(profile_pack: Dict[str, Any], include_soft: bool) -> Dict[str, Set[str]]:
        out: Dict[str, Set[str]] = {}
        rules = profile_pack.get("bucket_alignment_rules", [])
        if not isinstance(rules, list):
            return out
        for item in rules:
            if not isinstance(item, dict):
                continue
            agent_bucket = str(item.get("agent_bucket_id", "") or "").strip()
            if not agent_bucket:
                continue
            allowed = {
                str(x).strip()
                for x in (item.get("eligible_property_buckets", []) or [])
                if str(x).strip()
            }
            if include_soft:
                allowed.update(
                    {
                        str(x).strip()
                        for x in (item.get("soft_property_buckets", []) or [])
                        if str(x).strip()
                    }
                )
            out[agent_bucket] = allowed
        return out

    def _resolve_hard_bucket_context(self, cursor) -> Dict[str, Any]:
        if self._hard_bucket_context_cache is not None:
            return dict(self._hard_bucket_context_cache)
        mode_cfg = self._profiled_market_mode_cfg()
        enabled = bool(mode_cfg.get("enabled", False)) and bool(mode_cfg.get("hard_bucket_matcher_enabled", False))
        profile_pack = self._resolve_profiled_market_pack(mode_cfg) if enabled else {}
        buyer_bucket_map, property_bucket_map = self._load_profiled_bucket_maps(cursor) if enabled else ({}, {})
        alignment_map = self._build_alignment_map(
            profile_pack=profile_pack,
            include_soft=bool(mode_cfg.get("hard_bucket_include_soft_buckets", False)),
        ) if enabled else {}
        ctx = {
            "enabled": bool(enabled),
            "require_profiled_buyer": bool(mode_cfg.get("hard_bucket_require_profiled_buyer", False)),
            "strict_unmapped_property": bool(mode_cfg.get("hard_bucket_strict_unmapped_property", True)),
            "buyer_bucket_map": buyer_bucket_map,
            "property_bucket_map": property_bucket_map,
            "alignment_map": alignment_map,
        }
        self._hard_bucket_context_cache = dict(ctx)
        return ctx

    def _resolve_hard_bucket_property_whitelist(
        self,
        cursor,
        buyer_id: int,
    ) -> Tuple[Optional[Set[int]], Dict[str, Any]]:
        ctx = self._resolve_hard_bucket_context(cursor)
        if not bool(ctx.get("enabled", False)):
            return None, {"mode": "disabled"}
        buyer_bucket_map = ctx.get("buyer_bucket_map", {}) or {}
        property_bucket_map = ctx.get("property_bucket_map", {}) or {}
        alignment_map = ctx.get("alignment_map", {}) or {}
        require_profiled_buyer = bool(ctx.get("require_profiled_buyer", False))

        buyer_bucket = str(buyer_bucket_map.get(int(buyer_id), "") or "").strip()
        if not buyer_bucket:
            if require_profiled_buyer:
                return set(), {"mode": "enabled", "reason": "buyer_bucket_missing", "buyer_bucket": ""}
            return None, {"mode": "enabled", "reason": "buyer_bucket_missing_passthrough", "buyer_bucket": ""}
        allowed_buckets = set(alignment_map.get(buyer_bucket, set()) or set())
        if not allowed_buckets:
            return set(), {"mode": "enabled", "reason": "alignment_missing", "buyer_bucket": buyer_bucket}
        whitelist = {
            int(pid) for pid, bucket in property_bucket_map.items()
            if str(bucket) in allowed_buckets
        }
        return whitelist, {
            "mode": "enabled",
            "buyer_bucket": buyer_bucket,
            "allowed_property_buckets": sorted(str(x) for x in allowed_buckets),
            "whitelist_size": int(len(whitelist)),
        }

    def _ensure_recovery_tables(self, cursor):
        """
        Persist recovery lifecycle to avoid in-memory only reflow loops.
        """
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS buyer_recovery_queue (
                    month INTEGER NOT NULL,
                    buyer_id INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'active',
                    lock_reason TEXT,
                    rounds_used INTEGER NOT NULL DEFAULT 0,
                    last_progress_round INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (month, buyer_id)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS buyer_recovery_attempts (
                    month INTEGER NOT NULL,
                    buyer_id INTEGER NOT NULL,
                    property_id INTEGER NOT NULL,
                    attempt_round INTEGER NOT NULL DEFAULT 0,
                    result TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (month, buyer_id, property_id)
                )
                """
            )
            self._table_exists_cache["buyer_recovery_queue"] = True
            self._table_exists_cache["buyer_recovery_attempts"] = True
        except Exception as e:
            logger.debug(f"failed to ensure recovery tables: {e}")

    def _upsert_recovery_queue(
        self,
        cursor,
        month: int,
        buyer_id: int,
        lock_reason: str = "",
    ):
        try:
            cursor.execute(
                """
                INSERT INTO buyer_recovery_queue (month, buyer_id, state, lock_reason, rounds_used, last_progress_round)
                VALUES (?, ?, 'active', ?, 0, 0)
                ON CONFLICT(month, buyer_id) DO UPDATE SET
                    state='active',
                    lock_reason=excluded.lock_reason,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (int(month), int(buyer_id), str(lock_reason or "")),
            )
        except Exception as e:
            logger.debug(f"failed to upsert recovery queue: {e}")

    def _record_recovery_attempt(
        self,
        cursor,
        month: int,
        buyer_id: int,
        property_id: int,
        attempt_round: int,
        result: str,
    ):
        try:
            cursor.execute(
                """
                INSERT INTO buyer_recovery_attempts (month, buyer_id, property_id, attempt_round, result)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(month, buyer_id, property_id) DO UPDATE SET
                    attempt_round=excluded.attempt_round,
                    result=excluded.result,
                    created_at=CURRENT_TIMESTAMP
                """,
                (int(month), int(buyer_id), int(property_id), int(attempt_round), str(result or "")),
            )
        except Exception as e:
            logger.debug(f"failed to record recovery attempt: {e}")

    def _load_recovery_attempted_property_ids(self, cursor, month: int, buyer_id: int) -> set[int]:
        try:
            cursor.execute(
                """
                SELECT property_id
                FROM buyer_recovery_attempts
                WHERE month=? AND buyer_id=?
                """,
                (int(month), int(buyer_id)),
            )
            return {
                int(r[0]) for r in (cursor.fetchall() or [])
                if r and r[0] is not None
            }
        except Exception:
            return set()

    def _load_buyer_seen_property_ids(self, cursor, buyer_id: int, through_month: int) -> set[int]:
        if int(buyer_id) < 0 or int(through_month) <= 0:
            return set()
        if not self._table_exists(cursor, "property_buyer_matches"):
            return set()
        try:
            cursor.execute(
                """
                SELECT DISTINCT property_id
                FROM property_buyer_matches
                WHERE buyer_id=? AND month<=? AND property_id IS NOT NULL
                """,
                (int(buyer_id), int(through_month)),
            )
            return {
                int(r[0]) for r in (cursor.fetchall() or [])
                if r and r[0] is not None
            }
        except Exception:
            return set()

    def _load_buyer_last_seen_listing_prices(self, cursor, buyer_id: int, through_month: int) -> Dict[int, float]:
        if int(buyer_id) < 0 or int(through_month) <= 0:
            return {}
        if not self._table_exists(cursor, "property_buyer_matches"):
            return {}
        try:
            cursor.execute(
                """
                SELECT property_id, MAX(month) AS last_seen_month
                FROM property_buyer_matches
                WHERE buyer_id=? AND month<=? AND property_id IS NOT NULL
                GROUP BY property_id
                """,
                (int(buyer_id), int(through_month)),
            )
            last_month_rows = cursor.fetchall() or []
            out: Dict[int, float] = {}
            for pid, last_seen_month in last_month_rows:
                try:
                    pid_i = int(pid)
                    last_seen_month_i = int(last_seen_month)
                except Exception:
                    continue
                row = cursor.execute(
                    """
                    SELECT listing_price
                    FROM property_buyer_matches
                    WHERE buyer_id=? AND property_id=? AND month=?
                    ORDER BY match_id DESC
                    LIMIT 1
                    """,
                    (int(buyer_id), pid_i, last_seen_month_i),
                ).fetchone()
                if not row:
                    continue
                try:
                    listing_price = float(row[0] or 0.0)
                except Exception:
                    listing_price = 0.0
                if listing_price > 0:
                    out[pid_i] = listing_price
            return out
        except Exception:
            return {}

    def _record_shortlist_exposure_rows(
        self,
        cursor,
        month: int,
        buyer: Agent,
        shortlist_context: List[Dict],
        selected_ids: List[int],
        no_selection_code: str,
        match_ctx: Dict,
    ):
        if not shortlist_context or not self._table_exists(cursor, "property_buyer_matches"):
            return
        try:
            buyer_id = int(getattr(buyer, "id", -1) or -1)
        except Exception:
            buyer_id = -1
        if buyer_id < 0:
            return
        selected_id_set = {int(pid) for pid in (selected_ids or []) if pid is not None}
        selection_reason = str(match_ctx.get("selection_reason", "") or "")
        rows = []
        emitted_ids = set()
        for item in shortlist_context:
            try:
                pid = int(item.get("property_id"))
            except Exception:
                continue
            if pid in emitted_ids or pid in selected_id_set:
                continue
            emitted_ids.add(int(pid))
            try:
                listed_price = float(item.get("listed_price", 0.0) or 0.0)
            except Exception:
                listed_price = 0.0
            rows.append(
                (
                    int(month),
                    int(pid),
                    int(buyer_id),
                    float(listed_price),
                    None,
                    1,
                    0,
                    None,
                    json.dumps(
                        {
                            "shortlist_item": item,
                            "selection_reason": selection_reason,
                            "selected_property_ids": [int(x) for x in sorted(selected_id_set)],
                            "no_selection_reason_code": str(no_selection_code or ""),
                        },
                        ensure_ascii=False,
                    ),
                    selection_reason,
                    0,
                    "SHORTLIST_ONLY",
                    "DECISION",
                    str(no_selection_code or "SHORTLIST_NOT_SELECTED"),
                    None,
                )
            )
        if rows:
            cursor.executemany(
                """
                INSERT INTO property_buyer_matches
                (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation,
                 order_id, match_context, selection_reason, selected_in_shortlist,
                 final_outcome, failure_stage, failure_reason, final_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _mark_recovery_queue_state(
        self,
        cursor,
        month: int,
        buyer_id: int,
        state: str,
        progress_round: Optional[int] = None,
    ):
        state_norm = str(state or "active").strip().lower()
        if state_norm not in {"active", "exhausted", "completed", "deferred_next_month"}:
            state_norm = "active"
        try:
            if progress_round is None:
                cursor.execute(
                    """
                    UPDATE buyer_recovery_queue
                    SET state=?, updated_at=CURRENT_TIMESTAMP
                    WHERE month=? AND buyer_id=?
                    """,
                    (state_norm, int(month), int(buyer_id)),
                )
            else:
                cursor.execute(
                    """
                    UPDATE buyer_recovery_queue
                    SET state=?, last_progress_round=?, updated_at=CURRENT_TIMESTAMP
                    WHERE month=? AND buyer_id=?
                    """,
                    (state_norm, int(progress_round), int(month), int(buyer_id)),
                )
        except Exception as e:
            logger.debug(f"failed to mark recovery queue state: {e}")

    def _bump_recovery_rounds(self, cursor, month: int, buyer_ids: List[int]):
        if not buyer_ids:
            return
        for buyer_id in buyer_ids:
            try:
                cursor.execute(
                    """
                    UPDATE buyer_recovery_queue
                    SET rounds_used = rounds_used + 1,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE month=? AND buyer_id=? AND state='active'
                    """,
                    (int(month), int(buyer_id)),
                )
            except Exception:
                continue

    def _enqueue_decision_log(self, row: Tuple):
        self._decision_log_buffer.append(row)

    def _flush_decision_log_buffer(self, cursor):
        if not self._decision_log_buffer:
            return
        cursor.executemany(
            """
            INSERT INTO decision_logs
            (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._decision_log_buffer,
        )
        self._decision_log_buffer.clear()

    def _table_exists(self, cursor, table_name: str) -> bool:
        cached = self._table_exists_cache.get(str(table_name))
        if cached is not None:
            return bool(cached)
        try:
            cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (str(table_name),),
            )
            exists = cursor.fetchone() is not None
        except Exception:
            exists = False
        self._table_exists_cache[str(table_name)] = bool(exists)
        return bool(exists)

    def _column_exists(self, cursor, table_name: str, column_name: str) -> bool:
        key = (str(table_name), str(column_name))
        cached = self._column_exists_cache.get(key)
        if cached is not None:
            return bool(cached)
        exists = False
        try:
            cursor.execute(f"PRAGMA table_info({table_name})")
            exists = any(str(row[1]) == str(column_name) for row in (cursor.fetchall() or []))
        except Exception:
            exists = False
        self._column_exists_cache[key] = bool(exists)
        return bool(exists)

    def _ensure_transaction_order_snapshot_columns(self):
        if self.conn is None:
            return
        cursor = self.conn.cursor()
        if not self._table_exists(cursor, "transaction_orders"):
            return
        for column_name, column_type in (
            ("prequal_cash", "REAL"),
            ("prequal_total_debt", "REAL"),
            ("prequal_owned_property_count", "INTEGER"),
        ):
            if self._column_exists(cursor, "transaction_orders", column_name):
                continue
            try:
                cursor.execute(f"ALTER TABLE transaction_orders ADD COLUMN {column_name} {column_type}")
                self._column_exists_cache[("transaction_orders", column_name)] = True
            except Exception:
                continue
        try:
            self.conn.commit()
        except Exception:
            pass

    def _persist_order_prequalification_snapshot(self, cursor, order_id: int, buyer: Agent):
        if not self._column_exists(cursor, "transaction_orders", "prequal_cash"):
            return
        try:
            cursor.execute(
                """
                UPDATE transaction_orders
                SET prequal_cash=?,
                    prequal_total_debt=?,
                    prequal_owned_property_count=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE order_id=?
                """,
                (
                    float(getattr(buyer, "cash", 0.0) or 0.0),
                    float(getattr(buyer, "total_debt", 0.0) or 0.0),
                    int(len(getattr(buyer, "owned_properties", []) or [])),
                    int(order_id),
                ),
            )
        except Exception:
            return

    def _buyer_state_drifted_since_precheck(
        self,
        buyer: Agent,
        prequal_cash: Optional[float],
        prequal_total_debt: Optional[float],
        prequal_owned_property_count: Optional[int],
    ) -> bool:
        if prequal_cash is None and prequal_total_debt is None and prequal_owned_property_count is None:
            return False
        current_cash = float(getattr(buyer, "cash", 0.0) or 0.0)
        cash_changed = prequal_cash is not None and abs(float(prequal_cash) - current_cash) > 1e-6
        return bool(cash_changed)

    def _precheck_settlement_cash_consistency(
        self,
        buyer: Agent,
        listing: Dict,
        offer_price: float,
    ) -> Tuple[bool, str, Dict]:
        include_fees = self._resolve_precheck_include_tax_and_fee()
        liquidity_months = max(
            self._resolve_precheck_liquidity_buffer_months(),
            self._resolve_settlement_liquidity_floor_months(),
        )
        buyer_total_cost = 0.0
        if include_fees:
            costs = FinancialCalculator.calculate_transaction_costs(
                float(offer_price),
                config=self.config,
                side="buyer",
            )
            buyer_total_cost = float(costs.get("total", 0.0))

        price = float(offer_price)
        cash_now = float(getattr(buyer, "cash", 0.0) or 0.0)
        down_ratio = float(self.config.mortgage.get("down_payment_ratio", 0.3) or 0.3)
        down_payment = max(0.0, price * down_ratio)
        reserve_floor = max(
            0.0,
            float(liquidity_months) * max(0.0, float(getattr(buyer, "monthly_income", 0.0) or 0.0)) * 0.5,
        )
        required_cash = down_payment + float(buyer_total_cost) + reserve_floor
        ok = cash_now + 1e-6 >= required_cash
        metrics = {
            "offer_price": float(price),
            "property_id": int(listing.get("property_id", -1)),
            "buyer_cash": float(cash_now),
            "required_cash": float(required_cash),
            "down_payment": float(down_payment),
            "buyer_total_cost": float(buyer_total_cost),
            "reserve_floor": float(reserve_floor),
            "liquidity_buffer_months": int(liquidity_months),
        }
        return bool(ok), ("PASS" if ok else "CASH_SHORTFALL_PREQUALIFIED"), metrics

    def _results_dir(self) -> str:
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA database_list")
        rows = cursor.fetchall()
        main_db = next((r for r in rows if r[1] == "main"), None)
        db_path = main_db[2] if main_db and len(main_db) > 2 else ""
        if db_path:
            return os.path.dirname(db_path) or "."
        return "."

    def _flush_order_log_buffer(self):
        if not self._order_log_buffer:
            return
        try:
            log_path = os.path.join(self._results_dir(), "order_lifecycle.log")
            with open(log_path, "a", encoding="utf-8") as f:
                for line in self._order_log_buffer:
                    f.write(json.dumps(line, ensure_ascii=False) + "\n")
            self._order_log_buffer.clear()
        except Exception as e:
            logger.debug(f"failed to flush order log buffer: {e}")

    def _append_order_log(self, month: int, event: str, payload: Dict):
        try:
            line = {
                "ts": datetime.datetime.now().isoformat(timespec="seconds"),
                "month": month,
                "event": event,
                **payload,
            }
            self._order_log_buffer.append(line)
            if len(self._order_log_buffer) >= self._order_log_flush_size:
                self._flush_order_log_buffer()
        except Exception as e:
            logger.debug(f"failed to append order log: {e}")

    def _order_config(self) -> Dict[str, float]:
        """Resolve M18/M19 order params with backward-compatible keys."""
        ttl_days = int(self.config.get("smart_agent.order_ttl_days", self.config.get("order_ttl_days", 7)))
        deposit_ratio = float(self.config.get("smart_agent.deposit_ratio", self.config.get("deposit_ratio", 0.10)))
        settlement_delay_days = int(
            self.config.get("smart_agent.settlement_delay_days", self.config.get("settlement_delay_days", 15))
        )
        breach_penalty_ratio = float(
            self.config.get("smart_agent.breach_penalty_ratio", self.config.get("breach_penalty_ratio", 0.05))
        )
        ttl_months = max(1, int(math.ceil(max(1, ttl_days) / 30.0)))
        settlement_delay_months = max(1, int(math.ceil(max(1, settlement_delay_days) / 30.0)))
        return {
            "ttl_days": max(1, ttl_days),
            "ttl_months": ttl_months,
            "settlement_delay_days": max(1, settlement_delay_days),
            "settlement_delay_months": settlement_delay_months,
            "deposit_ratio": max(0.0, min(1.0, deposit_ratio)),
            "breach_penalty_ratio": max(0.0, min(1.0, breach_penalty_ratio)),
        }

    def _seller_deadline_cfg(self) -> Dict[str, float | bool]:
        def _to_bool(raw, default: bool) -> bool:
            if isinstance(raw, bool):
                return raw
            if raw is None:
                return default
            return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

        enabled = _to_bool(
            self.config.get("smart_agent.seller_deadline_enabled", False),
            False,
        )
        hard_clear = _to_bool(
            self.config.get("smart_agent.seller_deadline_hard_clear_enabled", True),
            True,
        )
        try:
            cut_start_ratio = float(self.config.get("smart_agent.seller_deadline_force_price_ratio", 0.88))
        except Exception:
            cut_start_ratio = 0.88
        try:
            cut_floor_ratio = float(self.config.get("smart_agent.seller_deadline_force_floor_ratio", 0.60))
        except Exception:
            cut_floor_ratio = 0.60
        try:
            hard_floor_ratio = float(self.config.get("smart_agent.seller_deadline_hard_floor_ratio", 0.01))
        except Exception:
            hard_floor_ratio = 0.01
        try:
            cut_step = float(self.config.get("smart_agent.seller_deadline_force_step_ratio", 0.05))
        except Exception:
            cut_step = 0.05
        try:
            penalty_ratio = float(self.config.get("smart_agent.seller_deadline_penalty_ratio", 0.08))
        except Exception:
            penalty_ratio = 0.08
        terminal_zero_price = _to_bool(
            self.config.get("smart_agent.seller_deadline_terminal_zero_price_enabled", True),
            True,
        )

        return {
            "enabled": bool(enabled),
            "hard_clear_enabled": bool(hard_clear),
            "cut_start_ratio": max(0.40, min(1.00, float(cut_start_ratio))),
            "cut_floor_ratio": max(0.01, min(0.95, float(cut_floor_ratio))),
            "hard_floor_ratio": max(0.0001, min(0.50, float(hard_floor_ratio))),
            "cut_step_ratio": max(0.01, min(0.20, float(cut_step))),
            "penalty_ratio": max(0.0, min(0.50, float(penalty_ratio))),
            "terminal_zero_price_enabled": bool(terminal_zero_price),
        }

    @staticmethod
    def _buyer_affordability_score(buyer: Agent) -> float:
        # Heuristic ranking only; final feasibility still uses affordability checks.
        cash = float(getattr(buyer, "cash", 0.0) or 0.0)
        income = float(getattr(buyer, "monthly_income", 0.0) or 0.0)
        debt = float(getattr(buyer, "total_debt", 0.0) or 0.0)
        return float(cash + income * 48.0 - debt * 0.2)

    def _force_sell_due_listings(
        self,
        cursor,
        month: int,
        buyers: List[Agent],
        agent_map: Dict[int, Agent],
        props_map: Dict[int, Dict],
        market,
    ) -> int:
        cfg = self._seller_deadline_cfg()
        if not bool(cfg.get("enabled", False)) or not bool(cfg.get("hard_clear_enabled", False)):
            return 0

        try:
            cursor.execute(
                """
                SELECT property_id, owner_id, listed_price, min_price, current_valuation,
                       sell_deadline_month, sell_deadline_total_months, forced_sale_mode
                FROM properties_market
                WHERE status='for_sale'
                  AND owner_id IS NOT NULL
                  AND owner_id > 0
                  AND sell_deadline_month IS NOT NULL
                  AND sell_deadline_month <= ?
                  AND COALESCE(forced_sale_mode, 0) = 1
                """,
                (int(month),),
            )
        except Exception:
            return 0
        due_rows = cursor.fetchall() or []
        if not due_rows:
            return 0

        from mortgage_system import check_affordability
        from transaction_engine import execute_transaction

        class _ForcedClearConfig:
            def __init__(self, base_config, cash_only: bool = False, ultra_permissive: bool = False):
                self._base = base_config
                base_mortgage = dict(getattr(base_config, "mortgage", {}) or {})
                if ultra_permissive:
                    base_mortgage["down_payment_ratio"] = 0.0
                    base_mortgage["annual_interest_rate"] = 0.0
                    base_mortgage["loan_term_years"] = 360
                    base_mortgage["max_dti_ratio"] = 9999.0
                if cash_only:
                    base_mortgage["down_payment_ratio"] = 1.0
                    base_mortgage["max_dti_ratio"] = 1.0
                self.mortgage = base_mortgage
                self._ultra = bool(ultra_permissive)

            def get(self, key, default=None):
                if str(key) in {"smart_agent.liquidity_floor_months", "liquidity_floor_months"}:
                    return 0
                if self._ultra and str(key) in {
                    "transaction_costs.buyer.brokerage_ratio",
                    "transaction_costs.buyer.tax_ratio",
                    "transaction_costs.buyer.misc_ratio",
                    "transaction_costs.seller.brokerage_ratio",
                    "transaction_costs.seller.tax_ratio",
                    "transaction_costs.seller.misc_ratio",
                }:
                    return 0.0
                if self._ultra and str(key) in {"smart_agent.leverage_cap", "leverage_cap"}:
                    return None
                return self._base.get(key, default)

        completed = 0
        forced_cfg = _ForcedClearConfig(self.config, cash_only=False)
        forced_cash_only_cfg = _ForcedClearConfig(self.config, cash_only=True)
        forced_ultra_cfg = _ForcedClearConfig(self.config, cash_only=False, ultra_permissive=True)

        candidate_pool: Dict[int, Agent] = {}
        for b in (buyers or []):
            if b is None:
                continue
            try:
                bid = int(getattr(b, "id", -1))
            except Exception:
                bid = -1
            if bid > 0:
                candidate_pool[bid] = b
        for aid, a in (agent_map or {}).items():
            if a is None:
                continue
            try:
                bid = int(getattr(a, "id", aid))
            except Exception:
                continue
            if bid > 0:
                candidate_pool[bid] = a

        all_candidates = list(candidate_pool.values())
        all_candidates.sort(key=self._buyer_affordability_score, reverse=True)
        cursor.execute(
            """
            SELECT buyer_id, COUNT(*)
            FROM transaction_orders
            WHERE status IN ('pending', 'pending_settlement')
            GROUP BY buyer_id
            """
        )
        blocked_buyer_order_counts = {
            int(r[0]): int(r[1] or 0)
            for r in (cursor.fetchall() or [])
            if r and r[0] is not None
        }

        for row in due_rows:
            pid = int(row[0])
            seller_id = int(row[1])
            listed_price = float(row[2] or 0.0)
            min_price = float(row[3] or 0.0)
            valuation = float(row[4] or 0.0)
            deadline_month = int(row[5]) if row[5] is not None else int(month)
            deadline_total = int(row[6]) if row[6] is not None else 0
            forced_mode = int(row[7] or 0)
            if forced_mode != 1:
                continue

            seller = agent_map.get(int(seller_id))
            if seller is None:
                continue

            candidate_buyers = []
            blocked_candidate_ids = []
            for b in all_candidates:
                buyer_id = int(getattr(b, "id", -1))
                if buyer_id == int(seller_id):
                    continue
                if int(blocked_buyer_order_counts.get(buyer_id, 0) or 0) > 0:
                    blocked_candidate_ids.append(int(buyer_id))
                    continue
                candidate_buyers.append(b)
            if not candidate_buyers:
                self._append_order_log(
                    month,
                    "DEADLINE_FORCED_SALE_SKIPPED",
                    {
                        "property_id": int(pid),
                        "seller_id": int(seller_id),
                        "deadline_month": int(deadline_month),
                        "reason": "no_real_buyer_exists_in_system",
                        "blocked_pending_buyers": blocked_candidate_ids,
                    },
                )
                continue

            property_data = props_map.get(pid)
            if property_data is None:
                cursor.execute(
                    """
                    SELECT ps.zone, ps.building_area, ps.is_school_district, ps.property_type
                    FROM properties_static ps
                    WHERE ps.property_id=?
                    """,
                    (pid,),
                )
                p_row = cursor.fetchone()
                property_data = {
                    "property_id": pid,
                    "owner_id": seller_id,
                    "status": "for_sale",
                    "zone": str(p_row[0] if p_row else ""),
                    "building_area": float(p_row[1] if p_row and p_row[1] is not None else 0.0),
                    "is_school_district": bool(int(p_row[2] or 0)) if p_row else False,
                    "property_type": str(p_row[3] if p_row and p_row[3] is not None else ""),
                }
                props_map[pid] = property_data

            base_price = max(1.0, float(listed_price or 0.0), float(min_price or 0.0), float(valuation or 0.0))
            start_ratio = float(cfg.get("cut_start_ratio", 0.88))
            floor_ratio = float(cfg.get("cut_floor_ratio", 0.60))
            hard_floor_ratio = float(cfg.get("hard_floor_ratio", 0.01))
            step_ratio = float(cfg.get("cut_step_ratio", 0.05))
            if floor_ratio > start_ratio:
                floor_ratio = start_ratio
            if hard_floor_ratio > floor_ratio:
                hard_floor_ratio = floor_ratio

            selected_buyer = None
            selected_price = None
            selected_mode = "normal"
            selected_exec_config = forced_cfg
            selected_ratio = None

            def _try_pick_buyer(trial_price: float, cfg_obj) -> Agent | None:
                for buyer in candidate_buyers:
                    buyer_costs = FinancialCalculator.calculate_transaction_costs(
                        trial_price, config=cfg_obj, side="buyer"
                    )
                    buyer_total_cost = float(buyer_costs.get("total", 0.0))
                    ok, _, _ = check_affordability(
                        buyer,
                        trial_price,
                        cfg_obj,
                        extra_cash_needed=buyer_total_cost,
                        liquidity_floor_months_override=0,
                    )
                    if ok:
                        return buyer
                return None

            # Stage-1: configured force-cut window.
            ratio = float(start_ratio)
            while ratio >= float(floor_ratio) - 1e-9 and selected_buyer is None:
                trial_price = max(1.0, round(base_price * ratio, 2))
                picked = _try_pick_buyer(trial_price, forced_cfg)
                if picked is not None:
                    selected_buyer = picked
                    selected_price = float(trial_price)
                    selected_mode = "normal"
                    selected_exec_config = forced_cfg
                    selected_ratio = float(ratio)
                    break
                ratio -= float(step_ratio)

            # Stage-2: hard crash lane (bone-crush down to hard floor).
            ratio = float(floor_ratio - step_ratio)
            while ratio >= float(hard_floor_ratio) - 1e-9 and selected_buyer is None:
                trial_price = max(1.0, round(base_price * ratio, 2))
                picked = _try_pick_buyer(trial_price, forced_cfg)
                if picked is not None:
                    selected_buyer = picked
                    selected_price = float(trial_price)
                    selected_mode = "normal"
                    selected_exec_config = forced_cfg
                    selected_ratio = float(ratio)
                    break
                ratio -= float(step_ratio)

            # Stage-3: cash-only fallback at ultra-low prices.
            if selected_buyer is None:
                emergency_prices = [
                    max(1.0, round(base_price * max(0.0001, float(hard_floor_ratio)), 2)),
                    100000.0,
                    50000.0,
                    20000.0,
                    10000.0,
                    5000.0,
                    1000.0,
                    100.0,
                    10.0,
                    1.0,
                ]
                seen_prices = set()
                for p in emergency_prices:
                    trial_price = max(1.0, round(float(p), 2))
                    if trial_price in seen_prices:
                        continue
                    seen_prices.add(trial_price)
                    picked = _try_pick_buyer(trial_price, forced_cash_only_cfg)
                    if picked is not None:
                        selected_buyer = picked
                        selected_price = float(trial_price)
                        selected_mode = "cash_only_fallback"
                        selected_exec_config = forced_cash_only_cfg
                        selected_ratio = float(trial_price / max(1.0, base_price))
                        break

            # Stage-4: terminal hard clear (must-sell) to a real buyer with zero-price custody transfer.
            # This path exists to honor "deadline month must be sold" without creating fake buyers.
            if (
                selected_buyer is None
                and bool(cfg.get("terminal_zero_price_enabled", True))
                and candidate_buyers
            ):
                trial_price = 0.0
                picked = _try_pick_buyer(trial_price, forced_ultra_cfg)
                if picked is not None:
                    selected_buyer = picked
                    selected_price = float(trial_price)
                    selected_mode = "terminal_zero_price_clear"
                    selected_exec_config = forced_ultra_cfg
                    selected_ratio = 0.0

            if selected_buyer is None or selected_price is None:
                self._append_order_log(
                    month,
                    "DEADLINE_FORCED_SALE_SKIPPED",
                    {
                        "property_id": int(pid),
                        "seller_id": int(seller_id),
                        "deadline_month": int(deadline_month),
                        "deadline_total_months": int(deadline_total),
                        "reason": "no_affordable_real_buyer_found_even_after_hard_crash",
                        "start_ratio": float(start_ratio),
                        "floor_ratio": float(floor_ratio),
                        "hard_floor_ratio": float(hard_floor_ratio),
                    },
                )
                continue

            tx_record = execute_transaction(
                selected_buyer,
                seller,
                property_data,
                float(selected_price),
                market,
                config=selected_exec_config,
            )
            if not tx_record:
                tx_error_code = str(getattr(selected_buyer, "_last_tx_error_code", "") or "")
                self._append_order_log(
                    month,
                    "DEADLINE_FORCED_SALE_SKIPPED",
                    {
                        "property_id": int(pid),
                        "seller_id": int(seller_id),
                        "buyer_id": int(getattr(selected_buyer, "id", -1)),
                        "reason": "execute_transaction_failed",
                        "error_code": tx_error_code,
                    },
                )
                continue

            penalty_ratio = float(cfg.get("penalty_ratio", 0.0) or 0.0)
            tx_price = float(tx_record.get("price", selected_price))
            penalty_base = max(0.0, float(listed_price), float(tx_price))
            penalty_amount = round(max(0.0, penalty_base * penalty_ratio), 2)
            if penalty_amount > 0:
                seller.cash = max(0.0, float(getattr(seller, "cash", 0.0) or 0.0) - float(penalty_amount))

            # Close any dangling orders on this listing to keep order tables consistent.
            cursor.execute(
                """
                SELECT order_id, buyer_id, deposit_amount
                FROM transaction_orders
                WHERE property_id=? AND status IN ('pending', 'pending_settlement')
                """,
                (int(pid),),
            )
            pending_orders = cursor.fetchall() or []
            for oid, bid, deposit in pending_orders:
                buyer_obj = agent_map.get(int(bid)) if bid is not None else None
                if buyer_obj is None:
                    continue
                self._close_order(
                    cursor,
                    month=month,
                    order_id=int(oid),
                    buyer=buyer_obj,
                    status="cancelled",
                    close_reason="Deadline forced sale completed",
                    release_amount=float(deposit or 0.0),
                    penalty_amount=0.0,
                )

            try:
                cursor.execute(
                    """
                    INSERT INTO transactions
                    (month, order_id, buyer_id, seller_id, property_id, final_price, down_payment, loan_amount,
                     buyer_transaction_cost, seller_transaction_cost, negotiation_rounds)
                    VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        int(month),
                        int(getattr(selected_buyer, "id", -1)),
                        int(seller_id),
                        int(pid),
                        float(tx_record.get("price", selected_price)),
                        float(tx_record.get("down_payment", 0.0)),
                        float(tx_record.get("loan_amount", 0.0)),
                        float(tx_record.get("buyer_transaction_cost", 0.0)),
                        float(tx_record.get("seller_transaction_cost", 0.0)),
                    ),
                )
            except sqlite3.OperationalError:
                cursor.execute(
                    """
                    INSERT INTO transactions
                    (month, order_id, buyer_id, seller_id, property_id, final_price, down_payment, loan_amount, negotiation_rounds)
                    VALUES (?, NULL, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        int(month),
                        int(getattr(selected_buyer, "id", -1)),
                        int(seller_id),
                        int(pid),
                        float(tx_record.get("price", selected_price)),
                        float(tx_record.get("down_payment", 0.0)),
                        float(tx_record.get("loan_amount", 0.0)),
                    ),
                )

            cursor.execute(
                """
                UPDATE properties_market
                SET status='off_market',
                    owner_id=?,
                    current_valuation=?,
                    last_transaction_month=?,
                    last_price_update_month=?,
                    last_price_update_reason=?
                WHERE property_id=?
                """,
                (
                    int(getattr(selected_buyer, "id", -1)),
                    float(tx_record.get("price", selected_price)),
                    int(month),
                    int(month),
                    "Seller deadline forced sale",
                    int(pid),
                ),
            )

            property_data["owner_id"] = int(getattr(selected_buyer, "id", -1))
            property_data["status"] = "off_market"
            property_data["last_transaction_price"] = float(tx_record.get("price", selected_price))

            self._sync_buyer_finance(cursor, selected_buyer)
            self._sync_buyer_finance(cursor, seller)

            self._enqueue_decision_log(
                (
                    int(seller_id),
                    int(month),
                    "SELLER_DEADLINE",
                    "FORCED_SALE",
                    f"deadline_month={deadline_month}, forced sale executed ({selected_mode})",
                    None,
                    json.dumps(
                        {
                            "property_id": int(pid),
                            "seller_id": int(seller_id),
                            "buyer_id": int(getattr(selected_buyer, "id", -1)),
                            "final_price": round(float(tx_record.get("price", selected_price)), 2),
                            "listed_price": round(float(listed_price), 2),
                            "min_price": round(float(min_price), 2),
                            "selected_ratio": round(float(selected_ratio or 0.0), 4),
                            "force_sell_mode": str(selected_mode),
                            "penalty_ratio": round(float(penalty_ratio), 4),
                            "penalty_base": round(float(penalty_base), 2),
                            "penalty_amount": round(float(penalty_amount), 2),
                            "sell_deadline_month": int(deadline_month),
                            "sell_deadline_total_months": int(deadline_total),
                        },
                        ensure_ascii=False,
                    ),
                    False,
                )
            )
            self._append_order_log(
                month,
                "DEADLINE_FORCED_SALE",
                {
                    "property_id": int(pid),
                    "seller_id": int(seller_id),
                    "buyer_id": int(getattr(selected_buyer, "id", -1)),
                    "final_price": round(float(tx_record.get("price", selected_price)), 2),
                    "listed_price": round(float(listed_price), 2),
                    "selected_ratio": round(float(selected_ratio or 0.0), 4),
                    "force_sell_mode": str(selected_mode),
                    "penalty_ratio": round(float(penalty_ratio), 4),
                    "penalty_base": round(float(penalty_base), 2),
                    "penalty_amount": round(float(penalty_amount), 2),
                    "sell_deadline_month": int(deadline_month),
                    "sell_deadline_total_months": int(deadline_total),
                },
            )
            completed += 1

        return int(completed)

    @staticmethod
    def _seller_persona_snapshot_from_row(row: Tuple) -> Dict[str, str]:
        return {
            "purchase_motive_primary": str(row[7] or ""),
            "housing_stage": str(row[8] or ""),
            "family_stage": str(row[9] or ""),
            "education_path": str(row[10] or ""),
            "financial_profile": str(row[11] or ""),
            "seller_profile": str(row[12] or ""),
        }

    @staticmethod
    def _listing_property_snapshot_from_row(row: Tuple) -> Dict[str, object]:
        listed_price = float(row[2] or 0.0)
        min_price = float(row[3] if row[3] is not None else listed_price * 0.95)
        price_per_sqm = float(row[15] or 0.0)
        building_area = float(row[13] or 0.0)
        unit_price = listed_price / building_area if building_area > 0 and listed_price > 0 else price_per_sqm
        return {
            "property_id": int(row[0] or -1),
            "zone": str(row[14] or ""),
            "building_area": building_area,
            "is_school_district": int(row[16] or 0),
            "listed_price": listed_price,
            "min_price": min_price,
            "price_per_sqm": round(float(unit_price or 0.0), 2),
        }

    @staticmethod
    def _price_adjust_reason_tags(
        action: str,
        market_trend: str,
        listing_duration: int,
        property_snapshot: Dict[str, object],
        persona_snapshot: Dict[str, str],
        demand_heat_band: str = "",
    ) -> List[str]:
        tags: List[str] = [
            f"action:{str(action or '').upper()}",
            f"trend:{str(market_trend or '').lower()}",
            f"duration:{'stale' if listing_duration >= 4 else 'fresh'}",
            f"zone:{str(property_snapshot.get('zone', '') or '')}",
            f"school:{'yes' if int(property_snapshot.get('is_school_district', 0) or 0) else 'no'}",
        ]
        for key in ("purchase_motive_primary", "housing_stage", "family_stage", "education_path", "financial_profile", "seller_profile"):
            value = str(persona_snapshot.get(key, "") or "")
            if value:
                tags.append(f"{key}:{value}")
        if demand_heat_band:
            tags.append(f"demand_heat:{str(demand_heat_band).upper()}")
        return tags

    def _collect_recent_listing_demand_heat(
        self,
        cursor,
        month: int,
        property_id: int,
        cfg: Dict[str, object] | None = None,
    ) -> Dict[str, object]:
        """
        Build demand-heat context for seller pricing decisions.
        This is evidence/context only; final action still comes from LLM.
        """
        try:
            lookback = int(
                self.config.get(
                    "smart_agent.price_adjustment_demand_lookback_months",
                    self.config.get("price_adjustment_demand_lookback_months", 2),
                )
            )
        except Exception:
            lookback = 2
        lookback = max(1, min(3, lookback))
        start_month = max(1, int(month) - lookback)
        end_month = max(1, int(month) - 1)
        if end_month < start_month:
            end_month = start_month

        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS matches,
                    COUNT(DISTINCT buyer_id) AS interest_buyers,
                    SUM(CASE WHEN is_valid_bid=1 THEN 1 ELSE 0 END) AS valid_bids,
                    SUM(CASE WHEN proceeded_to_negotiation=1 THEN 1 ELSE 0 END) AS negotiation_entries,
                    SUM(CASE WHEN COALESCE(failure_reason,'') LIKE 'Outbid:%' THEN 1 ELSE 0 END) AS outbid_losses,
                    MAX(CASE WHEN is_valid_bid=1 THEN buyer_bid ELSE NULL END) AS best_valid_bid
                FROM property_buyer_matches
                WHERE property_id=? AND month BETWEEN ? AND ?
                """,
                (int(property_id), int(start_month), int(end_month)),
            )
            row = cursor.fetchone() or (0, 0, 0, 0, 0, None)
        except Exception:
            row = (0, 0, 0, 0, 0, None)

        matches = int(row[0] or 0)
        interest_buyers = int(row[1] or 0)
        valid_bids = int(row[2] or 0)
        negotiation_entries = int(row[3] or 0)
        outbid_losses = int(row[4] or 0)
        best_valid_bid = float(row[5] or 0.0)

        property_score = (
            0.34 * min(1.0, valid_bids / 3.0)
            + 0.26 * min(1.0, negotiation_entries / 2.0)
            + 0.24 * min(1.0, outbid_losses / 2.0)
            + 0.16 * min(1.0, matches / 5.0)
        )

        zone = ""
        try:
            cursor.execute("SELECT zone FROM properties_static WHERE property_id=?", (int(property_id),))
            z = cursor.fetchone()
            zone = str((z[0] if z else "") or "")
        except Exception:
            zone = ""

        zone_matches = 0
        zone_valid_bids = 0
        zone_negotiation_entries = 0
        zone_outbid_losses = 0
        zone_score = 0.0
        if zone:
            try:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS matches,
                        SUM(CASE WHEN pbm.is_valid_bid=1 THEN 1 ELSE 0 END) AS valid_bids,
                        SUM(CASE WHEN pbm.proceeded_to_negotiation=1 THEN 1 ELSE 0 END) AS negotiation_entries,
                        SUM(CASE WHEN COALESCE(pbm.failure_reason,'') LIKE 'Outbid:%' THEN 1 ELSE 0 END) AS outbid_losses
                    FROM property_buyer_matches pbm
                    JOIN properties_static ps ON ps.property_id = pbm.property_id
                    WHERE ps.zone=? AND pbm.month BETWEEN ? AND ?
                    """,
                    (zone, int(start_month), int(end_month)),
                )
                z_row = cursor.fetchone() or (0, 0, 0, 0)
                zone_matches = int(z_row[0] or 0)
                zone_valid_bids = int(z_row[1] or 0)
                zone_negotiation_entries = int(z_row[2] or 0)
                zone_outbid_losses = int(z_row[3] or 0)
            except Exception:
                zone_matches = zone_valid_bids = zone_negotiation_entries = zone_outbid_losses = 0

            zone_match_norm = float((cfg or {}).get("heat_zone_match_norm", 16.0))
            zone_valid_norm = float((cfg or {}).get("heat_zone_valid_bid_norm", 10.0))
            zone_neg_norm = float((cfg or {}).get("heat_zone_negotiation_norm", 8.0))
            zone_outbid_norm = float((cfg or {}).get("heat_zone_outbid_norm", 6.0))
            zone_score = (
                0.34 * min(1.0, zone_valid_bids / max(1.0, zone_valid_norm))
                + 0.26 * min(1.0, zone_negotiation_entries / max(1.0, zone_neg_norm))
                + 0.24 * min(1.0, zone_outbid_losses / max(1.0, zone_outbid_norm))
                + 0.16 * min(1.0, zone_matches / max(1.0, zone_match_norm))
            )

        zone_mix_weight = max(0.0, min(0.80, float((cfg or {}).get("heat_zone_mix_weight", 0.35))))
        score = (1.0 - zone_mix_weight) * float(property_score) + zone_mix_weight * float(zone_score)

        # If the single listing is fresh but the zone is clearly hot, avoid "all-LOW" collapse.
        if matches == 0 and zone_score >= float((cfg or {}).get("heat_property_floor_on_zone_hot", 0.14)):
            score = max(float(score), float((cfg or {}).get("heat_property_floor_on_zone_hot", 0.14)))

        medium_th = max(0.05, min(0.80, float((cfg or {}).get("heat_medium_threshold", 0.16))))
        high_th = max(medium_th + 0.03, min(0.95, float((cfg or {}).get("heat_high_threshold", 0.36))))

        if score >= high_th:
            band = "HIGH"
        elif score >= medium_th:
            band = "MEDIUM"
        else:
            band = "LOW"

        monthly_valid_bids: Dict[int, int] = {m: 0 for m in range(start_month, end_month + 1)}
        try:
            cursor.execute(
                """
                SELECT month, SUM(CASE WHEN is_valid_bid=1 THEN 1 ELSE 0 END) AS valid_bids
                FROM property_buyer_matches
                WHERE property_id=? AND month BETWEEN ? AND ?
                GROUP BY month
                """,
                (int(property_id), int(start_month), int(end_month)),
            )
            for m, c in cursor.fetchall() or []:
                monthly_valid_bids[int(m)] = int(c or 0)
        except Exception:
            pass

        trailing_zero_valid_bid_streak = 0
        for m in range(end_month, start_month - 1, -1):
            if int(monthly_valid_bids.get(m, 0)) == 0:
                trailing_zero_valid_bid_streak += 1
            else:
                break

        return {
            "lookback_months": int(lookback),
            "window_start_month": int(start_month),
            "window_end_month": int(end_month),
            "zone": zone,
            "matches": matches,
            "interest_buyers": interest_buyers,
            "valid_bids": valid_bids,
            "negotiation_entries": negotiation_entries,
            "outbid_losses": outbid_losses,
            "best_valid_bid": round(float(best_valid_bid), 2),
            "property_score": round(float(property_score), 4),
            "zone_matches": zone_matches,
            "zone_valid_bids": zone_valid_bids,
            "zone_negotiation_entries": zone_negotiation_entries,
            "zone_outbid_losses": zone_outbid_losses,
            "zone_score": round(float(zone_score), 4),
            "zone_mix_weight": round(float(zone_mix_weight), 3),
            "medium_threshold": round(float(medium_th), 3),
            "high_threshold": round(float(high_th), 3),
            "score": round(float(score), 4),
            "band": band,
            "trailing_zero_valid_bid_streak": int(trailing_zero_valid_bid_streak),
            "latest_month_valid_bids": int(monthly_valid_bids.get(end_month, 0)),
        }

    def _collect_cold_listing_signal(
        self,
        cursor,
        month: int,
        property_id: int,
        cfg: Dict[str, object] | None = None,
    ) -> Dict[str, object]:
        lookback = max(1, min(4, int((cfg or {}).get("cold_house_lookback_months", 2))))
        streak_gate = max(1, min(4, int((cfg or {}).get("cold_house_no_match_streak", 2))))
        start_month = max(1, int(month) - lookback)
        end_month = max(1, int(month) - 1)
        if end_month < start_month:
            end_month = start_month

        counts_by_month: Dict[int, int] = {m: 0 for m in range(start_month, end_month + 1)}
        try:
            cursor.execute(
                """
                SELECT month, COUNT(*) AS c
                FROM property_buyer_matches
                WHERE property_id=? AND month BETWEEN ? AND ?
                GROUP BY month
                """,
                (int(property_id), int(start_month), int(end_month)),
            )
            for m, c in cursor.fetchall() or []:
                counts_by_month[int(m)] = int(c or 0)
        except Exception:
            pass

        trailing_zero_streak = 0
        for m in range(end_month, start_month - 1, -1):
            if int(counts_by_month.get(m, 0)) == 0:
                trailing_zero_streak += 1
            else:
                break

        total_matches = int(sum(counts_by_month.values()))
        is_cold = trailing_zero_streak >= streak_gate
        return {
            "lookback_months": int(lookback),
            "window_start_month": int(start_month),
            "window_end_month": int(end_month),
            "total_matches": int(total_matches),
            "trailing_zero_streak": int(trailing_zero_streak),
            "no_match_streak_gate": int(streak_gate),
            "is_cold": bool(is_cold),
        }

    def _price_adjust_cfg(self) -> Dict[str, object]:
        def _to_int(key: str, default: int) -> int:
            try:
                return int(self.config.get(key, default))
            except Exception:
                return int(default)

        def _to_bool(key: str, default: bool) -> bool:
            raw = self.config.get(key, default)
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

        try:
            monthly_cap = int(
                self.config.get(
                    "smart_agent.price_adjustment_monthly_llm_cap",
                    self.config.get("price_adjustment_monthly_llm_cap", 20),
                )
            )
        except Exception:
            monthly_cap = 20
        try:
            llm_min_duration = int(
                self.config.get(
                    "smart_agent.price_adjustment_llm_min_duration",
                    self.config.get("price_adjustment_llm_min_duration", 5),
                )
            )
        except Exception:
            llm_min_duration = 5
        return {
            "regime_engine_v1_enabled": _to_bool("smart_agent.regime_engine_v1_enabled", False),
            "regime_v1_price_reconsider_enabled": _to_bool(
                "smart_agent.regime_v1_price_reconsider_enabled",
                True,
            ),
            "regime_v1_raise_release_preforce_enabled": _to_bool(
                "smart_agent.regime_v1_raise_release_preforce_enabled",
                True,
            ),
            "regime_v1_raise_release_force_sample_enabled": _to_bool(
                "smart_agent.regime_v1_raise_release_force_sample_enabled",
                True,
            ),
            "regime_v1_raise_release_force_sample_ratio": max(
                0.0,
                min(
                    1.0,
                    float(self.config.get("smart_agent.regime_v1_raise_release_force_sample_ratio", 0.25)),
                ),
            ),
            "regime_v1_raise_release_force_sample_min_hits_per_month": max(
                0,
                min(
                    20,
                    _to_int(
                        "smart_agent.regime_v1_raise_release_force_sample_min_hits_per_month",
                        0,
                    ),
                ),
            ),
            "regime_v1_raise_release_routing_mode": str(
                self.config.get("smart_agent.regime_v1_raise_release_routing_mode", "ratio")
            ).strip().lower(),
            "regime_v1_raise_release_topk": max(
                0,
                min(
                    50,
                    _to_int("smart_agent.regime_v1_raise_release_topk", 0),
                ),
            ),
            "regime_v1_raise_release_topk_enabled": _to_bool(
                "smart_agent.regime_v1_raise_release_topk_enabled",
                False,
            ),
            "regime_v1_raise_release_min_valid_bids_filter": max(
                0,
                min(
                    20,
                    _to_int("smart_agent.regime_v1_raise_release_min_valid_bids_filter", 1),
                ),
            ),
            "regime_v1_raise_release_min_outbid_filter": max(
                0,
                min(
                    20,
                    _to_int("smart_agent.regime_v1_raise_release_min_outbid_filter", 1),
                ),
            ),
            "regime_v1_raise_release_min_negotiation_filter": max(
                0,
                min(
                    20,
                    _to_int("smart_agent.regime_v1_raise_release_min_negotiation_filter", 1),
                ),
            ),
            "regime_v1_raise_release_exclude_cold_zero_bid": _to_bool(
                "smart_agent.regime_v1_raise_release_exclude_cold_zero_bid",
                True,
            ),
            "regime_v1_raise_release_require_competition_evidence": _to_bool(
                "smart_agent.regime_v1_raise_release_require_competition_evidence",
                True,
            ),
            "regime_v1_raise_release_zero_valid_bid_streak_block_months": max(
                0,
                min(
                    4,
                    _to_int("smart_agent.regime_v1_raise_release_zero_valid_bid_streak_block_months", 2),
                ),
            ),
            "price_adjustment_llm_min_calls_per_month": max(
                0,
                min(
                    200,
                    _to_int("smart_agent.price_adjustment_llm_min_calls_per_month", 0),
                ),
            ),
            "regime_v1_raise_force_all_early_llm": _to_bool(
                "smart_agent.regime_v1_raise_force_all_early_llm",
                False,
            ),
            "regime_v1_raise_force_rule_path_enabled": _to_bool(
                "smart_agent.regime_v1_raise_force_rule_path_enabled",
                False,
            ),
            "regime_v1_raise_force_rule_coeff": max(
                1.01,
                min(1.12, float(self.config.get("smart_agent.regime_v1_raise_force_rule_coeff", 1.05))),
            ),
            "regime_v1_raise_release_cap_reserved": max(
                0,
                min(10, _to_int("smart_agent.regime_v1_raise_release_cap_reserved", 3)),
            ),
            "regime_v1_raise_release_force_llm_min_duration": max(
                1,
                min(
                    4,
                    _to_int("smart_agent.regime_v1_raise_release_force_llm_min_duration", 1),
                ),
            ),
            "normal_rule_enabled": _to_bool("smart_agent.normal_seller_rule_pricing_enabled", True),
            "smart_llm_enabled": _to_bool("smart_agent.price_adjustment_llm_enabled_for_smart", True),
            # Keep short-gate runs observable: allow month-1 LLM adjustment when configured.
            "llm_min_duration": max(1, llm_min_duration),
            "monthly_llm_cap": max(0, monthly_cap),
            "rule_mild_cut": max(0.80, min(0.999, float(self.config.get("smart_agent.price_adjustment_rule_mild_cut", 0.96)))),
            "rule_deep_cut": max(0.70, min(0.995, float(self.config.get("smart_agent.price_adjustment_rule_deep_cut", 0.92)))),
            "rule_downtrend_cut": max(0.70, min(0.995, float(self.config.get("smart_agent.price_adjustment_rule_downtrend_cut", 0.95)))),
            "deadline_hard_cut": max(
                0.50,
                min(0.95, float(self.config.get("smart_agent.seller_deadline_hard_cut_ratio", 0.75))),
            ),
            "deadline_soft_cut": max(
                0.60,
                min(0.98, float(self.config.get("smart_agent.seller_deadline_soft_cut_ratio", 0.90))),
            ),
            # Demand-heat calibration (P0 guardrail tuning).
            "heat_medium_threshold": max(
                0.05,
                min(0.80, float(self.config.get("smart_agent.price_adjustment_heat_medium_threshold", 0.16))),
            ),
            "heat_high_threshold": max(
                0.10,
                min(0.95, float(self.config.get("smart_agent.price_adjustment_heat_high_threshold", 0.36))),
            ),
            "heat_zone_mix_weight": max(
                0.0,
                min(0.80, float(self.config.get("smart_agent.price_adjustment_heat_zone_mix_weight", 0.35))),
            ),
            "heat_zone_match_norm": max(
                1.0,
                float(self.config.get("smart_agent.price_adjustment_heat_zone_match_norm", 16.0)),
            ),
            "heat_zone_valid_bid_norm": max(
                1.0,
                float(self.config.get("smart_agent.price_adjustment_heat_zone_valid_bid_norm", 10.0)),
            ),
            "heat_zone_negotiation_norm": max(
                1.0,
                float(self.config.get("smart_agent.price_adjustment_heat_zone_negotiation_norm", 8.0)),
            ),
            "heat_zone_outbid_norm": max(
                1.0,
                float(self.config.get("smart_agent.price_adjustment_heat_zone_outbid_norm", 6.0)),
            ),
            "heat_property_floor_on_zone_hot": max(
                0.0,
                min(0.6, float(self.config.get("smart_agent.price_adjustment_heat_property_floor_on_zone_hot", 0.14))),
            ),
            "price_adjustment_model_type": str(
                self.config.get("smart_agent.price_adjustment_model_type", "fast")
            ).strip().lower(),
            "price_adjustment_high_heat_model_type": str(
                self.config.get("smart_agent.price_adjustment_high_heat_model_type", "smart")
            ).strip().lower(),
            "lag_heat_compensation_enabled": _to_bool(
                "smart_agent.regime_v1_hot_signal_lag_compensation_enabled",
                False,
            ),
            "lag_heat_compensation_medium_delta_base": max(
                0.0,
                min(
                    0.20,
                    float(self.config.get("smart_agent.regime_v1_hot_signal_lag_compensation_medium_delta_base", 0.03)),
                ),
            ),
            "lag_heat_compensation_medium_delta_scale": max(
                0.0,
                min(
                    0.30,
                    float(self.config.get("smart_agent.regime_v1_hot_signal_lag_compensation_medium_delta_scale", 0.04)),
                ),
            ),
            "lag_heat_compensation_high_delta_base": max(
                0.0,
                min(
                    0.20,
                    float(self.config.get("smart_agent.regime_v1_hot_signal_lag_compensation_high_delta_base", 0.02)),
                ),
            ),
            "lag_heat_compensation_high_delta_scale": max(
                0.0,
                min(
                    0.30,
                    float(self.config.get("smart_agent.regime_v1_hot_signal_lag_compensation_high_delta_scale", 0.03)),
                ),
            ),
            "cold_house_enabled": _to_bool("smart_agent.price_adjustment_cold_house_enabled", True),
            "cold_house_lookback_months": max(
                1,
                min(4, _to_int("smart_agent.price_adjustment_cold_house_lookback_months", 2)),
            ),
            "cold_house_no_match_streak": max(
                1,
                min(4, _to_int("smart_agent.price_adjustment_cold_house_no_match_streak", 2)),
            ),
            "cold_house_min_duration_for_llm": max(
                1,
                min(12, _to_int("smart_agent.price_adjustment_cold_house_min_duration_for_llm", 2)),
            ),
            "dynamic_floor_enabled": _to_bool("smart_agent.dynamic_floor_enabled", True),
            "dynamic_floor_min_ratio": max(
                0.55,
                min(0.95, float(self.config.get("smart_agent.dynamic_floor_min_ratio", 0.70))),
            ),
            "dynamic_floor_max_ratio": max(
                0.80,
                min(0.99, float(self.config.get("smart_agent.dynamic_floor_max_ratio", 0.96))),
            ),
            "dynamic_floor_cold_discount": max(
                0.0,
                min(0.20, float(self.config.get("smart_agent.dynamic_floor_cold_discount", 0.08))),
            ),
            "dynamic_floor_deadline_discount": max(
                0.0,
                min(0.20, float(self.config.get("smart_agent.dynamic_floor_deadline_discount", 0.10))),
            ),
        }

    def _build_price_adjust_scorecard(
        self,
        *,
        current_price: float,
        demand_context: Dict[str, object],
        deadline_months_left: int | None,
        listing_duration: int,
    ) -> Dict[str, object]:
        valid_bids = int((demand_context or {}).get("valid_bids", 0) or 0)
        matches = int((demand_context or {}).get("matches", 0) or 0)
        negotiations = int((demand_context or {}).get("negotiation_entries", 0) or 0)
        best_valid_bid = float((demand_context or {}).get("best_valid_bid", 0.0) or 0.0)
        next_month_holding_cost = float(max(1.0, current_price) * 0.005)
        holding_pressure = self._clamp01(
            next_month_holding_cost / max(1.0, float(current_price) * 0.015)
        ) * 100.0
        sale_prob_proxy = self._clamp01(
            0.30 * min(1.0, matches / 4.0)
            + 0.35 * min(1.0, valid_bids / 2.0)
            + 0.20 * min(1.0, negotiations / 2.0)
            + 0.15 * min(1.0, best_valid_bid / max(1.0, current_price))
        ) * 100.0
        liquidity_risk = 100.0 - sale_prob_proxy
        if deadline_months_left is None:
            deadline_pressure = 35.0
        elif deadline_months_left <= 1:
            deadline_pressure = 100.0
        elif deadline_months_left == 2:
            deadline_pressure = 85.0
        elif deadline_months_left == 3:
            deadline_pressure = 70.0
        elif deadline_months_left == 4:
            deadline_pressure = 55.0
        else:
            deadline_pressure = 40.0
        stale_pressure = self._clamp01(float(listing_duration) / 8.0) * 100.0
        weights = {
            "holding_pressure": 0.30,
            "liquidity_risk": 0.30,
            "deadline_pressure": 0.20,
            "stale_pressure": 0.20,
        }
        close_priority_score = (
            holding_pressure * weights["holding_pressure"]
            + liquidity_risk * weights["liquidity_risk"]
            + deadline_pressure * weights["deadline_pressure"]
            + stale_pressure * weights["stale_pressure"]
        )
        return {
            "holding_pressure_score": float(round(holding_pressure, 4)),
            "liquidity_risk_score": float(round(liquidity_risk, 4)),
            "deadline_pressure_score": float(round(deadline_pressure, 4)),
            "stale_pressure_score": float(round(stale_pressure, 4)),
            "close_priority_score": float(round(close_priority_score, 4)),
            "weights": weights,
        }

    def _resolve_dynamic_floor_ratio(
        self,
        *,
        old_listed: float,
        old_min: float,
        action: str,
        demand_context: Dict[str, object],
        deadline_months_left: int | None,
        listing_duration: int,
        cfg: Dict[str, object],
    ) -> float:
        old_listed_safe = max(1.0, float(old_listed or 0.0))
        base_ratio = max(0.50, min(0.99, float(old_min or old_listed_safe) / old_listed_safe))
        if not bool(cfg.get("dynamic_floor_enabled", True)):
            return max(0.50, min(0.99, base_ratio))

        valid_bids = int((demand_context or {}).get("valid_bids", 0) or 0)
        negotiations = int((demand_context or {}).get("negotiation_entries", 0) or 0)
        matches = int((demand_context or {}).get("matches", 0) or 0)
        demand_cold = (valid_bids == 0 and negotiations == 0 and matches <= 1)

        ratio = float(base_ratio)
        if action == "C":
            ratio -= 0.04
        elif action == "B":
            ratio -= 0.02
        elif action in {"E", "F"}:
            ratio += 0.01

        if demand_cold and int(max(0, listing_duration)) >= 3:
            ratio -= float(cfg.get("dynamic_floor_cold_discount", 0.08))
        if deadline_months_left is not None and int(deadline_months_left) <= 2:
            ratio -= float(cfg.get("dynamic_floor_deadline_discount", 0.10))
        elif deadline_months_left is not None and int(deadline_months_left) <= 4:
            ratio -= 0.03

        ratio = max(float(cfg.get("dynamic_floor_min_ratio", 0.70)), ratio)
        ratio = min(float(cfg.get("dynamic_floor_max_ratio", 0.96)), ratio)
        return max(0.50, min(0.99, float(ratio)))

    def _rule_price_adjustment_decision(
        self,
        *,
        current_price: float,
        listing_duration: int,
        market_trend: str,
        observed_market_trend: str | None,
        recent_demand_context: Dict[str, object] | None,
        decision_profile: str,
        deadline_months_left: int | None,
        cfg: Dict[str, object],
    ) -> tuple[Dict, Dict]:
        actual_trend = str(market_trend or "STABLE").upper()
        trend = str(observed_market_trend or market_trend or "STABLE").upper()
        demand_ctx = dict(recent_demand_context or {})
        profile = str(decision_profile or "normal").strip().lower()
        dur = int(max(0, listing_duration))
        deadline_left = int(deadline_months_left) if deadline_months_left is not None else None
        action = "A"
        coefficient = 1.0
        reason = "挂牌时间较短，先维持价格观察市场反馈。"

        mild = float(cfg.get("rule_mild_cut", 0.96))
        deep = float(cfg.get("rule_deep_cut", 0.92))
        down_cut = float(cfg.get("rule_downtrend_cut", 0.95))
        deadline_hard_cut = max(0.50, min(0.95, float(cfg.get("deadline_hard_cut", 0.75))))
        deadline_soft_cut = max(0.60, min(0.98, float(cfg.get("deadline_soft_cut", 0.90))))
        heat_band = str(demand_ctx.get("band", "LOW") or "LOW").upper()
        valid_bids = int(demand_ctx.get("valid_bids", 0) or 0)
        outbid_losses = int(demand_ctx.get("outbid_losses", 0) or 0)
        negotiations = int(demand_ctx.get("negotiation_entries", 0) or 0)
        best_valid_bid = float(demand_ctx.get("best_valid_bid", 0.0) or 0.0)
        competition_strength = valid_bids + outbid_losses + negotiations
        current_price_safe = max(1.0, float(current_price or 0.0))
        price_close_to_list = best_valid_bid >= current_price_safe * 0.99

        if deadline_left is not None and deadline_left <= 1:
            action, coefficient, reason = (
                "C",
                deadline_hard_cut,
                "已到售出期限最后一个月，执行强降价确保本月去化。",
            )
        elif deadline_left is not None and deadline_left <= 2:
            action, coefficient, reason = (
                "B",
                deadline_soft_cut,
                "售出期限即将到期，先执行较明显降价加速成交。",
            )
        elif profile != "smart" and deadline_left is not None and deadline_left > 2 and (
            trend in {"UP", "BOOM", "PANIC_UP"} or heat_band in {"HIGH", "MEDIUM", "MED"}
        ):
            if heat_band == "HIGH" and (price_close_to_list or competition_strength >= 3):
                action, coefficient, reason = (
                    "F",
                    1.06 if price_close_to_list else 1.05,
                    "规则兜底观察到竞争升温且报价贴近挂牌，按低信息跟风路径明显提价。",
                )
            elif valid_bids >= 1 or outbid_losses >= 1:
                action, coefficient, reason = (
                    "E",
                    1.03,
                    "规则兜底观察到周边有人抢房/报价贴近挂牌，按低信息跟风路径小幅提价。",
                )
        elif trend in {"DOWN", "PANIC"}:
            if dur >= 6:
                action, coefficient, reason = (
                    "C",
                    down_cut if down_cut < mild else deep,
                    "市场走弱且挂牌偏久，执行较大幅度降价以提升成交概率。",
                )
            elif dur >= 4:
                action, coefficient, reason = (
                    "B",
                    down_cut,
                    "市场偏弱且挂牌已久，进行小幅降价试探需求。",
                )
        elif trend == "STABLE":
            if dur >= 6:
                action, coefficient, reason = (
                    "C",
                    deep,
                    "市场平稳但房源滞销较久，采用中等降价改善流动性。",
                )
            elif dur >= 4:
                action, coefficient, reason = (
                    "B",
                    mild,
                    "市场平稳且挂牌已久，执行温和降价提高曝光转化。",
                )
        else:  # UP / BOOM 等
            if dur >= 6:
                action, coefficient, reason = (
                    "B",
                    max(mild, 0.98),
                    "市场偏强但挂牌偏久，仅做轻微降价以加快成交。",
                )

        new_price = float(current_price)
        if action in {"B", "C"}:
            new_price = float(current_price) * float(coefficient)

        return (
            {
                "action": action,
                "coefficient": float(coefficient),
                "new_price": float(new_price),
                "reason": reason,
            },
            {
                "pricing_mode": "rule",
                "rule_listing_duration": int(dur),
                "rule_market_trend": actual_trend,
                "rule_observed_market_trend": trend,
                "rule_decision_profile": profile,
                "rule_heat_band": heat_band,
                "deadline_months_left": int(deadline_left) if deadline_left is not None else None,
            },
        )

    def _resolve_bid_aggressiveness(self, buyer: Agent) -> float:
        """
        Resolve buyer aggressiveness for rush-sale queue ranking.
        Smart agents can be more aggressive; normal agents keep low baseline.
        """
        if getattr(buyer, "agent_type", "normal") != "smart":
            return 0.1
        raw = self.config.get("smart_agent.bid_aggressiveness", self.config.get("bid_aggressiveness", 0.5))
        try:
            return max(0.0, min(1.0, float(raw)))
        except Exception:
            return 0.5

    def _resolve_tie_breaker_mode(self) -> str:
        raw = str(self.config.get("smart_agent.tie_breaker_mode", self.config.get("tie_breaker_mode", "random"))).lower()
        return raw if raw in ("random", "rotation") else "random"

    @staticmethod
    def _clamp01(v: float) -> float:
        try:
            return max(0.0, min(1.0, float(v)))
        except Exception:
            return 0.0

    @staticmethod
    def _extract_first_buyer_offer_by_id(history: List[Dict]) -> Dict[int, float]:
        """
        Extract first OFFER price per buyer from negotiation history.
        This is used to backfill property_buyer_matches.buyer_bid so DB evidence
        reflects real negotiation offers instead of order-creation snapshot.
        """
        out: Dict[int, float] = {}
        if not isinstance(history, list):
            return out
        for event in history:
            if not isinstance(event, dict):
                continue
            if str(event.get("party", "")).lower() != "buyer":
                continue
            if str(event.get("action", "")).upper() != "OFFER":
                continue
            buyer_id_raw = event.get("agent_id", event.get("buyer_id"))
            try:
                buyer_id = int(buyer_id_raw)
            except Exception:
                continue
            if buyer_id in out:
                continue
            try:
                price = float(event.get("price", 0.0) or 0.0)
            except Exception:
                price = 0.0
            if price > 0:
                out[buyer_id] = price
        return out

    def _collect_buyer_risk_scores(self, cursor, buyer_ids: List[int]) -> Dict[int, float]:
        """
        Build historical default-risk score from mortgage ledger.
        0.0 = low risk, 1.0 = high risk.
        """
        ids = [int(x) for x in buyer_ids if int(x) > 0]
        if not ids:
            return {}
        risk_map: Dict[int, float] = {i: 0.0 for i in ids}
        try:
            placeholders = ",".join(["?"] * len(ids))
            cursor.execute(
                f"""
                SELECT
                    agent_id,
                    SUM(CASE WHEN status='defaulted' THEN 1 ELSE 0 END) AS default_count,
                    SUM(CASE WHEN UPPER(COALESCE(delinquency_stage,'')) IN ('WATCH','DPD30','DPD60') THEN 1 ELSE 0 END) AS delinquent_count,
                    MAX(COALESCE(missed_payments, 0)) AS max_missed
                FROM mortgage_accounts
                WHERE agent_id IN ({placeholders})
                GROUP BY agent_id
                """,
                tuple(ids),
            )
            for row in cursor.fetchall() or []:
                aid = int(row[0])
                default_count = int(row[1] or 0)
                delinquent_count = int(row[2] or 0)
                max_missed = int(row[3] or 0)
                risk = (
                    0.60 * self._clamp01(default_count / 2.0)
                    + 0.25 * self._clamp01(delinquent_count / 3.0)
                    + 0.15 * self._clamp01(max_missed / 4.0)
                )
                risk_map[aid] = self._clamp01(risk)
        except Exception:
            # mortgage_accounts may be unavailable in legacy runs; keep neutral risk.
            pass
        return risk_map

    def _sort_classic_buyers(self, cursor, month: int, listing: Dict, interested_entries: List[Dict]) -> List[Dict]:
        """
        Rank negotiation queue for CLASSIC mode.
        Priority signals:
        1) effective bid capacity
        2) budget fit to listing price
        3) historical default risk (penalty)
        """
        if not interested_entries:
            return interested_entries

        listed_price = max(1.0, float(listing.get("listed_price", 0.0) or 0.0))
        listing_zone = str(listing.get("zone", "") or "").strip().upper()
        down_ratio = float(self.config.mortgage.get("down_payment_ratio", 0.3))
        down_ratio = max(0.05, min(0.95, down_ratio))
        buyer_ids = [int(e["buyer"].id) for e in interested_entries if e.get("buyer") is not None]
        risk_map = self._collect_buyer_risk_scores(cursor, buyer_ids)

        # P0 guardrail knobs:
        # - suppress extreme high-headroom dominance in late queue stage
        # - improve B-zone entry-level conversion for first-home buyers
        def _to_bool(raw, default: bool) -> bool:
            if raw is None:
                return bool(default)
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

        def _to_float(raw, default: float) -> float:
            try:
                return float(raw)
            except Exception:
                return float(default)

        def _to_int(raw, default: int) -> int:
            try:
                return int(raw)
            except Exception:
                return int(default)

        headroom_penalty_enabled = _to_bool(
            self.config.get("smart_agent.classic_queue_high_headroom_penalty_enabled", True), True
        )
        headroom_penalty_threshold = max(
            0.0,
            min(
                2.0,
                _to_float(
                    self.config.get("smart_agent.classic_queue_high_headroom_threshold_ratio", 0.35),
                    0.35,
                ),
            ),
        )
        headroom_penalty_span = max(
            0.05,
            min(
                3.0,
                _to_float(
                    self.config.get("smart_agent.classic_queue_high_headroom_penalty_span", 0.70),
                    0.70,
                ),
            ),
        )
        headroom_penalty_max = max(
            0.0,
            min(
                0.50,
                _to_float(
                    self.config.get("smart_agent.classic_queue_high_headroom_penalty_max", 0.16),
                    0.16,
                ),
            ),
        )
        b_zone_entry_boost = max(
            0.0,
            min(0.50, _to_float(self.config.get("smart_agent.classic_queue_b_zone_entry_boost", 0.08), 0.08)),
        )
        b_zone_entry_max_headroom = max(
            0.0,
            min(
                1.0,
                _to_float(
                    self.config.get("smart_agent.classic_queue_b_zone_entry_max_headroom_ratio", 0.25),
                    0.25,
                ),
            ),
        )
        b_zone_entry_min_budget_fit = max(
            0.0,
            min(
                1.0,
                _to_float(
                    self.config.get("smart_agent.classic_queue_b_zone_entry_min_budget_fit", 0.55),
                    0.55,
                ),
            ),
        )
        b_zone_entry_max_owned_properties = max(
            0,
            min(
                10,
                _to_int(
                    self.config.get("smart_agent.classic_queue_b_zone_entry_max_owned_properties", 1),
                    1,
                ),
            ),
        )
        b_zone_non_school_penalty_enabled = _to_bool(
            self.config.get("smart_agent.classic_queue_b_zone_non_school_penalty_enabled", True), True
        )
        b_zone_non_school_min_headroom_ratio = max(
            0.0,
            min(
                2.0,
                _to_float(
                    self.config.get("smart_agent.classic_queue_b_zone_non_school_min_headroom_ratio", 0.35),
                    0.35,
                ),
            ),
        )
        b_zone_non_school_penalty_span = max(
            0.05,
            min(
                3.0,
                _to_float(
                    self.config.get("smart_agent.classic_queue_b_zone_non_school_penalty_span", 0.60),
                    0.60,
                ),
            ),
        )
        b_zone_non_school_penalty_max = max(
            0.0,
            min(
                0.50,
                _to_float(
                    self.config.get("smart_agent.classic_queue_b_zone_non_school_penalty_max", 0.10),
                    0.10,
                ),
            ),
        )
        b_zone_non_school_owned_threshold = max(
            0,
            min(
                10,
                _to_int(
                    self.config.get("smart_agent.classic_queue_b_zone_non_school_owned_threshold", 1),
                    1,
                ),
            ),
        )

        ranked = []
        for entry in interested_entries:
            buyer = entry["buyer"]
            pref_max_price = float(getattr(getattr(buyer, "preference", None), "max_price", 0.0) or 0.0)
            cash = float(getattr(buyer, "cash", 0.0) or 0.0)
            income = float(getattr(buyer, "monthly_income", 0.0) or 0.0)

            max_price_cover = self._clamp01(pref_max_price / listed_price) if pref_max_price > 0 else 0.0
            down_cover = self._clamp01(cash / (listed_price * down_ratio))
            monthly_need = listed_price / 260.0  # rough monthly payment scale
            income_cover = self._clamp01(income / max(1.0, monthly_need))
            capacity = 0.55 * max_price_cover + 0.35 * down_cover + 0.10 * income_cover

            if pref_max_price <= 0:
                budget_fit = 0.0
            else:
                ratio = pref_max_price / listed_price
                # Highest near 1.0, declines when too far from listing.
                budget_fit = self._clamp01(1.0 - abs(ratio - 1.0))

            risk = float(risk_map.get(int(buyer.id), 0.0))
            headroom_ratio = 0.0
            if pref_max_price > 0:
                headroom_ratio = max(0.0, (pref_max_price / listed_price) - 1.0)

            headroom_penalty = 0.0
            if headroom_penalty_enabled and headroom_ratio > headroom_penalty_threshold and headroom_penalty_max > 0:
                overflow = headroom_ratio - headroom_penalty_threshold
                headroom_penalty = headroom_penalty_max * self._clamp01(overflow / headroom_penalty_span)

            owned_count = 0
            try:
                owned_count = int(len(getattr(buyer, "owned_properties", []) or []))
            except Exception:
                owned_count = 0

            entry_boost = 0.0
            if (
                listing_zone == "B"
                and owned_count <= b_zone_entry_max_owned_properties
                and headroom_ratio <= b_zone_entry_max_headroom
                and budget_fit >= b_zone_entry_min_budget_fit
            ):
                entry_boost = b_zone_entry_boost

            purchase_motive = str(getattr(getattr(buyer, "story", None), "purchase_motive_primary", "") or "").strip().lower()
            education_path = str(getattr(getattr(buyer, "story", None), "education_path", "") or "").strip().lower()
            need_school_district = bool(getattr(getattr(buyer, "preference", None), "need_school_district", False))
            school_urgency = int(getattr(buyer, "school_urgency", 0) or 0)
            school_driven = bool(
                need_school_district
                or school_urgency > 0
                or purchase_motive == "education_driven"
                or ("school" in education_path)
            )

            investor_like = bool(
                owned_count >= b_zone_non_school_owned_threshold
                or purchase_motive in {"asset_allocation", "cashflow_defensive", "intl_education_substitute"}
            )
            b_zone_non_school_penalty = 0.0
            if (
                listing_zone == "B"
                and b_zone_non_school_penalty_enabled
                and (not school_driven)
                and investor_like
                and headroom_ratio > b_zone_non_school_min_headroom_ratio
                and b_zone_non_school_penalty_max > 0
            ):
                overflow = headroom_ratio - b_zone_non_school_min_headroom_ratio
                b_zone_non_school_penalty = b_zone_non_school_penalty_max * self._clamp01(
                    overflow / b_zone_non_school_penalty_span
                )

            priority = (
                0.60 * capacity
                + 0.30 * budget_fit
                - 0.50 * risk
                - headroom_penalty
                - b_zone_non_school_penalty
                + entry_boost
            )

            ranked.append(
                {
                    "entry": entry,
                    "buyer": buyer,
                    "priority": float(priority),
                    "capacity": float(capacity),
                    "budget_fit": float(budget_fit),
                    "risk": float(risk),
                    "headroom_ratio": float(headroom_ratio),
                    "headroom_penalty": float(headroom_penalty),
                    "b_zone_non_school_penalty": float(b_zone_non_school_penalty),
                    "entry_boost": float(entry_boost),
                    "owned_count": int(owned_count),
                    "school_driven": bool(school_driven),
                    "purchase_motive": purchase_motive,
                }
            )

        ranked.sort(
            key=lambda x: (
                x["priority"],
                x["capacity"],
                x["budget_fit"],
                -x["risk"],
                -int(x["buyer"].id),
            ),
            reverse=True,
        )

        # B-zone terminal diversification (queue-only):
        # Keep high-priority ranking as base, then inject a limited number of
        # lower-headroom school/entry candidates into the head of queue.
        # This does NOT choose final winner; it only improves final-stage diversity.
        try:
            b_zone_diversify_enabled_raw = self.config.get(
                "smart_agent.classic_queue_b_zone_terminal_diversify_enabled",
                True,
            )
            if isinstance(b_zone_diversify_enabled_raw, bool):
                b_zone_diversify_enabled = b_zone_diversify_enabled_raw
            else:
                b_zone_diversify_enabled = str(b_zone_diversify_enabled_raw).strip().lower() in {
                    "1", "true", "yes", "y", "on"
                }
        except Exception:
            b_zone_diversify_enabled = True
        try:
            b_zone_diversify_top_n = int(
                self.config.get("smart_agent.classic_queue_b_zone_terminal_diversify_top_n", 6)
            )
        except Exception:
            b_zone_diversify_top_n = 6
        try:
            b_zone_diversify_min_injected = int(
                self.config.get("smart_agent.classic_queue_b_zone_terminal_diversify_min_injected", 2)
            )
        except Exception:
            b_zone_diversify_min_injected = 2
        try:
            b_zone_diversify_headroom_cap = float(
                self.config.get("smart_agent.classic_queue_b_zone_terminal_diversify_headroom_cap", 0.28)
            )
        except Exception:
            b_zone_diversify_headroom_cap = 0.28

        b_zone_diversify_top_n = max(3, min(12, int(b_zone_diversify_top_n)))
        b_zone_diversify_min_injected = max(0, min(4, int(b_zone_diversify_min_injected)))
        b_zone_diversify_headroom_cap = max(0.05, min(1.0, float(b_zone_diversify_headroom_cap)))

        b_zone_diversify_applied = False
        b_zone_diversify_injected_ids: List[int] = []
        if (
            b_zone_diversify_enabled
            and listing_zone == "B"
            and len(ranked) >= b_zone_diversify_top_n
            and b_zone_diversify_min_injected > 0
        ):
            top_slice = list(ranked[:b_zone_diversify_top_n])
            tail_slice = list(ranked[b_zone_diversify_top_n:])
            if tail_slice:
                inject_pool = [
                    row for row in tail_slice
                    if (
                        float(row.get("headroom_ratio", 0.0) or 0.0) <= b_zone_diversify_headroom_cap
                        and (
                            bool(row.get("school_driven", False))
                            or str(row.get("purchase_motive", "") or "") in {"starter_entry", "starter_home", "education_driven", "chain_replacement"}
                        )
                    )
                ]
                inject_pool.sort(
                    key=lambda x: (
                        x["priority"],
                        x["capacity"],
                        x["budget_fit"],
                        -x["risk"],
                        -int(x["buyer"].id),
                    ),
                    reverse=True,
                )
                injected = inject_pool[:b_zone_diversify_min_injected]
                if injected:
                    injected_ids = {int(r["buyer"].id) for r in injected}
                    trimmed_top: List[Dict] = []
                    for row in top_slice:
                        if len(trimmed_top) >= max(0, b_zone_diversify_top_n - len(injected)):
                            break
                        if int(row["buyer"].id) in injected_ids:
                            continue
                        trimmed_top.append(row)
                    new_head = trimmed_top + injected
                    remaining = [row for row in ranked if int(row["buyer"].id) not in {int(x["buyer"].id) for x in new_head}]
                    ranked = new_head + remaining
                    b_zone_diversify_applied = True
                    b_zone_diversify_injected_ids = [int(r["buyer"].id) for r in injected]

        self._append_order_log(
            month,
            "CLASSIC_QUEUE_RANK",
            {
                "property_id": int(listing.get("property_id", -1)),
                "listed_price": round(listed_price, 2),
                "buyer_count": len(ranked),
                "queue": [
                    {
                        "buyer_id": int(r["buyer"].id),
                        "priority": round(float(r["priority"]), 4),
                        "capacity": round(float(r["capacity"]), 4),
                        "budget_fit": round(float(r["budget_fit"]), 4),
                        "risk": round(float(r["risk"]), 4),
                        "headroom_ratio": round(float(r["headroom_ratio"]), 4),
                        "headroom_penalty": round(float(r["headroom_penalty"]), 4),
                        "b_zone_non_school_penalty": round(float(r["b_zone_non_school_penalty"]), 4),
                        "entry_boost": round(float(r["entry_boost"]), 4),
                        "owned_count": int(r["owned_count"]),
                        "school_driven": bool(r["school_driven"]),
                        "purchase_motive": str(r["purchase_motive"] or ""),
                    }
                    for r in ranked[:10]
                ],
                "guardrail_cfg": {
                    "headroom_penalty_enabled": bool(headroom_penalty_enabled),
                    "headroom_penalty_threshold": round(float(headroom_penalty_threshold), 4),
                    "headroom_penalty_span": round(float(headroom_penalty_span), 4),
                    "headroom_penalty_max": round(float(headroom_penalty_max), 4),
                    "b_zone_entry_boost": round(float(b_zone_entry_boost), 4),
                    "b_zone_entry_max_headroom": round(float(b_zone_entry_max_headroom), 4),
                    "b_zone_entry_min_budget_fit": round(float(b_zone_entry_min_budget_fit), 4),
                    "b_zone_entry_max_owned_properties": int(b_zone_entry_max_owned_properties),
                    "b_zone_non_school_penalty_enabled": bool(b_zone_non_school_penalty_enabled),
                    "b_zone_non_school_min_headroom_ratio": round(float(b_zone_non_school_min_headroom_ratio), 4),
                    "b_zone_non_school_penalty_span": round(float(b_zone_non_school_penalty_span), 4),
                    "b_zone_non_school_penalty_max": round(float(b_zone_non_school_penalty_max), 4),
                    "b_zone_non_school_owned_threshold": int(b_zone_non_school_owned_threshold),
                    "b_zone_terminal_diversify_enabled": bool(b_zone_diversify_enabled),
                    "b_zone_terminal_diversify_top_n": int(b_zone_diversify_top_n),
                    "b_zone_terminal_diversify_min_injected": int(b_zone_diversify_min_injected),
                    "b_zone_terminal_diversify_headroom_cap": round(float(b_zone_diversify_headroom_cap), 4),
                    "b_zone_terminal_diversify_applied": bool(b_zone_diversify_applied),
                    "b_zone_terminal_diversify_injected_ids": b_zone_diversify_injected_ids,
                },
            },
        )

        return [r["entry"] for r in ranked]

    def _resolve_observed_market_trend(self, cursor, month: int, buyer: Agent, default_trend: str) -> Dict[str, object]:
        """
        M14 enhancement:
        Resolve buyer-visible market trend with per-agent info delay.
        This affects decision context (LLM path) but never bypasses hard constraints.
        """
        try:
            delay = int(getattr(buyer, "info_delay_months", 0) or 0)
        except Exception:
            delay = 0
        delay = max(0, delay)
        if delay <= 0:
            return {
                "observed_trend": str(default_trend or "STABLE"),
                "delay_months": 0,
                "observed_month": int(month),
            }

        observed_month = max(1, int(month) - delay)
        observed_trend = str(default_trend or "STABLE")
        try:
            cursor.execute("SELECT trend_signal FROM market_bulletin WHERE month = ?", (observed_month,))
            row = cursor.fetchone()
            if row and row[0]:
                observed_trend = str(row[0])
        except Exception:
            observed_trend = str(default_trend or "STABLE")

        return {
            "observed_trend": observed_trend,
            "delay_months": delay,
            "observed_month": observed_month,
        }

    def _resolve_delayed_market_trend(self, cursor, month: int, info_delay_months: int, default_trend: str) -> Dict[str, object]:
        """
        Resolve visible market trend for non-buyer decisions such as seller repricing.
        This changes decision context only and never bypasses transaction constraints.
        """
        try:
            delay = int(info_delay_months or 0)
        except Exception:
            delay = 0
        delay = max(0, delay)
        if delay <= 0:
            return {
                "observed_trend": str(default_trend or "STABLE"),
                "delay_months": 0,
                "observed_month": int(month),
            }

        observed_month = max(1, int(month) - delay)
        observed_trend = str(default_trend or "STABLE")
        try:
            cursor.execute("SELECT trend_signal FROM market_bulletin WHERE month = ?", (observed_month,))
            row = cursor.fetchone()
            if row and row[0]:
                observed_trend = str(row[0])
        except Exception:
            observed_trend = str(default_trend or "STABLE")

        return {
            "observed_trend": observed_trend,
            "delay_months": delay,
            "observed_month": observed_month,
        }

    def _build_developer_priority_queue(self, month: int, listing: Dict, interested_entries: List[Dict]) -> List[Dict]:
        """
        Build rush-sale queue:
        1) higher offered price first
        2) tie-breaker by configured mode (random/rotation)
        """
        listed_price = float(listing.get("listed_price", 0.0))
        ranked = []
        for entry in interested_entries:
            buyer = entry["buyer"]
            # Uplift max 5% above list; reflects willingness in rush buying.
            uplift = 0.05 * self._resolve_bid_aggressiveness(buyer)
            offered_price = round(listed_price * (1.0 + uplift), 2)
            ranked.append({
                "entry": entry,
                "buyer": buyer,
                "offered_price": offered_price,
                "aggressiveness": round(self._resolve_bid_aggressiveness(buyer), 3),
            })

        ranked.sort(key=lambda x: x["offered_price"], reverse=True)
        tie_breaker = self._resolve_tie_breaker_mode()
        result = []
        i = 0
        while i < len(ranked):
            j = i + 1
            while j < len(ranked) and ranked[j]["offered_price"] == ranked[i]["offered_price"]:
                j += 1
            group = ranked[i:j]
            if len(group) > 1:
                if tie_breaker == "random":
                    # Deterministic random for reproducible regressions.
                    seed = hash((month, int(listing.get("property_id", 0)), len(group)))
                    rng = random.Random(seed)
                    rng.shuffle(group)
                else:  # rotation
                    group.sort(key=lambda x: x["buyer"].id)
                    shift = month % len(group)
                    group = group[shift:] + group[:shift]
            result.extend(group)
            i = j
        return result

    def _sync_buyer_finance(self, cursor, buyer: Agent):
        f = buyer.to_v2_finance_dict()
        cursor.execute(
            """
            UPDATE agents_finance
            SET mortgage_monthly_payment=?, cash=?, total_assets=?, total_debt=?, net_cashflow=?
            WHERE agent_id=?
            """,
            (f['mortgage_monthly_payment'], f['cash'], f['total_assets'], f['total_debt'], f['net_cashflow'], buyer.id)
        )

    def _repair_listing_price_invariants(self, cursor, month: int) -> int:
        """
        Heal stale/legacy listing rows where min_price > listed_price (or non-positive values).
        Keeps trading pipeline stable for resume runs on old databases.
        """
        cursor.execute(
            """
            SELECT property_id, listed_price, min_price
            FROM properties_market
            WHERE status='for_sale'
              AND (
                  listed_price IS NULL OR listed_price <= 0
                  OR min_price IS NULL OR min_price <= 0
                  OR min_price > listed_price
              )
            """
        )
        rows = cursor.fetchall() or []
        if not rows:
            return 0

        updates = []
        for pid, listed, minimum in rows:
            listed_price = max(1.0, float(listed or 0.0))
            min_price = max(1.0, float(minimum or 0.0))
            if min_price > listed_price:
                min_price = listed_price
            updates.append((listed_price, min_price, int(month), int(pid)))

        cursor.executemany(
            """
            UPDATE properties_market
            SET listed_price=?, min_price=?, last_price_update_month=?, last_price_update_reason='Invariant repair: min<=listed'
            WHERE property_id=?
            """,
            updates,
        )
        self._append_order_log(
            month,
            "LISTING_INVARIANT_REPAIR",
            {
                "repaired_count": len(updates),
                "property_ids": [u[3] for u in updates[:20]],
            },
        )
        return len(updates)

    def _resolve_effective_bid_floor_ratio(self) -> float:
        try:
            raw = float(
                self.config.get(
                    "smart_agent.effective_bid_floor_ratio",
                    self.config.get("effective_bid_floor_ratio", 0.98),
                )
            )
        except Exception:
            raw = 0.98
        return max(0.5, min(1.2, raw))

    def _resolve_precheck_liquidity_buffer_months(self) -> int:
        try:
            raw = int(
                self.config.get(
                    "smart_agent.precheck_liquidity_buffer_months",
                    self.config.get("precheck_liquidity_buffer_months", 3),
                )
            )
        except Exception:
            raw = 3
        return max(0, raw)

    def _resolve_settlement_liquidity_floor_months(self) -> int:
        try:
            raw = int(
                self.config.get(
                    "smart_agent.settlement_liquidity_floor_months",
                    self.config.get(
                        "smart_agent.liquidity_floor_months",
                        self.config.get(
                            "settlement_liquidity_floor_months",
                            self.config.get("liquidity_floor_months", 0),
                        ),
                    ),
                )
            )
        except Exception:
            raw = 0
        return max(0, raw)

    def _resolve_precheck_include_tax_and_fee(self) -> bool:
        raw = self.config.get(
            "smart_agent.precheck_include_tax_and_fee",
            self.config.get("precheck_include_tax_and_fee", True),
        )
        if isinstance(raw, bool):
            return raw
        if raw is None:
            return True
        return str(raw).strip().lower() not in {"0", "false", "no", "n", "off"}

    def _resolve_settlement_affordability_recheck_enabled(self) -> bool:
        """
        Default OFF: once an order enters pending_settlement, affordability is
        treated as already finalized by the presettlement hard gate.
        """
        raw = self.config.get(
            "smart_agent.settlement_affordability_recheck_enabled",
            self.config.get("settlement_affordability_recheck_enabled", False),
        )
        if isinstance(raw, bool):
            return raw
        if raw is None:
            return False
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _resolve_candidate_shortlist_controls(self, cursor, month: int) -> Dict[str, int]:
        try:
            base_top_k = int(
                self.config.get(
                    "smart_agent.candidate_top_k",
                    self.config.get("candidate_top_k", 5),
                )
            )
        except Exception:
            base_top_k = 5
        try:
            max_fallback_k = int(
                self.config.get(
                    "smart_agent.candidate_max_k_on_fallback",
                    self.config.get("candidate_max_k_on_fallback", 8),
                )
            )
        except Exception:
            max_fallback_k = 8
        try:
            exploration_slots = int(
                self.config.get(
                    "smart_agent.candidate_exploration_slots",
                    self.config.get("candidate_exploration_slots", 1),
                )
            )
        except Exception:
            exploration_slots = 1
        raw_auto = self.config.get(
            "simulation.low_tx_auto_relax_enabled",
            self.config.get("low_tx_auto_relax_enabled", True),
        )
        auto_relax = raw_auto if isinstance(raw_auto, bool) else str(raw_auto).strip().lower() not in {
            "0", "false", "no", "off", "n"
        }
        try:
            min_gate = int(self.config.get("simulation.min_transactions_gate", 3))
        except Exception:
            min_gate = 3

        top_k = max(1, min(12, base_top_k))
        if auto_relax and int(month) > 1:
            cursor.execute(
                "SELECT COALESCE(transaction_volume, 0) FROM market_bulletin WHERE month = ?",
                (int(month) - 1,),
            )
            prev_tx = int((cursor.fetchone() or [0])[0] or 0)
            if prev_tx < max(0, min_gate):
                top_k = max(top_k, max(1, min(12, max_fallback_k)))
                self._append_order_log(
                    int(month),
                    "LOW_TX_AUTO_RELAX",
                    {
                        "previous_month_transactions": prev_tx,
                        "min_transactions_gate": int(min_gate),
                        "candidate_top_k_before": int(base_top_k),
                        "candidate_top_k_after": int(top_k),
                    },
                )
        return {
            "top_k": int(top_k),
            "exploration_slots": max(0, min(3, int(exploration_slots))),
        }

    def _resolve_candidate_quota_controls(self) -> Dict[str, object]:
        try:
            enabled_raw = self.config.get(
                "smart_agent.candidate_quota_prefilter_enabled",
                self.config.get("candidate_quota_prefilter_enabled", True),
            )
            if isinstance(enabled_raw, bool):
                enabled = enabled_raw
            else:
                enabled = str(enabled_raw).strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            enabled = True
        try:
            base_quota = int(
                self.config.get(
                    "smart_agent.candidate_quota_per_property_base",
                    self.config.get("candidate_quota_per_property_base", 4),
                )
            )
        except Exception:
            base_quota = 4
        try:
            school_bonus = int(
                self.config.get(
                    "smart_agent.candidate_quota_per_property_school_bonus",
                    self.config.get("candidate_quota_per_property_school_bonus", 1),
                )
            )
        except Exception:
            school_bonus = 1
        try:
            b_zone_bonus = int(
                self.config.get(
                    "smart_agent.candidate_quota_per_property_b_zone_bonus",
                    self.config.get("candidate_quota_per_property_b_zone_bonus", 1),
                )
            )
        except Exception:
            b_zone_bonus = 1
        try:
            locked_extra = int(
                self.config.get(
                    "smart_agent.candidate_quota_buy_task_locked_extra",
                    self.config.get("candidate_quota_buy_task_locked_extra", 1),
                )
            )
        except Exception:
            locked_extra = 1
        try:
            dynamic_enabled_raw = self.config.get(
                "smart_agent.candidate_quota_dynamic_enabled",
                self.config.get("candidate_quota_dynamic_enabled", True),
            )
            if isinstance(dynamic_enabled_raw, bool):
                dynamic_enabled = dynamic_enabled_raw
            else:
                dynamic_enabled = str(dynamic_enabled_raw).strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            dynamic_enabled = True
        try:
            dynamic_pressure_step = float(
                self.config.get(
                    "smart_agent.candidate_quota_dynamic_pressure_step",
                    self.config.get("candidate_quota_dynamic_pressure_step", 1.0),
                )
            )
        except Exception:
            dynamic_pressure_step = 1.0
        try:
            dynamic_max_bonus = int(
                self.config.get(
                    "smart_agent.candidate_quota_dynamic_max_bonus",
                    self.config.get("candidate_quota_dynamic_max_bonus", 6),
                )
            )
        except Exception:
            dynamic_max_bonus = 6
        try:
            stage2_bonus = int(
                self.config.get(
                    "smart_agent.candidate_quota_stage2_bonus",
                    self.config.get("candidate_quota_stage2_bonus", 2),
                )
            )
        except Exception:
            stage2_bonus = 2
        try:
            blocked_recovery_bonus = int(
                self.config.get(
                    "smart_agent.candidate_quota_blocked_recovery_bonus",
                    self.config.get("candidate_quota_blocked_recovery_bonus", 1),
                )
            )
        except Exception:
            blocked_recovery_bonus = 1
        return {
            "enabled": bool(enabled),
            "base_quota": max(1, min(12, int(base_quota))),
            "school_bonus": max(0, min(4, int(school_bonus))),
            "b_zone_bonus": max(0, min(4, int(b_zone_bonus))),
            "buy_task_locked_extra": max(0, min(4, int(locked_extra))),
            "dynamic_enabled": bool(dynamic_enabled),
            "dynamic_pressure_step": max(0.25, min(4.0, float(dynamic_pressure_step))),
            "dynamic_max_bonus": max(0, min(12, int(dynamic_max_bonus))),
            "stage2_bonus": max(0, min(6, int(stage2_bonus))),
            "blocked_recovery_bonus": max(0, min(6, int(blocked_recovery_bonus))),
        }

    def _resolve_property_candidate_quota(
        self,
        listing: Dict,
        prop: Dict,
        buyer: Optional[Agent],
        quota_cfg: Dict[str, object],
        pressure_score: float = 0.0,
        stage: str = "stage1",
        blocked_recovery: bool = False,
    ) -> int:
        quota = int(quota_cfg.get("base_quota", 4) or 4)
        zone = str((prop or {}).get("zone", "") or "").upper()
        if zone == "B":
            quota += int(quota_cfg.get("b_zone_bonus", 0) or 0)
        if bool((prop or {}).get("is_school_district", False)):
            quota += int(quota_cfg.get("school_bonus", 0) or 0)
        if buyer is not None and bool(getattr(buyer, "_buy_task_locked", False)):
            quota += int(quota_cfg.get("buy_task_locked_extra", 0) or 0)
        stage_l = str(stage or "stage1").strip().lower()
        if stage_l == "stage2":
            quota += int(quota_cfg.get("stage2_bonus", 0) or 0)
        if bool(blocked_recovery):
            quota += int(quota_cfg.get("blocked_recovery_bonus", 0) or 0)
        if bool(quota_cfg.get("dynamic_enabled", True)):
            try:
                pressure = float(pressure_score or 0.0)
            except Exception:
                pressure = 0.0
            step = float(quota_cfg.get("dynamic_pressure_step", 1.0) or 1.0)
            max_bonus = int(quota_cfg.get("dynamic_max_bonus", 0) or 0)
            if step > 0.0 and max_bonus > 0 and pressure > 0.0:
                dyn_bonus = int(math.floor(float(pressure) / float(step)))
                quota += max(0, min(int(max_bonus), int(dyn_bonus)))
        return max(1, int(quota))

    def _resolve_two_stage_candidate_controls(self) -> Dict[str, int | bool]:
        try:
            enabled_raw = self.config.get(
                "smart_agent.candidate_two_stage_enabled",
                self.config.get("candidate_two_stage_enabled", True),
            )
            if isinstance(enabled_raw, bool):
                enabled = enabled_raw
            else:
                enabled = str(enabled_raw).strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            enabled = True
        try:
            min_stage1_pool = int(
                self.config.get(
                    "smart_agent.candidate_two_stage_min_stage1_pool",
                    self.config.get("candidate_two_stage_min_stage1_pool", 4),
                )
            )
        except Exception:
            min_stage1_pool = 4
        try:
            max_stage2_fill = int(
                self.config.get(
                    "smart_agent.candidate_two_stage_max_stage2_fill",
                    self.config.get("candidate_two_stage_max_stage2_fill", 6),
                )
            )
        except Exception:
            max_stage2_fill = 6
        return {
            "enabled": bool(enabled),
            "min_stage1_pool": max(1, min(12, int(min_stage1_pool))),
            "max_stage2_fill": max(1, min(16, int(max_stage2_fill))),
        }

    def _reprioritize_quota_replacement_candidates(
        self,
        buyer: Agent,
        listings: List[Dict],
        props_map: Dict[int, Dict],
        blocked_property_ids: List[int],
        combined_pressure: Dict[int, float],
        monthly_candidate_quota_used: Dict[int, int],
    ) -> List[Dict]:
        if not listings or not blocked_property_ids:
            return listings
        pref = getattr(buyer, "preference", None)
        target_zone = str(getattr(pref, "target_zone", "") or "").upper()
        if target_zone != "B":
            return listings
        need_school = bool(getattr(pref, "need_school_district", False))
        max_price = float(getattr(pref, "max_price", 0.0) or 0.0)

        blocked_zones = set()
        blocked_school = False
        for pid in blocked_property_ids:
            prop = props_map.get(int(pid), {}) or {}
            blocked_zones.add(str(prop.get("zone", "") or "").upper())
            blocked_school = blocked_school or bool(prop.get("is_school_district", False))
        if "B" not in blocked_zones:
            return listings

        def _score(li: Dict) -> Tuple[float, float, float, float, float]:
            try:
                pid = int(li.get("property_id"))
            except Exception:
                pid = -1
            prop = props_map.get(pid, {}) or {}
            zone = str(prop.get("zone", li.get("zone", "")) or "").upper()
            is_school = bool(prop.get("is_school_district", li.get("is_school_district", False)))
            try:
                listed_price = float(li.get("listed_price", li.get("price", 0.0)) or 0.0)
            except Exception:
                listed_price = 0.0
            pressure = float(combined_pressure.get(pid, 0.0) or 0.0)
            used_count = float(monthly_candidate_quota_used.get(pid, 0) or 0)
            same_zone = 1 if zone == "B" else 0
            school_match = 1 if (is_school == need_school) else 0
            if need_school and blocked_school and is_school:
                school_match += 1
            affordable = 1 if (max_price <= 0.0 or listed_price <= max_price * 1.02) else 0
            return (
                float(same_zone),
                float(school_match),
                float(affordable),
                -float(used_count),
                -float(pressure),
            )

        prioritized = sorted(listings, key=_score, reverse=True)
        b_candidates = []
        other_candidates = []
        for li in prioritized:
            try:
                pid = int(li.get("property_id"))
            except Exception:
                pid = -1
            prop = props_map.get(pid, {}) or {}
            zone = str(prop.get("zone", li.get("zone", "")) or "").upper()
            if zone == "B":
                b_candidates.append(li)
            else:
                other_candidates.append(li)
        if len(b_candidates) > 1:
            rotation = int(getattr(buyer, "id", 0) or 0) % len(b_candidates)
            b_candidates = b_candidates[rotation:] + b_candidates[:rotation]
        prioritized = b_candidates + other_candidates
        top_ids = []
        for li in prioritized[:6]:
            try:
                top_ids.append(int(li.get("property_id")))
            except Exception:
                continue
        if top_ids:
            self._append_order_log(
                int(getattr(buyer, "_current_matching_month", 0) or 0),
                "CANDIDATE_QUOTA_REFILL_PRIORITIZED",
                {
                    "buyer_id": int(getattr(buyer, "id", -1)),
                    "target_zone": target_zone,
                    "need_school_district": bool(need_school),
                    "blocked_property_ids": [int(x) for x in blocked_property_ids if x is not None],
                    "replacement_priority_ids": top_ids,
                },
            )
        return prioritized

    def _build_recovery_refill_candidates(
        self,
        cursor,
        month: int,
        buyer: Agent,
        active_listings: List[Dict],
        props_map: Dict,
        blocked_property_ids: set[int],
        attempted_property_ids: set[int],
        combined_pressure: Dict[int, float],
        monthly_candidate_quota_used: Dict[int, int],
        candidate_quota_cfg: Dict[str, object],
        limit: int = 6,
        candidate_id_whitelist: Optional[set[int]] = None,
        emit_log: bool = True,
        recovery_reason: str = "",
    ) -> List[Dict]:
        """
        Rebuild or reprioritize viable recovery candidates from the live market.

        This stays on the rule layer: it only filters and prioritizes candidates that are
        likely to pass hard constraints. The buyer still chooses via the normal matcher/LLM.
        """
        if not active_listings or buyer is None:
            return []
        pref = getattr(buyer, "preference", None)
        if pref is None:
            return []
        hard_whitelist, hard_meta = self._resolve_hard_bucket_property_whitelist(
            cursor=cursor,
            buyer_id=int(getattr(buyer, "id", -1) or -1),
        )
        hard_ctx = self._resolve_hard_bucket_context(cursor)
        strict_unmapped_property = bool((hard_ctx or {}).get("strict_unmapped_property", True))
        property_bucket_map = ((hard_ctx or {}).get("property_bucket_map", {}) or {})
        if hard_whitelist is not None and len(hard_whitelist) <= 0:
            if emit_log:
                self._append_order_log(
                    int(month),
                    "RECOVERY_REFILL_BLOCKED_BY_HARD_BUCKET",
                    {
                        "buyer_id": int(getattr(buyer, "id", -1)),
                        "hard_meta": dict(hard_meta or {}),
                    },
                )
            return []

        target_zone = str(getattr(pref, "target_zone", "") or "").upper()
        need_school = bool(getattr(pref, "need_school_district", False))
        min_bedrooms = max(1, int(getattr(pref, "min_bedrooms", 1) or 1))
        max_price = float(getattr(pref, "max_price", 0.0) or 0.0)
        story = getattr(buyer, "story", None)
        education_path = str(getattr(story, "education_path", "") or "").lower()
        purchase_motive = str(getattr(story, "purchase_motive_primary", "") or "").lower()
        school_urgency = int(getattr(buyer, "school_urgency", 0) or 0)
        strict_school_requirement = bool(
            need_school
            and (
                school_urgency >= 2
                or "public_school" in education_path
                or purchase_motive == "education_driven"
            )
        )
        waited_months = max(0, int(getattr(buyer, "waited_months", 0) or 0))
        max_wait_months = max(
            1,
            int(getattr(buyer, "max_wait_months", getattr(pref, "max_wait_months", 6)) or 6),
        )
        deadline_progress = float(waited_months) / float(max_wait_months) if max_wait_months > 0 else 0.0
        if deadline_progress >= 1.0:
            buy_deadline_stage = "overdue"
        elif deadline_progress >= 0.75:
            buy_deadline_stage = "late"
        elif deadline_progress >= 0.40:
            buy_deadline_stage = "mid"
        else:
            buy_deadline_stage = "early"
        recovery_reason_l = str(recovery_reason or "").strip().lower()
        near_equal_tiebreak = "near-equal final bids" in recovery_reason_l
        force_zone_flexible = buy_deadline_stage in {"late", "overdue"} or near_equal_tiebreak
        force_school_relax = buy_deadline_stage in {"late", "overdue"} or near_equal_tiebreak
        if near_equal_tiebreak:
            strict_school_requirement = False
            min_bedrooms = max(1, min_bedrooms - 1)
        order_cfg = self._order_config()
        deposit_ratio = float(order_cfg.get("deposit_ratio", 0.1) or 0.1)

        pending_counts: Dict[int, int] = {}
        try:
            cursor.execute(
                """
                SELECT property_id, COUNT(*)
                FROM transaction_orders
                WHERE status IN ('pending', 'pending_settlement')
                GROUP BY property_id
                """
            )
            for row in cursor.fetchall() or []:
                if row and row[0] is not None:
                    pending_counts[int(row[0])] = int(row[1] or 0)
        except Exception:
            pending_counts = {}

        refill_rows: List[Tuple[Tuple[float, ...], Dict]] = []
        for li in active_listings:
            try:
                pid = int(li.get("property_id", -1))
            except Exception:
                pid = -1
            if hard_whitelist is not None:
                if strict_unmapped_property and pid not in property_bucket_map:
                    continue
                if pid not in hard_whitelist:
                    continue
            if candidate_id_whitelist is not None and pid not in candidate_id_whitelist:
                continue
            if pid < 0 or pid in blocked_property_ids or pid in attempted_property_ids:
                continue
            prop = props_map.get(pid) or {}
            owner_val = li.get("owner_id", li.get("seller_id", prop.get("owner_id", -1)))
            try:
                owner_id = int(owner_val) if owner_val is not None else -1
            except Exception:
                owner_id = -1
            if owner_id == int(getattr(buyer, "id", -1)):
                continue
            zone = str(prop.get("zone", li.get("zone", "")) or "").upper()
            is_school = bool(prop.get("is_school_district", li.get("is_school_district", False)))
            try:
                listed_price = float(li.get("listed_price", li.get("price", 0.0)) or 0.0)
            except Exception:
                listed_price = 0.0
            if listed_price <= 0.0:
                continue

            is_developer = int(owner_id) == -1
            if (not force_zone_flexible) and target_zone in {"A", "B"} and zone != target_zone and (not is_developer):
                continue
            price_limit_multiplier = 1.35 if near_equal_tiebreak else 1.2
            if max_price > 0.0 and listed_price > max_price * price_limit_multiplier:
                continue
            try:
                bedrooms = int(prop.get("bedrooms", 999) or 999)
            except Exception:
                bedrooms = 999
            if (not is_developer) and bedrooms < min_bedrooms:
                continue
            if need_school and (not force_school_relax) and (not is_school):
                if strict_school_requirement:
                    continue

            quota_limit = self._resolve_property_candidate_quota(
                li,
                prop or {},
                buyer,
                candidate_quota_cfg,
                pressure_score=float(combined_pressure.get(int(pid), 0.0) or 0.0),
                stage="stage2",
                blocked_recovery=True,
            )
            used_count = int(monthly_candidate_quota_used.get(pid, 0) or 0)
            if used_count >= quota_limit:
                continue

            precheck_ok, _, _ = self._precheck_order_affordability(
                buyer=buyer,
                listing=li,
                offer_price=listed_price,
            )
            deposit_need = max(0.0, listed_price * deposit_ratio)
            deposit_ok = float(getattr(buyer, "cash", 0.0) or 0.0) >= deposit_need
            if not precheck_ok or not deposit_ok:
                continue

            pending_count = int(pending_counts.get(pid, 0) or 0)
            pressure = float(combined_pressure.get(pid, 0.0) or 0.0)
            price_ratio = listed_price / max(1.0, max_price) if max_price > 0 else 0.0
            affordability_headroom = max(0.0, 1.0 - price_ratio) if max_price > 0 else 0.0
            same_zone = 1.0 if (target_zone in {"A", "B"} and zone == target_zone) else 0.0
            school_match = 1.0 if (need_school and is_school) else (0.5 if not need_school else 0.0)
            price_fit = 1.0 - min(1.0, price_ratio) if max_price > 0 else 0.0

            refill_rows.append(
                (
                    (
                        float(same_zone),
                        float(school_match),
                        float(affordability_headroom),
                        float(price_fit),
                        -float(pending_count),
                        -float(used_count),
                        -float(pressure),
                    ),
                    {
                        "property_id": int(pid),
                        "listed_price": float(listed_price),
                        "owner_id": int(owner_id),
                        "pending_count": int(pending_count),
                        "used_count": int(used_count),
                        "quota_limit": int(quota_limit),
                        "zone": str(zone),
                        "is_school_district": bool(is_school),
                        "same_zone_score": float(same_zone),
                        "school_match_score": float(school_match),
                        "affordability_headroom": float(affordability_headroom),
                        "price_fit_score": float(price_fit),
                        "pressure": float(pressure),
                    },
                )
            )

        refill_rows.sort(key=lambda item: item[0], reverse=True)
        ranked_candidates = [dict(item[1]) for item in refill_rows]
        prefers_low_competition = recovery_reason_l.startswith("outbid:")
        prefers_budget_headroom = "stronger budget headroom" in recovery_reason_l

        filtered_candidates = list(ranked_candidates)
        if prefers_low_competition:
            uncontested_candidates = [
                item
                for item in filtered_candidates
                if int(item.get("pending_count", 0) or 0) <= 0
            ]
            limited_competition_candidates = [
                item
                for item in filtered_candidates
                if int(item.get("pending_count", 0) or 0) <= 1
            ]
            if len(uncontested_candidates) >= max(1, int(limit)):
                filtered_candidates = uncontested_candidates
            elif uncontested_candidates:
                seen_ids = {int(item.get("property_id", -1) or -1) for item in uncontested_candidates}
                fallback_candidates = [
                    item
                    for item in limited_competition_candidates
                    if int(item.get("property_id", -1) or -1) not in seen_ids
                ]
                filtered_candidates = uncontested_candidates + fallback_candidates
            elif limited_competition_candidates:
                filtered_candidates = limited_competition_candidates
        if prefers_budget_headroom:
            headroom_floor = 0.08 if max_price > 0 else 0.0
            headroom_candidates = [
                item
                for item in filtered_candidates
                if float(item.get("affordability_headroom", 0.0) or 0.0) >= float(headroom_floor)
            ]
            if len(headroom_candidates) >= max(1, int(limit)):
                filtered_candidates = headroom_candidates
            elif headroom_candidates:
                seen_ids = {int(item.get("property_id", -1) or -1) for item in headroom_candidates}
                fallback_candidates = [
                    item
                    for item in filtered_candidates
                    if int(item.get("property_id", -1) or -1) not in seen_ids
                ]
                filtered_candidates = headroom_candidates + fallback_candidates

        refill_candidates = [dict(item) for item in filtered_candidates[: max(1, int(limit))]]
        if refill_candidates and emit_log:
            self._append_order_log(
                int(month),
                "RECOVERY_REFILL_PRIORITIZED",
                {
                    "buyer_id": int(getattr(buyer, "id", -1)),
                    "target_zone": str(target_zone),
                    "need_school_district": bool(need_school),
                    "buy_deadline_stage": str(buy_deadline_stage),
                    "recovery_reason": str(recovery_reason or ""),
                    "near_equal_tiebreak": bool(near_equal_tiebreak),
                    "prefers_low_competition": bool(prefers_low_competition),
                    "prefers_budget_headroom": bool(prefers_budget_headroom),
                    "blocked_property_ids": [int(x) for x in sorted(blocked_property_ids)],
                    "attempted_property_ids": [int(x) for x in sorted(attempted_property_ids)],
                    "candidate_ids": [int(x["property_id"]) for x in refill_candidates],
                },
            )
        return refill_candidates

    def _recovery_candidate_sort_key(
        self,
        candidate: Dict,
        promotion_usage: Optional[Dict[int, int]] = None,
    ) -> Tuple[float, ...]:
        pid = int(candidate.get("property_id", -1) or -1)
        usage_penalty = float((promotion_usage or {}).get(pid, 0) or 0)
        return (
            float(candidate.get("same_zone_score", 0.0) or 0.0),
            float(candidate.get("school_match_score", 0.0) or 0.0),
            float(candidate.get("affordability_headroom", 0.0) or 0.0),
            float(candidate.get("price_fit_score", 0.0) or 0.0),
            -float(candidate.get("pending_count", 0) or 0),
            -float(candidate.get("used_count", 0) or 0),
            -float(usage_penalty),
            -float(candidate.get("pressure", 0.0) or 0.0),
        )

    def _build_candidate_history_pressure_map(
        self,
        cursor,
        month: int,
        active_listings: List[Dict],
    ) -> Dict[int, float]:
        """
        Build per-property pressure for shortlist diversification.
        +positive: hot listings (many recent matches) => penalty
        -negative: cold listings (recently no matches) => bonus
        """
        if not active_listings:
            return {}
        try:
            lookback = int(
                self.config.get(
                    "smart_agent.candidate_hotspot_lookback_months",
                    self.config.get("candidate_hotspot_lookback_months", 1),
                )
            )
        except Exception:
            lookback = 1
        try:
            hot_threshold = int(
                self.config.get(
                    "smart_agent.candidate_hotspot_hot_threshold",
                    self.config.get("candidate_hotspot_hot_threshold", 3),
                )
            )
        except Exception:
            hot_threshold = 3
        try:
            hot_max_units = float(
                self.config.get(
                    "smart_agent.candidate_hotspot_hot_max_units",
                    self.config.get("candidate_hotspot_hot_max_units", 4.0),
                )
            )
        except Exception:
            hot_max_units = 4.0
        try:
            cold_bonus_units = float(
                self.config.get(
                    "smart_agent.candidate_cold_bonus_units",
                    self.config.get("candidate_cold_bonus_units", 2.0),
                )
            )
        except Exception:
            cold_bonus_units = 2.0
        try:
            cold_age_gate = int(
                self.config.get(
                    "smart_agent.candidate_cold_listing_age_gate_months",
                    self.config.get("candidate_cold_listing_age_gate_months", 1),
                )
            )
        except Exception:
            cold_age_gate = 1

        lookback = max(0, min(4, int(lookback)))
        hot_threshold = max(1, min(12, int(hot_threshold)))
        hot_max_units = max(0.0, min(12.0, float(hot_max_units)))
        cold_bonus_units = max(0.0, min(8.0, float(cold_bonus_units)))
        cold_age_gate = max(0, min(12, int(cold_age_gate)))

        listing_ids = []
        listing_age_map: Dict[int, int] = {}
        for item in active_listings:
            try:
                pid = int(item.get("property_id"))
            except Exception:
                continue
            listing_ids.append(pid)
            try:
                listing_age_map[pid] = int(item.get("listing_age_months", 0) or 0)
            except Exception:
                listing_age_map[pid] = 0
        if not listing_ids:
            return {}

        counts: Dict[int, int] = {pid: 0 for pid in listing_ids}
        if lookback > 0 and int(month) > 1:
            start_month = max(1, int(month) - lookback)
            end_month = max(1, int(month) - 1)
            placeholders = ",".join("?" for _ in listing_ids)
            sql = f"""
                SELECT property_id, COUNT(*)
                FROM property_buyer_matches
                WHERE month BETWEEN ? AND ?
                  AND property_id IN ({placeholders})
                GROUP BY property_id
            """
            try:
                cursor.execute(sql, tuple([start_month, end_month] + listing_ids))
                for pid, c in cursor.fetchall() or []:
                    if pid is None:
                        continue
                    counts[int(pid)] = int(c or 0)
            except Exception:
                pass

        pressure_map: Dict[int, float] = {}
        for pid in listing_ids:
            cnt = int(counts.get(pid, 0))
            age = int(listing_age_map.get(pid, 0))
            hot_units = 0.0
            if cnt >= hot_threshold:
                hot_units = min(hot_max_units, float(cnt - hot_threshold + 1))
            cold_units = 0.0
            if cnt == 0 and age >= cold_age_gate:
                cold_units = float(cold_bonus_units)
            pressure = float(hot_units - cold_units)
            if abs(pressure) > 1e-9:
                pressure_map[int(pid)] = pressure
        return pressure_map

    def _classify_affordability_failure(
        self,
        buyer: Agent,
        price: float,
        buyer_total_cost: float = 0.0,
        liquidity_buffer_months: int = 0,
        available_cash_override: float = None,
    ) -> str:
        """
        Return structured affordability failure reason:
        INSUFFICIENT_DOWN_PAYMENT | FEE_SHORTFALL | DTI_EXCEEDED |
        INSUFFICIENT_LIQUIDITY_BUFFER | LEVERAGE_CAP_EXCEEDED
        """
        mortgage_cfg = self.config.mortgage if self.config else {}
        down_ratio = float(mortgage_cfg.get("down_payment_ratio", 0.3))
        annual_rate = float(mortgage_cfg.get("annual_interest_rate", 0.05))
        loan_term = int(mortgage_cfg.get("loan_term_years", 30))
        max_dti = float(mortgage_cfg.get("max_dti_ratio", 0.5))

        down_payment = float(price) * down_ratio
        if available_cash_override is None:
            cash_now = float(getattr(buyer, "cash", 0.0) or 0.0)
        else:
            cash_now = float(available_cash_override or 0.0)

        if cash_now < down_payment:
            return "INSUFFICIENT_DOWN_PAYMENT"
        if cash_now < (down_payment + float(buyer_total_cost or 0.0)):
            return "FEE_SHORTFALL"

        loan_amount = max(0.0, float(price) - down_payment)
        new_monthly = float(calculate_monthly_payment(loan_amount, annual_rate, loan_term))
        existing_payment = float(getattr(buyer, "mortgage_monthly_payment", 0.0) or 0.0)
        income = float(getattr(buyer, "monthly_income", 0.0) or 0.0)
        max_payment = income * max_dti
        if (existing_payment + new_monthly) > max_payment:
            return "DTI_EXCEEDED"

        if int(liquidity_buffer_months or 0) > 0:
            monthly_expense_proxy = max(1.0, income * 0.5)
            required_floor = float(liquidity_buffer_months) * monthly_expense_proxy
            remaining_cash = cash_now - (down_payment + float(buyer_total_cost or 0.0))
            if remaining_cash < required_floor:
                return "INSUFFICIENT_LIQUIDITY_BUFFER"

        leverage_cap = None
        try:
            raw_cap = self.config.get("smart_agent.leverage_cap", self.config.get("leverage_cap", None))
            if raw_cap is not None and str(raw_cap).strip() != "":
                leverage_cap = float(raw_cap)
        except Exception:
            leverage_cap = None

        if leverage_cap is not None and leverage_cap > 0:
            current_assets = float(getattr(buyer, "total_assets", 0.0) or 0.0)
            if current_assets <= 0:
                try:
                    current_assets = float(getattr(buyer, "net_worth", 0.0) or 0.0)
                except Exception:
                    current_assets = float(cash_now)
            new_total_debt = float(getattr(buyer, "total_debt", 0.0) or 0.0) + loan_amount
            asset_base = max(0.0, current_assets) + float(price)
            if asset_base <= 0 or (new_total_debt / asset_base) > leverage_cap:
                return "LEVERAGE_CAP_EXCEEDED"

        return "PASS"

    @staticmethod
    def _map_affordability_reason_to_close_reason(reason_code: str) -> str:
        reason_map = {
            "INSUFFICIENT_DOWN_PAYMENT": "Settlement failed: insufficient down payment",
            "DTI_EXCEEDED": "Settlement failed: DTI exceeded",
            "FEE_SHORTFALL": "Settlement failed: fee shortfall",
            "INSUFFICIENT_LIQUIDITY_BUFFER": "Settlement failed: liquidity buffer",
            "LEVERAGE_CAP_EXCEEDED": "Settlement failed: leverage cap exceeded",
        }
        return reason_map.get(str(reason_code or "PASS"), "Settlement failed: buyer affordability")

    @staticmethod
    def _map_execute_failure_reason(error_code: str) -> Tuple[str, str]:
        """
        Map execute_transaction transient error code to:
        (close_reason, failure_classification)
        """
        code = str(error_code or "").strip().upper()
        if code == "CASH_SHORTFALL_PREQUALIFIED":
            return "Settlement failed: cash reserve drift", "REAL_STATE_CHANGE"
        if code == "AFFORDABILITY_CHECK_FAILED":
            return "Settlement failed: affordability recheck mismatch", "RULE_MISMATCH"
        return "Settlement failed: execute transaction failed", "SYSTEM_EXECUTION"

    def _log_order_precheck(
        self,
        cursor,
        buyer_id: int,
        month: int,
        decision: str,
        reason: str,
        metrics: Dict,
        llm_called: bool = False,
    ):
        self._enqueue_decision_log(
            (
                int(buyer_id),
                int(month),
                "ORDER_PRECHECK",
                str(decision),
                str(reason),
                None,
                json.dumps(metrics or {}, ensure_ascii=False),
                bool(llm_called),
            )
        )

    def _log_bid_validation(
        self,
        cursor,
        buyer_id: int,
        month: int,
        decision: str,
        reason: str,
        metrics: Dict,
        llm_called: bool = False,
    ):
        self._enqueue_decision_log(
            (
                int(buyer_id),
                int(month),
                "BID_VALIDATION",
                str(decision),
                str(reason),
                None,
                json.dumps(metrics or {}, ensure_ascii=False),
                bool(llm_called),
            )
        )

    def _precheck_order_affordability(
        self,
        buyer: Agent,
        listing: Dict,
        offer_price: float,
        available_cash_credit: float = 0.0,
    ) -> Tuple[bool, str, Dict]:
        from mortgage_system import check_affordability

        include_fees = self._resolve_precheck_include_tax_and_fee()
        liquidity_months = self._resolve_precheck_liquidity_buffer_months()
        cash_credit = max(0.0, float(available_cash_credit or 0.0))
        effective_cash = float(getattr(buyer, "cash", 0.0) or 0.0) + cash_credit
        buyer_for_check = buyer
        if cash_credit > 0:
            # Keep presettlement/final-gate cash semantics aligned:
            # frozen order deposit still belongs to the same buyer and should be counted once.
            buyer_for_check = copy.copy(buyer)
            setattr(buyer_for_check, "cash", float(effective_cash))
        buyer_total_cost = 0.0
        if include_fees:
            costs = FinancialCalculator.calculate_transaction_costs(
                float(offer_price),
                config=self.config,
                side="buyer",
            )
            buyer_total_cost = float(costs.get("total", 0.0))

        # Keep precheck aligned with settlement hard floor to avoid pass-then-fail drift.
        liquidity_months = max(liquidity_months, self._resolve_settlement_liquidity_floor_months())
        is_affordable, _, _ = check_affordability(
            buyer_for_check,
            float(offer_price),
            self.config,
            extra_cash_needed=buyer_total_cost,
            liquidity_floor_months_override=liquidity_months,
        )
        reason = self._classify_affordability_failure(
            buyer=buyer_for_check,
            price=float(offer_price),
            buyer_total_cost=buyer_total_cost,
            liquidity_buffer_months=liquidity_months,
            available_cash_override=effective_cash,
        )
        if is_affordable and reason != "PASS":
            reason = "PASS"
        elif (not is_affordable) and reason == "PASS":
            # Fallback bucket for future guardrails not yet classified in detail.
            reason = "INSUFFICIENT_LIQUIDITY_BUFFER"
        metrics = {
            "offer_price": float(offer_price),
            "property_id": int(listing.get("property_id", -1)),
            "include_tax_and_fee": bool(include_fees),
            "buyer_total_cost": float(buyer_total_cost),
            "liquidity_buffer_months": int(liquidity_months),
            "settlement_liquidity_floor_months": int(self._resolve_settlement_liquidity_floor_months()),
            "buyer_cash": float(getattr(buyer, "cash", 0.0) or 0.0),
            "available_cash_credit": float(cash_credit),
            "available_cash_for_check": float(effective_cash),
            "buyer_income": float(getattr(buyer, "monthly_income", 0.0) or 0.0),
        }
        return bool(is_affordable), reason, metrics

    def _close_order(
        self,
        cursor,
        month: int,
        order_id: int,
        buyer: Agent,
        status: str,
        close_reason: str,
        release_amount: float = 0.0,
        penalty_amount: float = 0.0,
    ):
        if release_amount > 0:
            buyer.cash += release_amount
            self._sync_buyer_finance(cursor, buyer)
        cursor.execute(
            """
            UPDATE transaction_orders
            SET status=?, close_month=?, close_reason=?, penalty_amount=?, updated_at=CURRENT_TIMESTAMP
            WHERE order_id=?
            """,
            (status, month, close_reason, penalty_amount, order_id),
        )
        self._append_order_log(
            month,
            "ORDER_CLOSED",
            {
                "order_id": order_id,
                "buyer_id": buyer.id if buyer else None,
                "status": status,
                "reason": close_reason,
                "release_amount": round(float(release_amount or 0.0), 2),
                "penalty_amount": round(float(penalty_amount or 0.0), 2),
            },
        )
        # Sync buyer-match final route for observability.
        final_outcome = str(status or "").upper()
        failure_stage = None
        failure_reason = None
        if final_outcome in ("CANCELLED", "EXPIRED", "BREACHED"):
            failure_reason = str(close_reason or "")
            reason_lower = failure_reason.lower()
            if "settlement failed" in reason_lower or "insufficient down payment" in reason_lower:
                failure_stage = "SETTLEMENT"
            elif "outbid" in reason_lower or "negotiation" in reason_lower or "rejected" in reason_lower:
                failure_stage = "NEGOTIATION"
            else:
                failure_stage = "ORDER"
        if self._table_exists(cursor, "property_buyer_matches"):
            cursor.execute(
                """
                UPDATE property_buyer_matches
                SET final_outcome=?,
                    failure_stage=?,
                    failure_reason=?
                WHERE order_id=?
                """,
                (final_outcome, failure_stage, failure_reason, int(order_id)),
            )

    def _extract_batch_bid_prices(self, history: List[Dict]) -> Tuple[Dict[int, float], Optional[float]]:
        bid_map: Dict[int, float] = {}
        winner_price: Optional[float] = None
        if not isinstance(history, list):
            return bid_map, winner_price
        for event in history:
            if not isinstance(event, dict):
                continue
            action = str(event.get("action", "") or "").upper()
            if action in {"FINAL_BID", "WIN_BID"}:
                buyer_id = event.get("buyer_id", event.get("buyer", event.get("agent_id")))
                try:
                    buyer_id = int(buyer_id) if buyer_id is not None else None
                except Exception:
                    buyer_id = None
                try:
                    price = float(event.get("price", 0.0) or 0.0)
                except Exception:
                    price = 0.0
                if buyer_id is not None and price > 0:
                    bid_map[int(buyer_id)] = float(price)
                if action == "WIN_BID" and price > 0:
                    winner_price = float(price)
        return bid_map, winner_price

    def _derive_outbid_reason(
        self,
        winner: Agent,
        loser: Agent,
        listing: Dict,
        order_entries: List[Dict],
        history: Optional[List[Dict]] = None,
        session_mode: str = "",
    ) -> str:
        """
        Refine generic 'Outbid' into a traceable reason bucket.
        """
        mode = str(session_mode or "").strip().lower()
        if not mode and isinstance(history, list):
            for event in history:
                if not isinstance(event, dict):
                    continue
                if str(event.get("action", "") or "").upper() == "MODEL_ROUTE":
                    mode = str(event.get("session_mode", "") or event.get("mode", "") or "").strip().lower()
                    break
        bid_map, winner_price = self._extract_batch_bid_prices(history or [])
        loser_price = bid_map.get(int(getattr(loser, "id", -1)))
        winner_batch_price = bid_map.get(int(getattr(winner, "id", -1)), winner_price)
        listed_price = float(listing.get("listed_price", 0.0) or 0.0)
        price_gap_guard = max(5000.0, listed_price * 0.005)
        if mode == "batch_bidding" and loser_price and winner_batch_price:
            if abs(float(winner_batch_price) - float(loser_price)) <= price_gap_guard:
                try:
                    ranked_ids = [int(e["buyer"].id) for e in order_entries]
                    if loser.id in ranked_ids:
                        rank = ranked_ids.index(loser.id) + 1
                        if rank >= 3:
                            return f"Outbid: queue priority rank={rank}"
                    return "Outbid: batch tie-break after near-equal final bids"
                except Exception:
                    return "Outbid: batch tie-break after near-equal final bids"
        try:
            wmax = float(getattr(getattr(winner, "preference", None), "max_price", 0.0) or 0.0)
            lmax = float(getattr(getattr(loser, "preference", None), "max_price", 0.0) or 0.0)
            if listed_price > 0 and wmax > 0 and lmax > 0:
                gap_ratio = (wmax - lmax) / listed_price
                if gap_ratio >= 0.30:
                    return "Outbid: winner had significantly stronger budget headroom"
                if gap_ratio >= 0.15:
                    return "Outbid: winner had moderate budget headroom advantage"
        except Exception:
            pass

        try:
            ranked_ids = [int(e["buyer"].id) for e in order_entries]
            if loser.id in ranked_ids:
                rank = ranked_ids.index(loser.id) + 1
                if rank >= 3:
                    return f"Outbid: queue priority rank={rank}"
        except Exception:
            pass

        return "Outbid: lost in negotiation"

    def _slice_history_for_buyer(self, history: List[Dict], buyer_id: int) -> List[Dict]:
        """
        Extract one buyer-specific negotiation slice from consolidated history.
        """
        if not isinstance(history, list):
            return []
        try:
            target_id = int(buyer_id)
        except Exception:
            return []

        start_idx = None
        for idx, event in enumerate(history):
            if not isinstance(event, dict):
                continue
            if str(event.get("party", "")).lower() != "buyer":
                continue
            try:
                aid = int(event.get("agent_id"))
            except Exception:
                continue
            if aid == target_id:
                start_idx = idx
                break

        if start_idx is None:
            return []

        end_idx = len(history)
        for idx in range(start_idx + 1, len(history)):
            event = history[idx]
            if not isinstance(event, dict):
                continue
            if str(event.get("party", "")).lower() != "buyer":
                continue
            try:
                aid = int(event.get("agent_id"))
            except Exception:
                aid = None
            if aid is not None and aid != target_id:
                end_idx = idx
                break
        return history[start_idx:end_idx]

    def _derive_negotiation_failure_reason(
        self,
        session_reason: str,
        history: List[Dict],
        buyer: Agent,
        listing: Dict,
    ) -> str:
        """
        Break down generic "All negotiations failed" into concrete buckets.
        This improves observability and post-run diagnosis without changing
        any decision authority.
        """
        base_reason = str(session_reason or "Negotiation Failed")
        if base_reason != "All negotiations failed":
            return base_reason

        buyer_id = int(getattr(buyer, "id", -1))
        buyer_slice = self._slice_history_for_buyer(history, buyer_id)
        if not buyer_slice:
            return "All negotiations failed: missing buyer-specific trace"

        try:
            listed_price = float(listing.get("listed_price", 0.0) or 0.0)
        except Exception:
            listed_price = 0.0
        try:
            min_price = float(listing.get("min_price", listed_price) or listed_price)
        except Exception:
            min_price = listed_price

        first_offer = None
        last_buyer_offer = None
        first_seller_action = ""
        first_seller_price = None
        last_seller_action = ""
        last_seller_price = None
        buyer_withdrew = False
        had_clamp = False

        for event in buyer_slice:
            if not isinstance(event, dict):
                continue
            party = str(event.get("party", "")).lower()
            action = str(event.get("action", "")).upper()

            if action == "M16_CLAMP":
                had_clamp = True

            if party == "buyer":
                if action == "WITHDRAW":
                    buyer_withdrew = True
                if action == "OFFER" and first_offer is None:
                    try:
                        first_offer = float(event.get("price", 0.0) or 0.0)
                    except Exception:
                        first_offer = 0.0
                if action in {"OFFER", "ACCEPT"}:
                    try:
                        last_buyer_offer = float(event.get("price", 0.0) or 0.0)
                    except Exception:
                        last_buyer_offer = 0.0

            if party in {"seller", "seller_recheck", "seller_closeout"} and not first_seller_action:
                first_seller_action = action
                try:
                    first_seller_price = float(event.get("price", 0.0) or 0.0)
                except Exception:
                    first_seller_price = None
            if party in {"seller", "seller_recheck", "seller_closeout"}:
                last_seller_action = action
                try:
                    last_seller_price = float(event.get("price", 0.0) or 0.0)
                except Exception:
                    last_seller_price = None

        if first_offer is None:
            if buyer_withdrew:
                return "All negotiations failed: buyer withdrew before valid offer"
            if had_clamp:
                return "All negotiations failed: no valid offer after guardrail clamp"
            return "All negotiations failed: no buyer offer in trace"

        if first_seller_action == "REJECT":
            if min_price > 0 and first_offer < min_price - 1e-6:
                return "All negotiations failed: seller rejected offer below floor"
            if listed_price > 0 and first_offer >= listed_price - 1e-6:
                return "All negotiations failed: seller rejected offer at/above ask"
            return "All negotiations failed: seller rejected within negotiation band"

        terminal_seller_action = last_seller_action or first_seller_action

        if terminal_seller_action == "COUNTER":
            if buyer_withdrew:
                return "All negotiations failed: buyer withdrew after seller counter"
            return "All negotiations failed: no agreement after seller counter"

        if terminal_seller_action == "ACCEPT":
            terminal_price = last_seller_price if last_seller_price is not None else first_seller_price
            if terminal_price is not None and min_price > 0 and terminal_price < min_price - 1e-6:
                return "All negotiations failed: accepted price blocked by floor rule"
            if last_buyer_offer is not None and min_price > 0 and float(last_buyer_offer) < min_price - 1e-6:
                return "All negotiations failed: seller accepted offer below floor rule"
            return "All negotiations failed: accepted then blocked by downstream rule"

        if buyer_withdrew:
            return "All negotiations failed: buyer withdrew during negotiation"
        if had_clamp:
            return "All negotiations failed: offer stayed below effective floor after clamp"
        return "All negotiations failed: no agreement reached"

    def _expire_stale_orders(self, cursor, month: int, agent_map: Dict[int, Agent]):
        """Expire pending orders and apply breach penalty (M19)."""
        cfg = self._order_config()
        cursor.execute(
            """
            SELECT order_id, buyer_id, deposit_amount
            FROM transaction_orders
            WHERE status='pending' AND expires_month < ?
            """,
            (month,),
        )
        rows = cursor.fetchall()
        if not rows:
            return

        for row in rows:
            order_id = row[0]
            buyer_id = row[1]
            deposit = float(row[2] or 0.0)
            buyer = agent_map.get(buyer_id)
            if not buyer:
                continue
            penalty = min(deposit, deposit * cfg["breach_penalty_ratio"])
            release = max(0.0, deposit - penalty)
            self._close_order(
                cursor,
                month=month,
                order_id=order_id,
                buyer=buyer,
                status="expired",
                close_reason="Order TTL expired",
                release_amount=release,
                penalty_amount=penalty,
            )

    def _update_wait_state(self, cursor, buyer: Agent, matched: bool):
        """M12: maintain waited_months for buyers (deadline pacing basis)."""
        blocked = bool(getattr(buyer, "_blocked_by_target_price", False))
        cursor.execute(
            "SELECT waited_months, max_wait_months FROM active_participants WHERE agent_id=?",
            (buyer.id,),
        )
        row = cursor.fetchone()
        if not row:
            return
        waited = int(row[0] or 0)
        max_wait = int(row[1] or 6)
        if matched:
            waited = 0
        else:
            # Count one month of waiting when no match happened.
            waited = min(max_wait + 12, waited + 1)
            # If this month was explicitly blocked by target-price strictness, nudge faster.
            if blocked:
                waited = min(max_wait + 12, waited + 1)
        cursor.execute(
            "UPDATE active_participants SET waited_months=? WHERE agent_id=?",
            (waited, buyer.id),
        )
        buyer.waited_months = waited

    def _derive_no_selection_reason_code(
        self,
        *,
        listings_for_buyer_count: int,
        selected_ids: List[int],
        match_ctx: Dict,
        retry_trace: List[Dict],
        retry_budget: Dict,
    ) -> str:
        """
        Build a stable reason code for BUYER_MATCH selected=[] cases.
        This improves post-run diagnostics without changing buyer autonomy.
        """
        if selected_ids:
            return "HAS_SELECTION"
        if int(listings_for_buyer_count or 0) <= 0:
            return "NO_ACTIVE_LISTINGS"
        if any(str(x.get("result", "")).strip().lower() == "structural_no_candidate_defer_next_month" for x in (retry_trace or [])):
            return "STRUCTURAL_NO_CANDIDATE_DEFERRED"
        if bool(match_ctx.get("stop_search_this_month", False)):
            return "LLM_STOP_THIS_MONTH"
        selection_reason = str(match_ctx.get("selection_reason", "") or "").strip().lower()
        if selection_reason == "all_shortlist_over_crowd_tolerance":
            return "CROWD_HARD_GATE_ALL_BLOCKED"
        if any(str(x.get("result", "")).strip().lower() == "regime_v1_stop_same_month_loop" for x in (retry_trace or [])):
            return "SAME_MONTH_LOOP_STOP"
        attempts_spent = int(retry_budget.get("attempts_spent", 0) or 0)
        attempts_budget = int(retry_budget.get("attempt_budget", 0) or 0)
        if attempts_budget > 0 and attempts_spent >= attempts_budget:
            return "RETRY_BUDGET_EXHAUSTED"
        if int(retry_budget.get("llm_stop_signals", 0) or 0) > 0:
            return "LLM_STOP_SIGNALLED"
        return "NO_ELIGIBLE_CANDIDATE"

    def _build_buyer_match_summary_log(
        self,
        *,
        buyer: Agent,
        month: int,
        strategy_profile: str,
        selected_ids: List[int],
        no_selection_code: str,
        listings_for_buyer_count: int,
        shortlist_ids: List[int],
        quota_prefilter_enabled: bool,
        quota_blocked_ids: List[int],
        quota_blocked_meta: List[Dict],
        quota_charge_ids: List[int],
        weight_payload: Dict,
        match_ctx: Dict,
        retry_trace: List[Dict],
        retry_budget: Dict,
        pipeline_stage_trace: List[str],
        pipeline_order_violation: bool,
        market_trend: str,
        observed_trend: str,
        observed_month: int,
        observed_delay: int,
    ) -> tuple:
        reason = (
            f"selected={selected_ids}"
            if selected_ids
            else f"selected={selected_ids}; no_selection_code={no_selection_code}"
        )
        thought = {
            "strategy_profile": strategy_profile,
            "selected_property_ids": [int(pid) for pid in (selected_ids or []) if pid is not None],
            "no_selection_reason_code": no_selection_code if not selected_ids else "HAS_SELECTION",
            "listings_for_buyer_count": int(listings_for_buyer_count),
            "shortlist_property_ids": [int(pid) for pid in (shortlist_ids or []) if pid is not None],
            "shortlist_count": int(len(shortlist_ids or [])),
            "candidate_quota_prefilter_enabled": bool(quota_prefilter_enabled),
            "candidate_quota_blocked_count": int(len(quota_blocked_ids or [])),
            "candidate_quota_blocked_property_ids": list(quota_blocked_ids or []),
            "candidate_quota_blocked_meta": list(quota_blocked_meta or []),
            "candidate_quota_charge_ids": [int(pid) for pid in (quota_charge_ids or []) if pid is not None],
            "weights": weight_payload or {},
            "persona_snapshot": match_ctx.get("persona_snapshot", {}),
            "selection_reason": match_ctx.get("selection_reason", ""),
            "selected_in_shortlist": match_ctx.get("selected_in_shortlist", False),
            "crowd_mode": match_ctx.get("crowd_mode", ""),
            "crowd_profile_reasons": match_ctx.get("crowd_profile_reasons", []),
            "retry_attempt": match_ctx.get("retry_attempt", 0),
            "excluded_property_count": match_ctx.get("excluded_property_count", 0),
            "retry_trace": retry_trace or [],
            "retry_budget": retry_budget or {},
            "llm_route_model": match_ctx.get("llm_route_model", ""),
            "llm_route_reason": match_ctx.get("llm_route_reason", ""),
            "llm_gray_score": match_ctx.get("llm_gray_score", 0.0),
            "factor_contract": match_ctx.get("factor_contract", {}),
            "bucket_plan": match_ctx.get("bucket_plan", {}),
            "bucket_distribution": match_ctx.get("bucket_distribution", {}),
            "pipeline_stage_trace": pipeline_stage_trace or [],
            "pipeline_order_violation": bool(pipeline_order_violation),
            "market_trend_global": market_trend,
            "observed_market_trend": observed_trend,
            "observed_market_month": observed_month,
            "m14_info_delay_months": observed_delay,
        }
        metrics = {
            **(weight_payload or {}),
            "shortlist_count": int(len(shortlist_ids or [])),
            "listings_for_buyer_count": int(listings_for_buyer_count),
            "candidate_quota_blocked_count": int(len(quota_blocked_ids or [])),
            "candidate_quota_prefilter_enabled": bool(quota_prefilter_enabled),
            "llm_route_model": match_ctx.get("llm_route_model", ""),
            "llm_route_reason": match_ctx.get("llm_route_reason", ""),
            "llm_gray_score": match_ctx.get("llm_gray_score", 0.0),
            "crowd_mode": match_ctx.get("crowd_mode", ""),
            "retry_attempt": match_ctx.get("retry_attempt", 0),
            "factor_contract": match_ctx.get("factor_contract", {}),
            "bucket_plan": match_ctx.get("bucket_plan", {}),
            "bucket_distribution": match_ctx.get("bucket_distribution", {}),
            "pipeline_stage_trace": pipeline_stage_trace or [],
            "pipeline_order_violation": bool(pipeline_order_violation),
            "market_trend_global": market_trend,
            "observed_market_trend": observed_trend,
            "observed_market_month": observed_month,
            "m14_info_delay_months": observed_delay,
        }
        return (
            int(getattr(buyer, "id", -1) or -1),
            int(month),
            "BUYER_MATCH_SUMMARY",
            str(strategy_profile or "unknown"),
            str(reason),
            json.dumps(thought, ensure_ascii=False),
            json.dumps(metrics, ensure_ascii=False),
            bool(match_ctx.get("llm_called", True)),
        )

    def _register_buyer_failure(self, cursor, buyer: Agent, month: int, reason: str):
        """M15: failure recovery state update."""
        cursor.execute(
            """
            SELECT consecutive_failures, cooldown_months, risk_mode, max_price, target_buy_price
            FROM active_participants
            WHERE agent_id=?
            """,
            (buyer.id,),
        )
        row = cursor.fetchone()
        if not row:
            return
        consecutive = int(row[0] or 0) + 1
        cooldown = int(row[1] or 0)
        risk_mode = (row[2] or "balanced").lower()
        max_price = float(row[3] or 0.0)
        target_buy_price = float(row[4] or 0.0)

        # Step-down policy: after repeated failures become more conservative.
        if consecutive >= 2:
            risk_mode = "conservative"
            if max_price > 0:
                max_price *= 0.95
            if target_buy_price > 0:
                target_buy_price *= 0.97
        if consecutive >= 3:
            cooldown = max(cooldown, 1)

        cursor.execute(
            """
            UPDATE active_participants
            SET consecutive_failures=?,
                cooldown_months=?,
                risk_mode=?,
                max_price=?,
                target_buy_price=?
            WHERE agent_id=?
            """,
            (consecutive, cooldown, risk_mode, max_price, target_buy_price, buyer.id),
        )
        buyer.consecutive_failures = consecutive
        buyer.cooldown_months = cooldown
        buyer.risk_mode = risk_mode
        if hasattr(buyer, "preference") and max_price > 0:
            buyer.preference.max_price = min(buyer.preference.max_price, max_price)
        if target_buy_price > 0:
            buyer.target_buy_price = target_buy_price
        reason_lower = str(reason or "").strip().lower()
        buy_task_locked = any(
            token in reason_lower
            for token in (
                "outbid",
                "budget headroom",
                "queue priority",
                "lost in negotiation",
            )
        )
        if buy_task_locked:
            # Keep replacement / starter buyers on the buy path after being displaced.
            setattr(buyer, "_buy_task_locked", True)
            setattr(buyer, "_buy_task_lock_reason", str(reason or ""))
            setattr(buyer, "_buy_task_lock_month", int(month))
        self._append_order_log(
            month,
            "BUYER_FAILURE_RECOVERY",
            {
                "buyer_id": buyer.id,
                "reason": reason,
                "consecutive_failures": consecutive,
                "cooldown_months": cooldown,
                "risk_mode": risk_mode,
                "buy_task_locked": bool(buy_task_locked),
            },
        )
        recoverable_failure = any(
            token in reason_lower
            for token in (
                "outbid",
                "negotiation",
                "no valid bids",
                "rejected",
                "queue priority",
                "buy cap reached",
            )
        )
        if recoverable_failure and not bool(getattr(buyer, "buy_completed", 0)):
            self._upsert_recovery_queue(
                cursor=cursor,
                month=int(month),
                buyer_id=int(getattr(buyer, "id", -1)),
                lock_reason=str(reason or ""),
            )

    def _register_buyer_success(self, cursor, buyer_id: int, month: Optional[int] = None):
        cursor.execute(
            """
            UPDATE active_participants
            SET consecutive_failures=0,
                cooldown_months=0,
                waited_months=0,
                buy_completed=1
            WHERE agent_id=?
            """,
            (buyer_id,),
        )
        if month is not None:
            self._mark_recovery_queue_state(
                cursor=cursor,
                month=int(month),
                buyer_id=int(buyer_id),
                state="completed",
                progress_round=0,
            )

    def _register_seller_chain_progress(self, cursor, seller_id: int):
        if seller_id is None or seller_id < 0:
            return
        cursor.execute(
            """
            UPDATE active_participants
            SET sell_completed=1
            WHERE agent_id=? AND role='BUYER_SELLER'
            """,
            (seller_id,),
        )

    def _create_orders_for_matches(self, cursor, month: int, buyer_matches: List[Dict], agent_map: Dict[int, Agent]) -> List[Dict]:
        """Create pending orders and freeze deposits. Return valid order entries."""
        cfg = self._order_config()
        bid_floor_ratio = self._resolve_effective_bid_floor_ratio()
        try:
            gate_force_multi_buyer_pool = bool(
                self.config.get(
                    "smart_agent.gate_force_multi_buyer_pool_enabled",
                    self.config.get("gate_force_multi_buyer_pool_enabled", False),
                )
            )
        except Exception:
            gate_force_multi_buyer_pool = False
        # Keep bypass strictly gate-only to avoid polluting normal experiment conclusions.
        gate_bypass_precheck = bool(gate_force_multi_buyer_pool)
        try:
            pending_order_cap = int(
                self.config.get("smart_agent.buyer_pending_order_cap", self.config.get("buyer_pending_order_cap", 1))
            )
        except Exception:
            pending_order_cap = 1
        pending_order_cap = max(1, min(3, pending_order_cap))
        try:
            backup_slot_buffer_cap = int(
                self.config.get("smart_agent.backup_slot_buffer_cap", self.config.get("backup_slot_buffer_cap", 6))
            )
        except Exception:
            backup_slot_buffer_cap = 6
        backup_slot_buffer_cap = max(0, min(12, backup_slot_buffer_cap))
        entries = []
        expires_month = month + cfg["ttl_months"] - 1
        cursor.execute(
            """
            SELECT buyer_id, COUNT(*)
            FROM transaction_orders
            WHERE status IN ('pending', 'pending_settlement')
            GROUP BY buyer_id
            """
        )
        buyer_active_order_count = {
            int(r[0]): int(r[1] or 0) for r in cursor.fetchall() if r and r[0] is not None
        }
        backup_reset_done: set[int] = set()
        property_status_cache: Dict[int, str] = {}
        has_properties_market_table = self._table_exists(cursor, "properties_market")

        for m in buyer_matches:
            buyer = m["buyer"]
            listing = m["listing"]
            buyer_id = int(getattr(buyer, "id", -1))
            recovery_mode = bool(getattr(buyer, "_recovery_queue_mode", False))
            recovery_round = int(getattr(buyer, "_recovery_round", 0) or 0)
            if buyer_id >= 0 and buyer_id not in backup_reset_done:
                pool_month = int(getattr(buyer, "_backup_slot_pool_month", -1) or -1)
                if pool_month != int(month):
                    setattr(buyer, "_backup_slot_pool", [])
                    setattr(buyer, "_backup_slot_pool_month", int(month))
                    setattr(buyer, "_backup_slot_preferred_ids", [])
                backup_reset_done.add(buyer_id)
            seller_id_raw = listing.get("seller_id", listing.get("owner_id", -1))
            try:
                seller_id = int(seller_id_raw) if seller_id_raw is not None else -1
            except Exception:
                seller_id = -1
            # Hard guard: never create order where buyer == seller.
            if seller_id >= 0 and int(buyer.id) == int(seller_id):
                self._log_order_precheck(
                    cursor=cursor,
                    buyer_id=buyer.id,
                    month=month,
                    decision="REJECT",
                    reason="SELF_TRADE_BLOCKED",
                    metrics={
                        "property_id": int(listing.get("property_id", -1)),
                        "buyer_id": int(buyer.id),
                        "seller_id": int(seller_id),
                    },
                    llm_called=False,
                )
                continue
            pid_i = int(listing.get("property_id", -1) or -1)
            if pid_i < 0:
                if recovery_mode and buyer_id >= 0:
                    self._record_recovery_attempt(
                        cursor, month, buyer_id, int(max(-1, pid_i)), recovery_round, "INVALID_PROPERTY_ID"
                    )
                self._log_order_precheck(
                    cursor=cursor,
                    buyer_id=buyer.id,
                    month=month,
                    decision="REJECT",
                    reason="INVALID_PROPERTY_ID",
                    metrics={
                        "property_id": int(pid_i),
                        "buyer_id": int(buyer.id),
                        "seller_id": int(seller_id),
                    },
                    llm_called=False,
                )
                continue
            if has_properties_market_table:
                if pid_i not in property_status_cache:
                    cursor.execute(
                        "SELECT status FROM properties_market WHERE property_id=?",
                        (int(pid_i),),
                    )
                    row_status = cursor.fetchone()
                    property_status_cache[int(pid_i)] = (
                        str(row_status[0]) if row_status and row_status[0] is not None else "MISSING"
                    )
                db_status = str(property_status_cache.get(int(pid_i), "MISSING") or "MISSING")
            else:
                db_status = "for_sale"
            if db_status != "for_sale":
                if recovery_mode and buyer_id >= 0 and pid_i >= 0:
                    self._record_recovery_attempt(
                        cursor, month, buyer_id, pid_i, recovery_round, "PROPERTY_NOT_FOR_SALE_DB_STATUS"
                    )
                existing_pool = list(getattr(buyer, "_backup_slot_pool", []) or [])
                if existing_pool:
                    keep_pool = []
                    removed_count = 0
                    for item in existing_pool:
                        try:
                            pool_pid = int(item.get("property_id", -1))
                        except Exception:
                            pool_pid = -1
                        if pool_pid == int(pid_i):
                            removed_count += 1
                            continue
                        keep_pool.append(item)
                    if removed_count > 0:
                        setattr(buyer, "_backup_slot_pool", keep_pool)
                        self._append_order_log(
                            month,
                            "BACKUP_SLOT_EVICTED",
                            {
                                "buyer_id": int(buyer.id),
                                "property_id": int(pid_i),
                                "removed_count": int(removed_count),
                                "reason": "PROPERTY_NOT_FOR_SALE_DB_STATUS",
                            },
                        )
                self._log_order_precheck(
                    cursor=cursor,
                    buyer_id=buyer.id,
                    month=month,
                    decision="REJECT",
                    reason="PROPERTY_NOT_FOR_SALE_DB_STATUS",
                    metrics={
                        "property_id": int(pid_i),
                        "db_status": str(db_status),
                        "buyer_id": int(buyer.id),
                    },
                    llm_called=False,
                )
                self._append_order_log(
                    month,
                    "ORDER_REJECTED_PROPERTY_STATUS",
                    {
                        "buyer_id": int(buyer.id),
                        "property_id": int(pid_i),
                        "db_status": str(db_status),
                    },
                )
                continue
            active_count = int(buyer_active_order_count.get(int(buyer.id), 0))
            if active_count >= pending_order_cap:
                if recovery_mode and buyer_id >= 0 and pid_i >= 0:
                    self._record_recovery_attempt(
                        cursor, month, buyer_id, pid_i, recovery_round, "PENDING_CAP_REACHED"
                    )
                existing_pool = list(getattr(buyer, "_backup_slot_pool", []) or [])
                existing_ids = set()
                for item in existing_pool:
                    try:
                        existing_ids.add(int(item.get("property_id", -1)))
                    except Exception:
                        continue
                if (
                    backup_slot_buffer_cap > 0
                    and pid_i >= 0
                    and pid_i not in existing_ids
                    and len(existing_pool) < int(backup_slot_buffer_cap)
                ):
                    existing_pool.append(
                        {
                            "property_id": int(pid_i),
                            "candidate_rank": int(listing.get("candidate_rank", 1) or 1),
                            "listed_price": float(listing.get("listed_price", 0.0) or 0.0),
                            "owner_id": int(listing.get("seller_id", listing.get("owner_id", -1)) or -1),
                        }
                    )
                    setattr(buyer, "_backup_slot_pool", existing_pool)
                    self._append_order_log(
                        month,
                        "BACKUP_SLOT_BUFFERED",
                        {
                            "buyer_id": int(buyer.id),
                            "property_id": int(pid_i),
                            "candidate_rank": int(listing.get("candidate_rank", 1) or 1),
                            "pool_size": int(len(existing_pool)),
                            "pool_cap": int(backup_slot_buffer_cap),
                            "reason": "PENDING_CAP_REACHED",
                        },
                    )
                logger.debug(
                    f"Order skipped: buyer {buyer.id} active pending orders reached cap "
                    f"({active_count}/{pending_order_cap})"
                )
                continue
            offer_price = float(listing["listed_price"])

            # P1-1: effective bid validity floor (physics check, not replacing LLM choice).
            min_price = float(listing.get("min_price", offer_price) or offer_price)
            effective_floor = max(0.0, min_price * bid_floor_ratio)
            if offer_price < effective_floor:
                if recovery_mode and buyer_id >= 0 and pid_i >= 0:
                    self._record_recovery_attempt(
                        cursor, month, buyer_id, pid_i, recovery_round, "INVALID_BID_BELOW_FLOOR"
                    )
                self._log_bid_validation(
                    cursor=cursor,
                    buyer_id=buyer.id,
                    month=month,
                    decision="INVALID_BID",
                    reason="INVALID_BID_BELOW_FLOOR",
                    metrics={
                        "offer_price": float(offer_price),
                        "min_price": float(min_price),
                        "effective_bid_floor_ratio": float(bid_floor_ratio),
                        "effective_floor": float(effective_floor),
                        "property_id": int(listing.get("property_id", -1)),
                    },
                    llm_called=False,
                )
                continue

            # P0-1: settlement precheck before creating order.
            if gate_bypass_precheck:
                precheck_metrics = {
                    "gate_mode": True,
                    "property_id": int(listing.get("property_id", -1)),
                    "offer_price": float(offer_price),
                }
                self._log_order_precheck(
                    cursor=cursor,
                    buyer_id=buyer.id,
                    month=month,
                    decision="PASS",
                    reason="GATE_BYPASS_PRECHECK",
                    metrics=precheck_metrics,
                    llm_called=False,
                )
                deposit = 0.0
            else:
                ok_precheck, precheck_reason, precheck_metrics = self._precheck_order_affordability(
                    buyer=buyer,
                    listing=listing,
                    offer_price=offer_price,
                )
                if not ok_precheck:
                    if recovery_mode and buyer_id >= 0 and pid_i >= 0:
                        self._record_recovery_attempt(
                            cursor, month, buyer_id, pid_i, recovery_round, str(precheck_reason)
                        )
                    self._log_order_precheck(
                        cursor=cursor,
                        buyer_id=buyer.id,
                        month=month,
                        decision="REJECT",
                        reason=precheck_reason,
                        metrics=precheck_metrics,
                        llm_called=False,
                    )
                    continue
                self._log_order_precheck(
                    cursor=cursor,
                    buyer_id=buyer.id,
                    month=month,
                    decision="PASS",
                    reason="PASS",
                    metrics=precheck_metrics,
                    llm_called=False,
                )

                deposit = max(0.0, offer_price * cfg["deposit_ratio"])
                if buyer.cash < deposit:
                    if recovery_mode and buyer_id >= 0 and pid_i >= 0:
                        self._record_recovery_attempt(
                            cursor, month, buyer_id, pid_i, recovery_round, "INSUFFICIENT_DEPOSIT_CASH"
                        )
                    logger.info(
                        f"Order skipped: buyer {buyer.id} insufficient cash for deposit "
                        f"(need {deposit:,.0f}, have {buyer.cash:,.0f})"
                    )
                    continue

                buyer.cash -= deposit
                self._sync_buyer_finance(cursor, buyer)

            cursor.execute(
                """
                INSERT INTO transaction_orders
                (created_month, expires_month, settlement_due_month, buyer_id, seller_id, property_id, offer_price,
                 agreed_price, negotiation_rounds, deposit_amount, penalty_amount, status, close_month, close_reason, agent_type)
                VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?, 0, 'pending', NULL, NULL, ?)
                """,
                (
                    month,
                    expires_month,
                    buyer.id,
                    int(seller_id),
                    listing["property_id"],
                    offer_price,
                    deposit,
                    getattr(buyer, "agent_type", "normal"),
                ),
            )
            listing_selection_snapshot = dict(listing.get("_selection_snapshot", {}) or {})
            merged_match_context = dict(listing_selection_snapshot or (getattr(buyer, "_last_buyer_match_context", {}) or {}))
            merged_match_context["retry_trace"] = list(
                listing.get("_selection_retry_trace", [])
                or getattr(buyer, "_last_buyer_match_retry_trace", [])
                or []
            )
            merged_match_context["retry_budget"] = dict(
                listing.get("_selection_retry_budget", {})
                or getattr(buyer, "_last_buyer_match_retry_budget", {})
                or {}
            )

            entries.append(
                {
                    "order_id": int(cursor.lastrowid),
                    "buyer": buyer,
                    "listing": listing,
                    "deposit_amount": deposit,
                    "match_context": merged_match_context,
                    "selection_reason": str(
                        merged_match_context.get("selection_reason", "")
                    ),
                    "selected_in_shortlist": bool(
                        merged_match_context.get("selected_in_shortlist", False)
                    ),
                }
            )
            if recovery_mode and buyer_id >= 0 and pid_i >= 0:
                self._record_recovery_attempt(
                    cursor, month, buyer_id, pid_i, recovery_round, "ORDER_CREATED"
                )
            buyer_active_order_count[int(buyer.id)] = active_count + 1
            # Main order created, keep backup queue only for candidate_rank>1 listings.
            # Explicitly remove same property from backup pool to avoid same-month duplicate attempt.
            existing_pool = list(getattr(buyer, "_backup_slot_pool", []) or [])
            if existing_pool:
                keep_pool = []
                created_pid = int(listing.get("property_id", -1) or -1)
                for item in existing_pool:
                    try:
                        pid_i = int(item.get("property_id", -1))
                    except Exception:
                        pid_i = -1
                    if pid_i == created_pid:
                        continue
                    keep_pool.append(item)
                setattr(buyer, "_backup_slot_pool", keep_pool)
            self._append_order_log(
                month,
                "ORDER_CREATED",
                {
                    "order_id": int(cursor.lastrowid),
                    "buyer_id": buyer.id,
                    "seller_id": int(seller_id),
                    "property_id": listing["property_id"],
                    "offer_price": round(float(offer_price), 2),
                    "deposit_amount": round(float(deposit), 2),
                    "agent_type": getattr(buyer, "agent_type", "normal"),
                    "buyer_active_pending_orders": int(buyer_active_order_count[int(buyer.id)]),
                    "buyer_pending_order_cap": int(pending_order_cap),
                    "candidate_rank": int(listing.get("candidate_rank", 1) or 1),
                },
            )

        self._flush_decision_log_buffer(cursor)
        return entries

    def _move_order_to_pending_settlement(
        self,
        cursor,
        month: int,
        order_id: int,
        agreed_price: float,
        negotiation_rounds: int,
    ) -> bool:
        cfg = self._order_config()
        settlement_due_month = month + cfg["settlement_delay_months"]
        cursor.execute(
            """
            UPDATE transaction_orders
            SET status='pending_settlement',
                agreed_price=?,
                negotiation_rounds=?,
                settlement_due_month=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE order_id=? AND status='pending'
            """,
            (agreed_price, negotiation_rounds, settlement_due_month, order_id),
        )
        if int(cursor.rowcount or 0) <= 0:
            self._append_order_log(
                month,
                "ORDER_PENDING_SETTLEMENT_REJECTED",
                {
                    "order_id": int(order_id),
                    "reason": "ORDER_NOT_PENDING",
                },
            )
            return False
        self._append_order_log(
            month,
            "ORDER_PENDING_SETTLEMENT",
            {
                "order_id": order_id,
                "agreed_price": round(float(agreed_price), 2),
                "negotiation_rounds": int(negotiation_rounds),
                "settlement_due_month": int(settlement_due_month),
            },
        )
        if self._table_exists(cursor, "property_buyer_matches"):
            cursor.execute(
                """
                UPDATE property_buyer_matches
                SET final_outcome='PENDING_SETTLEMENT',
                    failure_stage=NULL,
                    failure_reason=NULL,
                    final_price=?
                WHERE order_id=?
                """,
                (float(agreed_price), int(order_id)),
            )
        return True

    def _lock_property_for_pending_settlement(self, cursor, month: int, property_id: int) -> bool:
        """
        Acquire a strong per-property lock by state transition for_sale -> pending_settlement.
        """
        cursor.execute(
            """
            UPDATE properties_market
            SET status='pending_settlement'
            WHERE property_id=? AND status='for_sale'
            """,
            (int(property_id),),
        )
        locked = int(cursor.rowcount or 0) > 0
        if not locked:
            self._append_order_log(
                month,
                "PROPERTY_LOCK_FAILED",
                {
                    "property_id": int(property_id),
                    "reason": "NOT_FOR_SALE_AT_LOCK_TIME",
                },
            )
        return locked

    def _process_due_settlements(self, cursor, month: int, agent_map: Dict[int, Agent], props_map: Dict, market) -> int:
        """
        Settle orders that reached settlement_due_month.
        Returns count of settled transactions.
        """
        cfg = self._order_config()
        settled_count = 0
        snapshot_select = ""
        if self._column_exists(cursor, "transaction_orders", "prequal_cash"):
            snapshot_select = ", prequal_cash, prequal_total_debt, prequal_owned_property_count"
        cursor.execute(
            f"""
            SELECT order_id, buyer_id, seller_id, property_id, agreed_price, deposit_amount, negotiation_rounds{snapshot_select}
            FROM transaction_orders
            WHERE status='pending_settlement' AND settlement_due_month <= ?
            """,
            (month,),
        )
        due_rows = cursor.fetchall()
        if not due_rows:
            return 0

        from transaction_engine import execute_transaction

        for row in due_rows:
            order_id = int(row[0])
            buyer_id = int(row[1])
            seller_id = int(row[2]) if row[2] is not None else -1
            pid = int(row[3])
            agreed_price = float(row[4] or 0.0)
            deposit = float(row[5] or 0.0)
            neg_rounds = int(row[6] or 0)
            prequal_cash = float(row[7]) if len(row) > 7 and row[7] is not None else None
            prequal_total_debt = float(row[8]) if len(row) > 8 and row[8] is not None else None
            prequal_owned_property_count = int(row[9]) if len(row) > 9 and row[9] is not None else None

            buyer = agent_map.get(buyer_id)
            prop_data = props_map.get(pid)
            if not buyer:
                # Seller-side/object-side breach: fully release deposit to buyer if buyer still exists.
                cursor.execute(
                    """
                    UPDATE transaction_orders
                    SET status='breached', close_month=?, close_reason=?, penalty_amount=0, updated_at=CURRENT_TIMESTAMP
                    WHERE order_id=?
                    """,
                    (month, "Settlement failed: missing buyer", order_id),
                )
                continue

            # Always trust DB as source of truth for settlement status.
            cursor.execute(
                """
                SELECT pm.status, pm.owner_id, ps.zone, ps.building_area, ps.is_school_district, ps.property_type
                FROM properties_market pm
                JOIN properties_static ps ON pm.property_id = ps.property_id
                WHERE pm.property_id=?
                """,
                (pid,),
            )
            p_row = cursor.fetchone()
            if not p_row:
                self._close_order(
                    cursor, month, order_id, buyer, "breached", "Settlement failed: property missing",
                    release_amount=deposit, penalty_amount=0.0
                )
                continue

            db_status = str(p_row[0] or "").lower()
            db_owner_id = int(p_row[1]) if p_row[1] is not None else None
            if prop_data is None:
                prop_data = {
                    "property_id": pid,
                    "owner_id": db_owner_id,
                    "status": db_status,
                    "zone": p_row[2],
                    "building_area": p_row[3],
                    "is_school_district": bool(p_row[4]),
                    "property_type": p_row[5],
                }
                props_map[pid] = prop_data
            else:
                prop_data["status"] = db_status
                if db_owner_id is not None:
                    prop_data["owner_id"] = db_owner_id

            # Keep property locked before settlement (DB authoritative).
            if db_status not in ("pending_settlement", "for_sale"):
                if db_status == "off_market":
                    # Property already transferred by another completed order. Treat this as
                    # stale order cleanup and release frozen deposit to avoid false unavailable tails.
                    self._append_order_log(
                        month,
                        "STALE_ORDER_CLEANED_AFTER_TRANSFER",
                        {
                            "order_id": int(order_id),
                            "buyer_id": int(buyer.id),
                            "property_id": int(pid),
                            "db_status": str(db_status),
                            "reason": "property_already_transferred",
                        },
                    )
                    self._close_order(
                        cursor, month, order_id, buyer, "cancelled",
                        "Order cleaned: property already transferred",
                        release_amount=deposit, penalty_amount=0.0
                    )
                else:
                    self._close_order(
                        cursor, month, order_id, buyer, "breached",
                        f"Settlement failed: property unavailable (status={db_status})",
                        release_amount=0.0, penalty_amount=0.0
                    )
                continue

            # Convert frozen deposit back to available cash before affordability/transaction execution.
            if deposit > 0:
                buyer.cash += deposit
                self._sync_buyer_finance(cursor, buyer)

            buyer_costs = FinancialCalculator.calculate_transaction_costs(
                agreed_price, config=self.config, side="buyer"
            )
            seller_costs = FinancialCalculator.calculate_transaction_costs(
                agreed_price, config=self.config, side="seller"
            )
            buyer_total_cost = float(buyer_costs.get("total", 0.0))
            seller_total_cost = float(seller_costs.get("total", 0.0))

            settlement_recheck_enabled = self._resolve_settlement_affordability_recheck_enabled()
            buyer_state_drifted = self._buyer_state_drifted_since_precheck(
                buyer=buyer,
                prequal_cash=prequal_cash,
                prequal_total_debt=prequal_total_debt,
                prequal_owned_property_count=prequal_owned_property_count,
            )
            should_recheck = bool(settlement_recheck_enabled or buyer_state_drifted)
            if should_recheck:
                if settlement_recheck_enabled:
                    ok_settle, reason_code, settle_metrics = self._precheck_order_affordability(
                        buyer=buyer,
                        listing={"property_id": int(pid)},
                        offer_price=float(agreed_price),
                    )
                else:
                    ok_settle, reason_code, settle_metrics = self._precheck_settlement_cash_consistency(
                        buyer=buyer,
                        listing={"property_id": int(pid)},
                        offer_price=float(agreed_price),
                    )
                if not ok_settle:
                    penalty = min(deposit, deposit * cfg["breach_penalty_ratio"])
                    if penalty > 0:
                        buyer.cash = max(0.0, float(buyer.cash) - float(penalty))
                        self._sync_buyer_finance(cursor, buyer)
                    self._append_order_log(
                        month,
                        "SETTLEMENT_PRECHECK_REJECTED",
                        {
                            "order_id": int(order_id),
                            "buyer_id": int(buyer.id),
                            "property_id": int(pid),
                            "reason_code": str(reason_code),
                            "metrics": settle_metrics,
                        },
                    )
                    self._close_order(
                        cursor,
                        month,
                        order_id,
                        buyer,
                        "breached",
                        self._map_affordability_reason_to_close_reason(reason_code),
                        release_amount=0.0,
                        penalty_amount=penalty,
                    )
                    continue
                self._append_order_log(
                    month,
                    "SETTLEMENT_PRECHECK_REPLAYED",
                    {
                        "order_id": int(order_id),
                        "buyer_id": int(buyer.id),
                        "property_id": int(pid),
                        "reason": "BUYER_STATE_DRIFTED_SINCE_PRECHECK" if buyer_state_drifted else "CONFIG_ENABLED",
                    },
                )
            else:
                self._append_order_log(
                    month,
                    "SETTLEMENT_PRECHECK_SKIPPED",
                    {
                        "order_id": int(order_id),
                        "buyer_id": int(buyer.id),
                        "property_id": int(pid),
                        "reason": "PRESETTLEMENT_GATE_IS_FINAL_AND_STATE_STABLE",
                    },
                )

            tx_record = None
            if seller_id == -1:
                # Developer settlement
                down_ratio = self.config.mortgage.get('down_payment_ratio', 0.3)
                down_payment = agreed_price * down_ratio
                loan_amount = agreed_price - down_payment
                if buyer.cash < (down_payment + buyer_total_cost):
                    penalty = min(deposit, deposit * cfg["breach_penalty_ratio"])
                    if penalty > 0:
                        buyer.cash = max(0.0, float(buyer.cash) - float(penalty))
                        self._sync_buyer_finance(cursor, buyer)
                    self._close_order(
                        cursor, month, order_id, buyer, "breached", "Settlement failed: buyer insufficient down payment",
                        release_amount=0.0, penalty_amount=penalty
                    )
                    continue

                if self.developer_service:
                    self.developer_service.record_sale(max(0.0, agreed_price - seller_total_cost), month)

                buyer.cash -= (down_payment + buyer_total_cost)
                annual_rate = self.config.mortgage.get('annual_interest_rate', 0.05)
                loan_term = self.config.mortgage.get('loan_term_years', 30)
                new_monthly_payment = calculate_monthly_payment(loan_amount, annual_rate, loan_term)
                buyer.mortgage_monthly_payment += new_monthly_payment
                buyer.total_debt += loan_amount
                buyer.net_cashflow = buyer.monthly_income - buyer.mortgage_monthly_payment

                new_prop = dict(prop_data)
                new_prop['owner_id'] = buyer.id
                new_prop['status'] = 'off_market'
                new_prop['last_transaction_price'] = agreed_price
                buyer.owned_properties.append(new_prop)
                prop_data['owner_id'] = buyer.id
                prop_data['status'] = 'off_market'
                prop_data['last_transaction_price'] = agreed_price
                tx_record = {
                    "price": agreed_price,
                    "down_payment": down_payment,
                    "loan_amount": loan_amount,
                    "buyer_transaction_cost": buyer_total_cost,
                    "seller_transaction_cost": seller_total_cost,
                }
            else:
                seller_agent = agent_map.get(seller_id)
                if not seller_agent:
                    self._close_order(
                        cursor, month, order_id, buyer, "breached", "Settlement failed: seller missing",
                        release_amount=0.0, penalty_amount=0.0
                    )
                    continue
                tx_record = execute_transaction(
                    buyer,
                    seller_agent,
                    prop_data,
                    agreed_price,
                    market,
                    config=self.config,
                    # Settlement affordability is finalized by the replayed
                    # gate above; execution should not apply a second,
                    # differently-scoped affordability rule to the same order.
                    skip_affordability_check=True,
                )

            if not tx_record:
                tx_error_code = str(getattr(buyer, "_last_tx_error_code", "") or "")
                mapped_reason, mapped_class = self._map_execute_failure_reason(tx_error_code)
                penalty = min(deposit, deposit * cfg["breach_penalty_ratio"])
                if penalty > 0:
                    buyer.cash = max(0.0, float(buyer.cash) - float(penalty))
                    self._sync_buyer_finance(cursor, buyer)
                self._append_order_log(
                    month,
                    "SETTLEMENT_EXECUTION_FAILED",
                    {
                        "order_id": int(order_id),
                        "buyer_id": int(buyer.id),
                        "property_id": int(pid),
                        "error_code": tx_error_code,
                        "failure_classification": mapped_class,
                    },
                )
                self._close_order(
                    cursor, month, order_id, buyer, "breached", mapped_reason,
                    release_amount=0.0, penalty_amount=penalty
                )
                continue

            annual_rate = float(self.config.mortgage.get('annual_interest_rate', 0.05))
            loan_term_years = int(self.config.mortgage.get('loan_term_years', 30))

            if self.mortgage_risk_service and seller_id is not None and seller_id > 0:
                self.mortgage_risk_service.close_seller_mortgage_by_property(seller_id, pid, month)
            if self.mortgage_risk_service and float(tx_record.get("loan_amount", 0.0) or 0.0) > 0:
                monthly_pay = float(
                    calculate_monthly_payment(
                        float(tx_record.get("loan_amount", 0.0)),
                        annual_rate,
                        max(1, loan_term_years),
                    )
                )
                self.mortgage_risk_service.register_new_mortgage(
                    agent_id=int(buyer.id),
                    property_id=int(pid),
                    loan_amount=float(tx_record.get("loan_amount", 0.0)),
                    annual_rate=annual_rate,
                    remaining_term_years=loan_term_years,
                    monthly_payment=monthly_pay,
                    month=int(month),
                )

            # Persist transaction and ownership
            try:
                cursor.execute(
                    """
                    INSERT INTO transactions
                    (month, order_id, buyer_id, seller_id, property_id, final_price, down_payment, loan_amount, buyer_transaction_cost, seller_transaction_cost, negotiation_rounds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        month,
                        order_id,
                        buyer.id,
                        seller_id,
                        pid,
                        tx_record["price"],
                        tx_record["down_payment"],
                        tx_record["loan_amount"],
                        float(tx_record.get("buyer_transaction_cost", 0.0)),
                        float(tx_record.get("seller_transaction_cost", 0.0)),
                        neg_rounds,
                    ),
                )
            except sqlite3.OperationalError:
                # Backward compatibility for old schema without transaction cost columns.
                cursor.execute(
                    """
                    INSERT INTO transactions
                    (month, order_id, buyer_id, seller_id, property_id, final_price, down_payment, loan_amount, negotiation_rounds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        month,
                        order_id,
                        buyer.id,
                        seller_id,
                        pid,
                        tx_record["price"],
                        tx_record["down_payment"],
                        tx_record["loan_amount"],
                        neg_rounds,
                    ),
                )
            cursor.execute(
                """
                UPDATE properties_market
                SET status='off_market', owner_id=?, last_transaction_month=?, current_valuation=?
                WHERE property_id=?
                """,
                (buyer.id, month, tx_record["price"], pid),
            )
            self._sync_buyer_finance(cursor, buyer)
            self._close_order(
                cursor, month, order_id, buyer, "filled", "Settlement completed", release_amount=0.0, penalty_amount=0.0
            )
            # Strong consistency: once one order is filled for this property, clean stale
            # pending/pending_settlement siblings immediately to prevent next-month unavailable tails.
            cursor.execute(
                """
                SELECT order_id, buyer_id, deposit_amount
                FROM transaction_orders
                WHERE property_id=? AND order_id<>? AND status IN ('pending', 'pending_settlement')
                """,
                (int(pid), int(order_id)),
            )
            sibling_rows = cursor.fetchall() or []
            for sibling_order_id, sibling_buyer_id, sibling_deposit in sibling_rows:
                sibling_buyer = agent_map.get(int(sibling_buyer_id)) if sibling_buyer_id is not None else None
                if sibling_buyer is None:
                    cursor.execute(
                        """
                        UPDATE transaction_orders
                        SET status='cancelled',
                            close_month=?,
                            close_reason=?,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE order_id=?
                        """,
                        (month, "Order cleaned: property already transferred", int(sibling_order_id)),
                    )
                    continue
                self._append_order_log(
                    month,
                    "STALE_ORDER_CLEANED_AFTER_TRANSFER",
                    {
                        "order_id": int(sibling_order_id),
                        "buyer_id": int(sibling_buyer.id),
                        "property_id": int(pid),
                        "winner_order_id": int(order_id),
                        "reason": "property_already_transferred",
                    },
                )
                self._close_order(
                    cursor,
                    month,
                    int(sibling_order_id),
                    sibling_buyer,
                    "cancelled",
                    "Order cleaned: property already transferred",
                    release_amount=float(sibling_deposit or 0.0),
                    penalty_amount=0.0,
                )
            setattr(buyer, "_must_continue_search", False)
            setattr(buyer, "_search_exhausted", False)
            setattr(buyer, "_buy_task_locked", False)
            setattr(buyer, "_buy_task_lock_reason", "")
            self._append_order_log(
                month,
                "SETTLEMENT_COMPLETED",
                {
                    "order_id": order_id,
                    "buyer_id": buyer.id,
                    "seller_id": seller_id,
                    "property_id": pid,
                    "final_price": round(float(tx_record["price"]), 2),
                    "down_payment": round(float(tx_record["down_payment"]), 2),
                    "loan_amount": round(float(tx_record["loan_amount"]), 2),
                    "buyer_transaction_cost": round(float(tx_record.get("buyer_transaction_cost", 0.0)), 2),
                    "seller_transaction_cost": round(float(tx_record.get("seller_transaction_cost", 0.0)), 2),
                },
            )
            # M13: keep replacement-chain continuity for BUYER_SELLER.
            cursor.execute("SELECT role, chain_mode FROM active_participants WHERE agent_id = ?", (buyer.id,))
            buyer_active = cursor.fetchone()
            if buyer_active and str(buyer_active[0]).upper() == "BUYER_SELLER":
                chain_mode = (buyer_active[1] or "").lower()
                cursor.execute(
                    """
                    UPDATE active_participants
                    SET role='SELLER', buy_completed=1, role_duration=0
                    WHERE agent_id=?
                    """,
                    (buyer.id,),
                )
                buyer.role = "SELLER"
                self._append_order_log(
                    month,
                    "CHAIN_PROGRESS",
                    {
                        "agent_id": buyer.id,
                        "from_role": "BUYER_SELLER",
                        "to_role": "SELLER",
                        "chain_mode": chain_mode,
                        "reason": "buy objective completed, keep sell objective active",
                    },
                )
            else:
                buyer.role = "OBSERVER"
                cursor.execute("DELETE FROM active_participants WHERE agent_id = ?", (buyer.id,))

            # Seller side chain progression (sell-first -> buyer leg)
            if seller_id is not None and seller_id >= 0:
                cursor.execute("SELECT role, chain_mode FROM active_participants WHERE agent_id = ?", (seller_id,))
                seller_active = cursor.fetchone()
                if seller_active and str(seller_active[0]).upper() == "BUYER_SELLER":
                    chain_mode = (seller_active[1] or "").lower()
                    cursor.execute(
                        """
                        UPDATE active_participants
                        SET role='BUYER', sell_completed=1, role_duration=0
                        WHERE agent_id=?
                        """,
                        (seller_id,),
                    )
                    self._append_order_log(
                        month,
                        "CHAIN_PROGRESS",
                        {
                            "agent_id": seller_id,
                            "from_role": "BUYER_SELLER",
                            "to_role": "BUYER",
                            "chain_mode": chain_mode,
                            "reason": "sell objective completed, keep buy objective active",
                        },
                    )
            settled_count += 1

        return settled_count


    async def process_listing_price_adjustments(self, month: int, market_trend: str):
        """Tier 3: LLM Autonomous Price Adjustment."""
        cursor = self.conn.cursor()

        # Select stale listings using V2 tables
        try:
            cursor.execute("""
                SELECT pm.property_id, pm.owner_id, pm.listed_price, pm.min_price, pm.listing_month,
                       ast.name, ast.investment_style,
                       ast.purchase_motive_primary, ast.housing_stage, ast.family_stage,
                       ast.education_path, ast.financial_profile, ast.seller_profile,
                       ps.building_area, ps.zone, ps.is_school_district, ps.price_per_sqm,
                       COALESCE(ast.agent_type, 'normal') AS agent_type,
                       pm.sell_deadline_month, pm.sell_deadline_total_months, pm.forced_sale_mode,
                       COALESCE(ast.info_delay_months, 0) AS info_delay_months
                FROM properties_market pm
                JOIN agents_static ast ON pm.owner_id = ast.agent_id
                LEFT JOIN properties_static ps ON pm.property_id = ps.property_id
                WHERE pm.status='for_sale' AND pm.listing_month <= ? AND pm.owner_id != -1
            """, (month - 2,))
        except sqlite3.OperationalError:
            cursor.execute("""
                SELECT pm.property_id, pm.owner_id, pm.listed_price, pm.min_price, pm.listing_month,
                       ast.name, ast.investment_style,
                       '' AS purchase_motive_primary, '' AS housing_stage, '' AS family_stage,
                       '' AS education_path, '' AS financial_profile, '' AS seller_profile,
                       0 AS building_area, '' AS zone, 0 AS is_school_district, 0 AS price_per_sqm,
                       'normal' AS agent_type,
                       NULL AS sell_deadline_month, NULL AS sell_deadline_total_months, 0 AS forced_sale_mode,
                       0 AS info_delay_months
                FROM properties_market pm
                JOIN agents_static ast ON pm.owner_id = ast.agent_id
                WHERE pm.status='for_sale' AND pm.listing_month <= ? AND pm.owner_id != -1
            """, (month - 2,))

        stale_listings = cursor.fetchall()

        if not stale_listings:
            return

        cfg = self._price_adjust_cfg()
        tasks = []
        task_meta = []
        llm_candidates = []
        regime_raise_release_enabled = bool(
            cfg.get("regime_engine_v1_enabled", False)
            and cfg.get("regime_v1_price_reconsider_enabled", True)
        )

        def _heat_rank(band: str) -> int:
            b = str(band or "").upper()
            if b == "HIGH":
                return 2
            if b in {"MEDIUM", "MED"}:
                return 1
            return 0

        def _cold_rank(cold_signal: Dict[str, object]) -> int:
            return 1 if bool((cold_signal or {}).get("is_cold", False)) else 0

        # Build listing contexts first so routing can do "filter -> rank -> select".
        listing_contexts: List[Dict[str, object]] = []

        def _apply_lag_heat_compensation(demand_heat: Dict[str, object]) -> Dict[str, object]:
            adjusted = dict(demand_heat or {})
            info_delay_enabled = bool(self.config.get("smart_agent.info_delay_enabled", False))
            if not (info_delay_enabled and bool(cfg.get("lag_heat_compensation_enabled", False))):
                return adjusted
            try:
                delay_ratio = float(self.config.get("smart_agent.info_delay_ratio", 0.0) or 0.0)
            except Exception:
                delay_ratio = 0.0
            score = float(adjusted.get("score", 0.0) or 0.0)
            medium_th = float(adjusted.get("medium_threshold", cfg.get("heat_medium_threshold", 0.16)) or 0.16)
            high_th = float(adjusted.get("high_threshold", cfg.get("heat_high_threshold", 0.36)) or 0.36)
            med_delta = float(cfg.get("lag_heat_compensation_medium_delta_base", 0.03)) + delay_ratio * float(
                cfg.get("lag_heat_compensation_medium_delta_scale", 0.04)
            )
            high_delta = float(cfg.get("lag_heat_compensation_high_delta_base", 0.02)) + delay_ratio * float(
                cfg.get("lag_heat_compensation_high_delta_scale", 0.03)
            )
            medium_eff = max(0.02, medium_th - med_delta)
            high_eff = max(medium_eff + 0.02, high_th - high_delta)
            if score >= high_eff:
                band = "HIGH"
            elif score >= medium_eff:
                band = "MEDIUM"
            else:
                band = "LOW"
            adjusted["lag_compensated"] = True
            adjusted["medium_threshold_effective"] = round(float(medium_eff), 4)
            adjusted["high_threshold_effective"] = round(float(high_eff), 4)
            adjusted["band_before_lag_compensation"] = str(demand_heat.get("band", "LOW")).upper()
            adjusted["band"] = band
            adjusted["medium_threshold"] = round(float(medium_eff), 4)
            adjusted["high_threshold"] = round(float(high_eff), 4)
            return adjusted

        for row in stale_listings:
            pid, seller_id, current_price, _, created_m = row[0], row[1], row[2], row[3], row[4]
            agent_name, investment_style = row[5], row[6]
            agent_type = str(row[17] if len(row) > 17 and row[17] is not None else "normal").lower()
            deadline_month = int(row[18]) if len(row) > 18 and row[18] is not None else None
            deadline_total = int(row[19]) if len(row) > 19 and row[19] is not None else None
            forced_sale_mode = int(row[20]) if len(row) > 20 and row[20] is not None else 0
            info_delay_months = int(row[21]) if len(row) > 21 and row[21] is not None else 0
            listing_duration = month - created_m
            deadline_months_left = None
            if deadline_month is not None:
                deadline_months_left = int(deadline_month - int(month) + 1)
            observed_market = self._resolve_delayed_market_trend(
                cursor,
                month=int(month),
                info_delay_months=int(info_delay_months),
                default_trend=str(market_trend or "STABLE"),
            )
            demand_heat = self._collect_recent_listing_demand_heat(cursor, month, int(pid), cfg)
            demand_heat = _apply_lag_heat_compensation(demand_heat)
            cold_signal = {}
            if bool(cfg.get("cold_house_enabled", True)):
                cold_signal = self._collect_cold_listing_signal(cursor, month, int(pid), cfg)
            demand_context = dict(demand_heat or {})
            demand_context["cold_listing_signal"] = cold_signal or {}
            listing_contexts.append(
                {
                    "pid": int(pid),
                    "seller_id": int(seller_id),
                    "row": row,
                    "agent_name": agent_name,
                    "investment_style": investment_style,
                    "agent_type": agent_type,
                    "current_price": float(current_price or 0.0),
                    "listing_duration": int(listing_duration),
                    "deadline_month": deadline_month,
                    "deadline_total": deadline_total,
                    "deadline_months_left": deadline_months_left,
                    "forced_sale_mode": forced_sale_mode,
                    "info_delay_months": int(info_delay_months),
                    "observed_market_trend": str(observed_market.get("observed_trend", market_trend)),
                    "observed_market_month": int(observed_market.get("observed_month", month)),
                    "demand_heat": demand_heat,
                    "demand_context": demand_context,
                    "cold_signal": cold_signal,
                    "is_cold_listing": bool((cold_signal or {}).get("is_cold", False)),
                }
            )

        # Build force-sample routing set:
        # default ratio mode; top-k mode (filter->rank->select) is supported but disabled by default.
        force_sample_key_set: set[tuple[int, int]] = set()
        preforce_eligible: List[Dict[str, object]] = []
        if regime_raise_release_enabled and bool(cfg.get("regime_v1_raise_release_force_sample_enabled", False)):
            min_valid_filter = int(cfg.get("regime_v1_raise_release_min_valid_bids_filter", 1))
            min_outbid_filter = int(cfg.get("regime_v1_raise_release_min_outbid_filter", 1))
            min_neg_filter = int(cfg.get("regime_v1_raise_release_min_negotiation_filter", 1))
            exclude_cold_zero = bool(cfg.get("regime_v1_raise_release_exclude_cold_zero_bid", True))
            require_competition_evidence = bool(cfg.get("regime_v1_raise_release_require_competition_evidence", True))
            zero_valid_bid_streak_block_months = int(
                cfg.get("regime_v1_raise_release_zero_valid_bid_streak_block_months", 2)
            )
            for ctx in listing_contexts:
                demand_heat = dict(ctx.get("demand_heat", {}) or {})
                valid_bids = int(demand_heat.get("valid_bids", 0) or 0)
                outbid_losses = int(demand_heat.get("outbid_losses", 0) or 0)
                negotiations = int(demand_heat.get("negotiation_entries", 0) or 0)
                zero_valid_bid_streak = int(demand_heat.get("trailing_zero_valid_bid_streak", 0) or 0)
                deadline_left = ctx.get("deadline_months_left")
                is_cold = bool(ctx.get("is_cold_listing", False))
                if is_cold:
                    continue
                if deadline_left is not None and int(deadline_left) <= 1:
                    continue
                if zero_valid_bid_streak_block_months > 0 and zero_valid_bid_streak >= zero_valid_bid_streak_block_months:
                    continue
                # "exclude cold zero-bid" means exclude strictly cold+zero cases,
                # not every valid_bids==0 record.
                if exclude_cold_zero and is_cold and valid_bids <= 0:
                    continue
                if require_competition_evidence and not (valid_bids >= 1 or outbid_losses >= 1):
                    continue
                if not (
                    valid_bids >= min_valid_filter
                    or outbid_losses >= min_outbid_filter
                    or negotiations >= min_neg_filter
                ):
                    continue
                heat_score = float(demand_heat.get("score", 0.0) or 0.0)
                preforce_eligible.append(
                    {
                        "ctx": ctx,
                        "rank_tuple": (
                            heat_score,
                            float(valid_bids),
                            float(outbid_losses),
                            float(negotiations),
                            -float(ctx.get("listing_duration", 0) or 0),
                            -float(ctx.get("pid", 0) or 0),
                        ),
                    }
                )
            preforce_eligible.sort(key=lambda x: x["rank_tuple"], reverse=True)

            routing_mode = str(cfg.get("regime_v1_raise_release_routing_mode", "ratio")).lower()
            use_topk_mode = bool(cfg.get("regime_v1_raise_release_topk_enabled", False)) and routing_mode == "topk"
            if use_topk_mode:
                topk = int(cfg.get("regime_v1_raise_release_topk", 0))
                if topk <= 0:
                    topk = max(0, len(preforce_eligible))
                chosen = preforce_eligible[: max(0, topk)]
                for item in chosen:
                    ctx = dict(item.get("ctx", {}) or {})
                    force_sample_key_set.add((int(ctx.get("seller_id", 0)), int(ctx.get("pid", 0))))
            else:
                sample_ratio = float(cfg.get("regime_v1_raise_release_force_sample_ratio", 0.25))
                for item in preforce_eligible:
                    ctx = dict(item.get("ctx", {}) or {})
                    seller_id = int(ctx.get("seller_id", 0))
                    pid = int(ctx.get("pid", 0))
                    sample_hit = False
                    if sample_ratio >= 1.0:
                        sample_hit = True
                    elif sample_ratio > 0.0:
                        sample_key = (
                            (int(month) * 1000003)
                            ^ (int(seller_id) * 9176)
                            ^ (int(pid) * 131)
                        ) & 0xFFFFFFFF
                        sample_hit = (sample_key % 10000) < int(sample_ratio * 10000)
                    if sample_hit:
                        force_sample_key_set.add((seller_id, pid))

            min_hits = int(cfg.get("regime_v1_raise_release_force_sample_min_hits_per_month", 0))
            if min_hits > 0 and len(force_sample_key_set) < min_hits:
                for item in preforce_eligible:
                    ctx = dict(item.get("ctx", {}) or {})
                    key = (int(ctx.get("seller_id", 0)), int(ctx.get("pid", 0)))
                    if key in force_sample_key_set:
                        continue
                    force_sample_key_set.add(key)
                    if len(force_sample_key_set) >= min_hits:
                        break
            # Fallback expansion: when strict filter is too narrow, widen to
            # non-cold and non-deadline listings, then pick hottest first.
            if min_hits > 0 and len(force_sample_key_set) < min_hits:
                relaxed: List[Dict[str, object]] = []
                for ctx in listing_contexts:
                    demand_heat = dict(ctx.get("demand_heat", {}) or {})
                    deadline_left = ctx.get("deadline_months_left")
                    is_cold = bool(ctx.get("is_cold_listing", False))
                    valid_bids = int(demand_heat.get("valid_bids", 0) or 0)
                    outbid_losses = int(demand_heat.get("outbid_losses", 0) or 0)
                    zero_valid_bid_streak = int(demand_heat.get("trailing_zero_valid_bid_streak", 0) or 0)
                    if is_cold:
                        continue
                    if deadline_left is not None and int(deadline_left) <= 1:
                        continue
                    if zero_valid_bid_streak_block_months > 0 and zero_valid_bid_streak >= zero_valid_bid_streak_block_months:
                        continue
                    if exclude_cold_zero and is_cold and valid_bids <= 0:
                        continue
                    if require_competition_evidence and not (valid_bids >= 1 or outbid_losses >= 1):
                        continue
                    relaxed.append(
                        {
                            "ctx": ctx,
                            "rank_tuple": (
                                float(demand_heat.get("score", 0.0) or 0.0),
                                float(valid_bids),
                                float(demand_heat.get("outbid_losses", 0) or 0),
                                float(demand_heat.get("negotiation_entries", 0) or 0),
                                -float(ctx.get("listing_duration", 0) or 0),
                                -float(ctx.get("pid", 0) or 0),
                            ),
                        }
                    )
                relaxed.sort(key=lambda x: x["rank_tuple"], reverse=True)
                for item in relaxed:
                    ctx = dict(item.get("ctx", {}) or {})
                    key = (int(ctx.get("seller_id", 0)), int(ctx.get("pid", 0)))
                    if key in force_sample_key_set:
                        continue
                    force_sample_key_set.add(key)
                    if len(force_sample_key_set) >= min_hits:
                        break

        # LLM minimum-calls uplift set:
        # if natural allow_llm candidates are too few, pre-select extra rows
        # by heat ranking and let them enter LLM path.
        llm_uplift_key_set: set[tuple[int, int]] = set()
        llm_min_calls_cfg = int(cfg.get("price_adjustment_llm_min_calls_per_month", 0))
        if llm_min_calls_cfg > 0:
            llm_uplift_pool: List[Dict[str, object]] = []
            for ctx in listing_contexts:
                deadline_left = ctx.get("deadline_months_left")
                if deadline_left is not None and int(deadline_left) <= 1:
                    continue
                demand_heat = dict(ctx.get("demand_heat", {}) or {})
                is_cold = bool(ctx.get("is_cold_listing", False))
                llm_uplift_pool.append(
                    {
                        "ctx": ctx,
                        "rank_tuple": (
                            int(0 if is_cold else 1),
                            float(demand_heat.get("score", 0.0) or 0.0),
                            float(demand_heat.get("valid_bids", 0) or 0),
                            float(demand_heat.get("outbid_losses", 0) or 0),
                            float(demand_heat.get("negotiation_entries", 0) or 0),
                            -float(ctx.get("listing_duration", 0) or 0),
                            -float(ctx.get("pid", 0) or 0),
                        ),
                    }
                )
            llm_uplift_pool.sort(key=lambda x: x["rank_tuple"], reverse=True)
            for item in llm_uplift_pool[: max(0, llm_min_calls_cfg)]:
                ctx = dict(item.get("ctx", {}) or {})
                llm_uplift_key_set.add((int(ctx.get("seller_id", 0)), int(ctx.get("pid", 0))))

        for ctx in listing_contexts:
            pid = int(ctx["pid"])
            seller_id = int(ctx["seller_id"])
            row = ctx["row"]
            current_price = float(ctx["current_price"])
            listing_duration = int(ctx["listing_duration"])
            agent_name = str(ctx["agent_name"])
            investment_style = str(ctx["investment_style"])
            agent_type = str(ctx["agent_type"])
            deadline_month = ctx["deadline_month"]
            deadline_total = ctx["deadline_total"]
            forced_sale_mode = int(ctx["forced_sale_mode"])
            deadline_months_left = ctx["deadline_months_left"]
            demand_heat = dict(ctx["demand_heat"])
            demand_context = dict(ctx["demand_context"])
            cold_signal = dict(ctx["cold_signal"])
            is_cold_listing = bool(ctx["is_cold_listing"])
            heat_band = str(demand_heat.get("band", "LOW")).upper()
            heat_score = float(demand_heat.get("score", 0.0) or 0.0)
            zero_valid_bid_streak = int(demand_heat.get("trailing_zero_valid_bid_streak", 0) or 0)
            zero_valid_bid_streak_block_months = int(
                cfg.get("regime_v1_raise_release_zero_valid_bid_streak_block_months", 2)
            )
            has_competition_evidence = bool(
                int(demand_heat.get("valid_bids", 0) or 0) >= 1
                or int(demand_heat.get("outbid_losses", 0) or 0) >= 1
            )

            allow_llm = bool(cfg.get("smart_llm_enabled", True))
            if bool(cfg.get("normal_rule_enabled", True)) and agent_type != "smart":
                allow_llm = (
                    str(demand_heat.get("band", "LOW")).upper() in {"MEDIUM", "HIGH"}
                    or is_cold_listing
                )

            base_min_duration = int(cfg.get("llm_min_duration", 5))
            if regime_raise_release_enabled and bool(cfg.get("regime_v1_raise_force_all_early_llm", False)):
                base_min_duration = 1
            if heat_band == "HIGH":
                llm_min_duration = max(1, base_min_duration - 2)
            elif heat_band in {"MEDIUM", "MED"}:
                llm_min_duration = max(1, base_min_duration - 1)
            else:
                llm_min_duration = max(1, base_min_duration)

            if is_cold_listing:
                llm_min_duration = min(
                    llm_min_duration,
                    max(1, int(cfg.get("cold_house_min_duration_for_llm", 2))),
                )

            hot_raise_release = bool(
                regime_raise_release_enabled
                and heat_band in {"HIGH", "MEDIUM", "MED"}
                and has_competition_evidence
                and (
                    zero_valid_bid_streak_block_months <= 0
                    or zero_valid_bid_streak < zero_valid_bid_streak_block_months
                )
                and (deadline_months_left is None or int(deadline_months_left) > 1)
            )
            force_sample_raise_only = (seller_id, pid) in force_sample_key_set
            force_raise_only = bool(
                hot_raise_release
                and bool(cfg.get("regime_v1_raise_release_preforce_enabled", True))
            )
            if force_sample_raise_only:
                force_raise_only = True
                hot_raise_release = True
            if force_raise_only:
                allow_llm = True
            if (seller_id, pid) in llm_uplift_key_set:
                allow_llm = True
                llm_min_duration = 1
            if hot_raise_release:
                llm_min_duration = min(
                    llm_min_duration,
                    max(1, int(cfg.get("regime_v1_raise_release_force_llm_min_duration", 1))),
                )

            if listing_duration < llm_min_duration and not hot_raise_release:
                allow_llm = False

            if allow_llm:
                if force_raise_only:
                    llm_min_duration = 1
                llm_candidates.append(
                    {
                        "pid": pid,
                        "seller_id": seller_id,
                        "row": row,
                        "listing_duration": listing_duration,
                        "agent_name": agent_name,
                        "investment_style": investment_style,
                        "agent_type": agent_type,
                        "current_price": float(current_price or 0.0),
                        "info_delay_months": int(ctx.get("info_delay_months", 0) or 0),
                        "observed_market_trend": str(ctx.get("observed_market_trend", market_trend) or market_trend),
                        "demand_heat": demand_heat,
                        "demand_context": demand_context,
                        "cold_signal": cold_signal,
                        "heat_band": heat_band,
                        "heat_score": heat_score,
                        "heat_rank": _heat_rank(heat_band),
                        "cold_rank": _cold_rank(cold_signal),
                        "hot_raise_release": bool(hot_raise_release),
                        "force_raise_only": bool(force_raise_only),
                        "force_sample_raise_only": bool(force_sample_raise_only),
                        "deadline_month": deadline_month,
                        "deadline_total": deadline_total,
                        "deadline_months_left": deadline_months_left,
                        "forced_sale_mode": forced_sale_mode,
                        "model_type": (
                            str(cfg.get("price_adjustment_high_heat_model_type", "smart"))
                            if bool(force_raise_only) or _heat_rank(heat_band) >= 2
                            else str(cfg.get("price_adjustment_model_type", "fast"))
                        ),
                    }
                )
            else:
                rule_result, rule_metrics = self._rule_price_adjustment_decision(
                    current_price=float(current_price or 0.0),
                    listing_duration=int(listing_duration),
                    market_trend=str(market_trend or "STABLE"),
                    observed_market_trend=str(ctx.get("observed_market_trend", market_trend) or market_trend),
                    recent_demand_context=demand_context,
                    decision_profile="smart" if agent_type == "smart" else "normal",
                    deadline_months_left=deadline_months_left,
                    cfg=cfg,
                )
                if (
                    regime_raise_release_enabled
                    and bool(cfg.get("regime_v1_raise_force_rule_path_enabled", False))
                    and str(heat_band).upper() in {"MEDIUM", "MED", "HIGH"}
                    and not bool(is_cold_listing)
                    and has_competition_evidence
                    and (
                        zero_valid_bid_streak_block_months <= 0
                        or zero_valid_bid_streak < zero_valid_bid_streak_block_months
                    )
                    and (deadline_months_left is None or int(deadline_months_left) > 2)
                ):
                    coeff = float(cfg.get("regime_v1_raise_force_rule_coeff", 1.05))
                    rule_result["action"] = "E" if coeff <= 1.05 else "F"
                    rule_result["coefficient"] = float(coeff)
                    rule_result["new_price"] = float(current_price or 0.0) * float(coeff)
                    rule_result["reason"] = (
                        f"{rule_result.get('reason', '')} | RegimeV1激进试验：热盘规则路径强制上调。"
                    ).strip()
                    rule_metrics["pricing_mode"] = "rule_raise_force"
                rule_metrics["recent_demand_context"] = demand_context
                rule_metrics["decision_profile"] = "smart" if agent_type == "smart" else "normal"
                rule_metrics["info_delay_months"] = int(ctx.get("info_delay_months", 0) or 0)
                rule_metrics["observed_market_trend"] = str(ctx.get("observed_market_trend", market_trend) or market_trend)
                tasks.append((pid, None, seller_id, row))
                task_meta.append(
                    {
                        "result": (rule_result, rule_metrics),
                        "llm_called": False,
                    }
                )

        # Apply monthly LLM cap:
        # prioritize hot-demand listings first, then stale duration.
        llm_cap = int(cfg.get("monthly_llm_cap", 20))
        llm_min_calls = int(cfg.get("price_adjustment_llm_min_calls_per_month", 0))
        effective_llm_cap = max(int(llm_cap), int(llm_min_calls))
        def _llm_rank_key(item: Dict[str, object]):
            return (
                int(1 if bool(item.get("force_raise_only", False)) else 0),
                int(1 if bool(item.get("hot_raise_release", False)) else 0),
                int(item.get("cold_rank", 0)),
                int(item.get("heat_rank", 0)),
                float(item.get("heat_score", 0.0)),
                int(item.get("listing_duration", 0)),
            )

        llm_candidates.sort(key=_llm_rank_key, reverse=True)
        if effective_llm_cap >= 0:
            reserved_hot = max(0, int(cfg.get("regime_v1_raise_release_cap_reserved", 3)))
            reserved_hot = min(effective_llm_cap, reserved_hot)
            hot_rows = [x for x in llm_candidates if bool(x.get("hot_raise_release", False))]
            llm_enabled_rows = []
            if reserved_hot > 0 and hot_rows:
                llm_enabled_rows.extend(hot_rows[:reserved_hot])
            remaining_cap = max(0, effective_llm_cap - len(llm_enabled_rows))
            if remaining_cap > 0:
                seen = {id(x) for x in llm_enabled_rows}
                for cand in llm_candidates:
                    if id(cand) in seen:
                        continue
                    llm_enabled_rows.append(cand)
                    if len(llm_enabled_rows) >= effective_llm_cap:
                        break
            enabled_ids = {id(x) for x in llm_enabled_rows}
            llm_downgraded_rows = [x for x in llm_candidates if id(x) not in enabled_ids]
        else:
            llm_enabled_rows = llm_candidates
            llm_downgraded_rows = []

        for item in llm_downgraded_rows:
            row = item["row"]
            current_price = row[2]
            listing_duration = item["listing_duration"]
            rule_result, rule_metrics = self._rule_price_adjustment_decision(
                current_price=float(current_price or 0.0),
                listing_duration=int(listing_duration),
                market_trend=str(market_trend or "STABLE"),
                observed_market_trend=str(item.get("observed_market_trend", market_trend) or market_trend),
                recent_demand_context=item.get("demand_context", {}),
                decision_profile="smart" if str(item.get("agent_type", "normal")) == "smart" else "normal",
                deadline_months_left=item.get("deadline_months_left"),
                cfg=cfg,
            )
            if (
                regime_raise_release_enabled
                and bool(cfg.get("regime_v1_raise_force_rule_path_enabled", False))
                and str(item.get("heat_band", "")).upper() in {"MEDIUM", "MED", "HIGH"}
                and not bool((item.get("cold_signal", {}) or {}).get("is_cold", False))
                and (
                    int((item.get("demand_heat", {}) or {}).get("valid_bids", 0) or 0) >= 1
                    or int((item.get("demand_heat", {}) or {}).get("outbid_losses", 0) or 0) >= 1
                )
                and (
                    int(cfg.get("regime_v1_raise_release_zero_valid_bid_streak_block_months", 2)) <= 0
                    or int((item.get("demand_heat", {}) or {}).get("trailing_zero_valid_bid_streak", 0) or 0)
                    < int(cfg.get("regime_v1_raise_release_zero_valid_bid_streak_block_months", 2))
                )
                and (item.get("deadline_months_left") is None or int(item.get("deadline_months_left")) > 2)
            ):
                coeff = float(cfg.get("regime_v1_raise_force_rule_coeff", 1.05))
                rule_result["action"] = "E" if coeff <= 1.05 else "F"
                rule_result["coefficient"] = float(coeff)
                rule_result["new_price"] = float(current_price or 0.0) * float(coeff)
                rule_result["reason"] = (
                    f"{rule_result.get('reason', '')} | RegimeV1激进试验：热盘规则降级路径强制上调。"
                ).strip()
                rule_metrics["pricing_mode"] = "rule_cap_downgrade_raise_force"
            else:
                rule_metrics["pricing_mode"] = "rule_cap_downgrade"
            rule_metrics["recent_demand_context"] = item.get("demand_context", {})
            rule_metrics["decision_profile"] = "smart" if str(item.get("agent_type", "normal")) == "smart" else "normal"
            rule_metrics["info_delay_months"] = int(item.get("info_delay_months", 0) or 0)
            rule_metrics["observed_market_trend"] = str(item.get("observed_market_trend", market_trend) or market_trend)
            tasks.append((item["pid"], None, item["seller_id"], row))
            task_meta.append(
                {
                    "result": (rule_result, rule_metrics),
                    "llm_called": False,
                }
            )

        llm_tasks = []
        for item in llm_enabled_rows:
            tasks.append((item["pid"], None, item["seller_id"], item["row"]))
            llm_tasks.append(
                decide_price_adjustment(
                    agent_id=item["seller_id"],
                    agent_name=item["agent_name"],
                    investment_style=item["investment_style"],
                    property_id=item["pid"],
                    current_price=item["current_price"],
                    listing_duration=int(item["listing_duration"]),
                    market_trend=market_trend,
                    db_conn=self.conn,
                    recent_demand_context=item.get("demand_context", {}),
                    sell_deadline_month=item.get("deadline_month"),
                    sell_deadline_total_months=item.get("deadline_total"),
                    current_month=int(month),
                    model_type=str(item.get("model_type", "fast")),
                    force_raise_only=bool(item.get("force_raise_only", False)),
                    decision_profile="smart" if str(item.get("agent_type", "normal")) == "smart" else "normal",
                    info_delay_months=int(item.get("info_delay_months", 0) or 0),
                    observed_market_trend=str(item.get("observed_market_trend", market_trend) or market_trend),
                )
            )
            task_meta.append({"result": None, "llm_called": True})

        llm_results = []
        if llm_tasks:
            llm_results = await asyncio.gather(*llm_tasks)

        # Rebind gathered LLM results back to task_meta in insertion order.
        llm_idx = 0
        for meta in task_meta:
            if meta["llm_called"]:
                meta["result"] = llm_results[llm_idx]
                llm_idx += 1

        batch_decision_logs = []

        for (pid, _, seller_id, row), meta in zip(tasks, task_meta):
            # unpack result which is now (decision_dict, context_metrics)
            result_tuple = meta["result"]
            result = result_tuple[0]
            metrics = result_tuple[1]
            llm_called = bool(meta.get("llm_called", False))

            action = str(result.get("action", "A") or "A").strip().upper()
            action_alias = {
                "MAINTAIN": "A",
                "HOLD": "A",
                "KEEP": "A",
                "CUT_SMALL": "B",
                "SMALL_CUT": "B",
                "REDUCE_SMALL": "B",
                "CUT_DEEP": "C",
                "DEEP_CUT": "C",
                "REDUCE_BIG": "C",
                "RAISE_SMALL": "E",
                "SMALL_RAISE": "E",
                "RAISE_BIG": "F",
                "BIG_RAISE": "F",
                "HOLD_OFF": "D",
            }
            action = action_alias.get(action, action)
            if action not in {"A", "B", "C", "D", "E", "F"}:
                action = "A"
            deadline_month = int(row[18]) if len(row) > 18 and row[18] is not None else None
            deadline_total = int(row[19]) if len(row) > 19 and row[19] is not None else None
            forced_sale_mode = int(row[20]) if len(row) > 20 and row[20] is not None else 0
            deadline_months_left = None
            if deadline_month is not None:
                deadline_months_left = int(deadline_month - int(month) + 1)

            # Deadline guardrail:
            # if hard-clear mode is enabled and we are at deadline month, disallow hold/raise actions.
            # Keep this as code-side execution rule; decision ownership remains with LLM on normal months.
            if forced_sale_mode and deadline_months_left is not None:
                hard_cut = max(0.50, min(0.95, float(cfg.get("deadline_hard_cut", 0.75))))
                soft_cut = max(0.60, min(0.98, float(cfg.get("deadline_soft_cut", 0.90))))
                if deadline_months_left <= 1 and action in {"A", "D", "E", "F"}:
                    action = "C"
                    result["coefficient"] = float(hard_cut)
                    result["reason"] = (
                        f"{result.get('reason', '')} | 到达售出期限最后一个月，系统执行强降价清仓规则。"
                    ).strip(" |")
                elif deadline_months_left <= 2 and action in {"A", "D"}:
                    action = "B"
                    result["coefficient"] = float(soft_cut)
                    result["reason"] = (
                        f"{result.get('reason', '')} | 售出期限临近，系统将维持/撤牌改为降价出清。"
                    ).strip(" |")
            listing_duration = month - int(row[4] or month)
            demand_ctx_now = dict((metrics or {}).get("recent_demand_context") or {})
            valid_bids_now = int(demand_ctx_now.get("valid_bids", 0) or 0)
            negotiations_now = int(demand_ctx_now.get("negotiation_entries", 0) or 0)
            matches_now = int(demand_ctx_now.get("matches", 0) or 0)
            outbid_now = int(demand_ctx_now.get("outbid_losses", 0) or 0)
            zero_valid_bid_streak_now = int(demand_ctx_now.get("trailing_zero_valid_bid_streak", 0) or 0)
            zero_valid_bid_streak_block_months = int(
                cfg.get("regime_v1_raise_release_zero_valid_bid_streak_block_months", 2)
            )
            require_competition_evidence = bool(
                cfg.get("regime_v1_raise_release_require_competition_evidence", True)
            )
            severe_cold = (
                valid_bids_now == 0
                and negotiations_now == 0
                and matches_now <= 1
                and int(max(0, listing_duration)) >= 3
            )
            if action == "D" and severe_cold:
                # Keep decision ownership with LLM on normal cases;
                # only rewrite pathological cold/no-bid delist into deep markdown trial.
                action = "C"
                result["coefficient"] = min(
                    float(result.get("coefficient", 0.92) or 0.92),
                    max(0.60, float(cfg.get("rule_deep_cut", 0.92)) - 0.05),
                )
                result["reason"] = (
                    f"{result.get('reason', '')} | 连续冷盘且有效报价长期为0，系统将撤牌改为明显降价试成交。"
                ).strip(" |")

            # Regime hard guardrail:
            # under continuous zero valid bids or no competition evidence, disallow E/F.
            # This keeps "hot listings can raise, cold listings should not raise".
            if action in {"E", "F"}:
                zero_streak_blocked = (
                    zero_valid_bid_streak_block_months > 0
                    and zero_valid_bid_streak_now >= zero_valid_bid_streak_block_months
                )
                no_competition_evidence = require_competition_evidence and not (
                    valid_bids_now >= 1 or outbid_now >= 1
                )
                if zero_streak_blocked or no_competition_evidence:
                    action = "B"
                    result["coefficient"] = min(
                        float(result.get("coefficient", 0.96) or 0.96),
                        float(cfg.get("rule_mild_cut", 0.96)),
                    )
                    result["reason"] = (
                        f"{result.get('reason', '')} | 连续零有效报价或无竞争证据，系统禁止提价并改为小幅降价。"
                    ).strip(" |")

            scorecard = self._build_price_adjust_scorecard(
                current_price=float(row[2] or 0.0),
                demand_context=demand_ctx_now,
                deadline_months_left=deadline_months_left,
                listing_duration=listing_duration,
            )
            new_price = result.get("new_price", 0)  # default?
            reason = result.get("reason", "LLM决策")
            persona_snapshot = self._seller_persona_snapshot_from_row(row)
            property_snapshot = self._listing_property_snapshot_from_row(row)
            reason_tags = self._price_adjust_reason_tags(
                action=action,
                market_trend=market_trend,
                listing_duration=listing_duration,
                property_snapshot=property_snapshot,
                persona_snapshot=persona_snapshot,
                demand_heat_band=str((metrics.get("recent_demand_context") or {}).get("band", "")),
            )

            # Defensive check
            if not new_price and action in ["B", "C"]:
                continue  # Should not happen

            if action == "A":
                # Maintain price
                logger.debug(f"Property {pid}: 维持原价 - {reason}")
            elif action in ["B", "C"]:
                # Legacy path: B/C as rule-based markdown actions for compatibility.
                # Update price (V2)
                old_listed = max(1.0, float(row[2] or 0.0))
                old_min = float(row[3] if row[3] is not None else old_listed * 0.95)
                min_ratio = old_min / old_listed if old_listed > 0 else 0.95
                if min_ratio <= 0 or min_ratio > 1.0:
                    min_ratio = 0.95
                new_price_raw = result.get("new_price", None)
                if new_price_raw is not None and float(new_price_raw or 0.0) > 0:
                    new_listed = max(1.0, round(float(new_price_raw), 2))
                else:
                    coef = float(result.get("coefficient", 1.0) or 1.0)
                    if action == "B":
                        coef = max(0.90, min(1.00, coef))
                    elif action == "C":
                        coef = max(0.60, min(0.95, coef))
                    else:
                        coef = max(0.70, min(1.20, coef))
                    new_listed = max(1.0, round(float(old_listed * coef), 2))
                dynamic_floor_ratio = self._resolve_dynamic_floor_ratio(
                    old_listed=float(old_listed),
                    old_min=float(old_min),
                    action=action,
                    demand_context=demand_ctx_now,
                    deadline_months_left=deadline_months_left,
                    listing_duration=listing_duration,
                    cfg=cfg,
                )
                # Keep min_price coupled to listed_price and allow dynamic floor movement.
                floor_ratio = min(float(min_ratio), float(dynamic_floor_ratio))
                new_min = max(1.0, round(min(new_listed, new_listed * floor_ratio), 2))
                cursor.execute("""
                    UPDATE properties_market
                    SET listed_price = ?, min_price = ?, last_price_update_month = ?, last_price_update_reason = ?
                    WHERE property_id = ?
                """, (new_listed, new_min, month, reason, pid))
                logger.info(f"Property {pid}: 调价至 {new_listed:,.0f} - {reason}")
            elif action == "D":
                # Delist (V2)
                cursor.execute("""
                    UPDATE properties_market
                    SET status='off_market', last_price_update_month = ?, last_price_update_reason = ?
                    WHERE property_id = ?
                """, (month, reason, pid))
                logger.info(f"Property {pid}: 撤牌观望 - {reason}")
            elif action in {"E", "F"}:
                # Hold-and-raise (seller market reaction), still bounded by guardrails.
                old_listed = max(1.0, float(row[2] or 0.0))
                old_min = float(row[3] if row[3] is not None else old_listed * 0.95)
                min_ratio = old_min / old_listed if old_listed > 0 else 0.95
                if min_ratio <= 0 or min_ratio > 1.0:
                    min_ratio = 0.95
                coef = float(result.get("coefficient", 1.0) or 1.0)
                if action == "E":
                    coef = max(1.01, min(1.05, coef))
                else:
                    coef = max(1.05, min(1.12, coef))
                new_listed = max(1.0, round(float(old_listed * coef), 2))
                dynamic_floor_ratio = self._resolve_dynamic_floor_ratio(
                    old_listed=float(old_listed),
                    old_min=float(old_min),
                    action=action,
                    demand_context=demand_ctx_now,
                    deadline_months_left=deadline_months_left,
                    listing_duration=listing_duration,
                    cfg=cfg,
                )
                floor_ratio = min(float(min_ratio), float(dynamic_floor_ratio))
                new_min = max(1.0, round(min(new_listed, new_listed * floor_ratio), 2))
                cursor.execute("""
                    UPDATE properties_market
                    SET listed_price = ?, min_price = ?, last_price_update_month = ?, last_price_update_reason = ?
                    WHERE property_id = ?
                """, (new_listed, new_min, month, reason, pid))
                logger.info(f"Property {pid}: {'小幅提价' if action=='E' else '明显提价'}至 {new_listed:,.0f} - {reason}")

            # Log decision with context_metrics
            metrics_payload = dict(metrics or {})
            metrics_payload.update(
                {
                    "seller_persona_snapshot": persona_snapshot,
                    "property_snapshot": property_snapshot,
                    "selection_reason_tags": reason_tags,
                    "listing_duration": int(listing_duration),
                    "market_trend": str(market_trend or ""),
                    "new_listed_price": round(float(result.get("new_price", new_price) or property_snapshot.get("listed_price", 0.0)), 2),
                    "sell_deadline_month": int(deadline_month) if deadline_month is not None else None,
                    "sell_deadline_total_months": int(deadline_total) if deadline_total is not None else None,
                    "deadline_months_left": int(deadline_months_left) if deadline_months_left is not None else None,
                    "forced_sale_mode": int(forced_sale_mode),
                    "price_adjust_scorecard": scorecard,
                    "severe_cold_rewrite_to_c": bool(severe_cold and action == "C" and "撤牌改为明显降价试成交" in str(reason)),
                    "ef_competition_evidence": {
                        "recent_valid_bids": int((demand_ctx_now or {}).get("recent_valid_bids", 0) or 0),
                        "recent_outbid_losses": int((demand_ctx_now or {}).get("recent_outbid_losses", 0) or 0),
                        "recent_negotiations": int((demand_ctx_now or {}).get("recent_negotiations", 0) or 0),
                        "recent_matches": int((demand_ctx_now or {}).get("recent_matches", 0) or 0),
                        "zero_valid_bid_streak": int((demand_ctx_now or {}).get("zero_valid_bid_streak", 0) or 0),
                        "require_competition_evidence": bool(require_competition_evidence),
                        "passed_competition_evidence_gate": bool(
                            int((demand_ctx_now or {}).get("recent_valid_bids", 0) or 0) >= 1
                            or int((demand_ctx_now or {}).get("recent_outbid_losses", 0) or 0) >= 1
                        ),
                    },
                }
            )
            metrics_json = json.dumps(metrics_payload, ensure_ascii=False)

            batch_decision_logs.append((
                seller_id, month, "PRICE_ADJUSTMENT", action, reason, None,
                metrics_json, llm_called
            ))

        if batch_decision_logs:
            cursor.executemany("""INSERT INTO decision_logs
                (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", batch_decision_logs)
            self.conn.commit()

    async def process_monthly_transactions(self, month: int, buyers: List[Agent],
                                           listings_by_zone: Dict, active_listings: List[Dict],
                                           props_map: Dict, agent_map: Dict,
                                           market, wf_logger, exchange_display,
                                           reflow_pass: bool = False,
                                           reflow_depth: int = 0):
        """
        Orchestrate matching, negotiation, and execution.
        Returns: (transactions_count, failed_negotiations_count)
        """
        cursor = self.conn.cursor()
        self._ensure_recovery_tables(cursor)
        transactions_count = 0
        failed_negotiations = 0

        repaired = self._repair_listing_price_invariants(cursor, month)
        if repaired > 0:
            logger.warning(f"Month {month}: repaired {repaired} listing price invariant violations (min_price<=listed_price).")

        # M18/M19: expire stale pending orders before creating new ones.
        # Reflow pass runs within the same month, so skip re-expire to avoid duplicate churn.
        if not reflow_pass:
            self._expire_stale_orders(cursor, month, agent_map)

        # --- 1. Matching Phase (批量匹配重构) ---
        from transaction_engine import get_buyer_monthly_buy_cap, match_properties_for_buyer
        market_trend = "STABLE"
        try:
            cursor.execute("SELECT trend_signal FROM market_bulletin WHERE month = ?", (month,))
            row = cursor.fetchone()
            if row and row[0]:
                market_trend = str(row[0])
        except Exception:
            market_trend = "STABLE"

        # Optional force helper:
        # when enabled, supplement buyer pool if activation produced too few buyers.
        # This is safe for regular runs (does NOT bypass affordability precheck).
        try:
            force_multi_buyer_pool = bool(
                self.config.get(
                    "smart_agent.force_multi_buyer_pool_enabled",
                    self.config.get("force_multi_buyer_pool_enabled", False),
                )
            )
        except Exception:
            force_multi_buyer_pool = False
        try:
            gate_force_multi_buyer_pool = bool(
                self.config.get(
                    "smart_agent.gate_force_multi_buyer_pool_enabled",
                    self.config.get("gate_force_multi_buyer_pool_enabled", False),
                )
            )
        except Exception:
            gate_force_multi_buyer_pool = False
        enable_forced_pooling = bool(force_multi_buyer_pool or gate_force_multi_buyer_pool)
        if enable_forced_pooling and not reflow_pass:
            try:
                gate_min_buyers = int(
                    self.config.get(
                        "smart_agent.force_min_buyers",
                        self.config.get(
                            "force_min_buyers",
                            self.config.get(
                        "smart_agent.gate_force_min_buyers",
                        self.config.get("gate_force_min_buyers", 6),
                            ),
                        ),
                    )
                )
            except Exception:
                gate_min_buyers = 6
            gate_min_buyers = max(3, min(20, gate_min_buyers))
            if len(buyers) < gate_min_buyers:
                existing_ids = {int(getattr(b, "id", -1)) for b in buyers}
                supplement = []
                for ag in agent_map.values():
                    aid = int(getattr(ag, "id", -1))
                    if aid in existing_ids:
                        continue
                    if aid <= 0:
                        continue
                    # developer/invalid owner markers are negative; agents are positive ids
                    if float(getattr(ag, "cash", 0.0) or 0.0) <= 0:
                        continue
                    supplement.append(ag)
                supplement.sort(key=lambda x: float(getattr(x, "cash", 0.0) or 0.0), reverse=True)
                for ag in supplement:
                    if len(buyers) >= gate_min_buyers:
                        break
                    buyers.append(ag)
                self._append_order_log(
                    month,
                    "FORCE_BUYER_POOL",
                    {
                        "target_min_buyers": int(gate_min_buyers),
                        "actual_buyers_after_force": int(len(buyers)),
                        "gate_mode": bool(gate_force_multi_buyer_pool),
                    },
                )

        # Use simpler matching for now or re-import the bulk match function if available
        # The previous version imported bulk_match_all_buyers but it wasn't in the provided transaction_engine.py content
        # I only wrote basic functions to transaction_engine.py in Step 4565.
        # So I should use the loop based matching from Step 4565 logic or implement bulk there.
        # Step 4565 transaction_engine.py has match_property_for_buyer.
        # Let's use simple loop matching since bulk_match isn't in my written file yet.
        buyer_matches = []
        matched_buyer_ids = set()
        batch_match_logs = []
        downtrend_mode = ("DOWN" in str(market_trend).upper()) or ("PANIC" in str(market_trend).upper())
        try:
            max_holdings_downtrend = int(
                self.config.get(
                    "smart_agent.max_holdings_in_downtrend",
                    self.config.get("max_holdings_in_downtrend", 6),
                )
            )
        except Exception:
            max_holdings_downtrend = 6
        try:
            max_downtrend_buys_per_month = int(
                self.config.get(
                    "smart_agent.max_downtrend_buys_per_month",
                    self.config.get("max_downtrend_buys_per_month", 1),
                )
            )
        except Exception:
            max_downtrend_buys_per_month = 1
        max_downtrend_buys_per_month = max(1, max_downtrend_buys_per_month)

        downtrend_monthly_buys: Dict[int, int] = {}
        if downtrend_mode:
            cursor.execute(
                """
                SELECT buyer_id, COUNT(*)
                FROM transaction_orders
                WHERE created_month = ?
                  AND status IN ('pending_settlement', 'filled')
                GROUP BY buyer_id
                """,
                (month,),
            )
            for r in cursor.fetchall() or []:
                if r and r[0] is not None:
                    downtrend_monthly_buys[int(r[0])] = int(r[1] or 0)
        shortlist_ctrl = self._resolve_candidate_shortlist_controls(cursor, month)
        history_pressure_map = self._build_candidate_history_pressure_map(cursor, month, active_listings)
        # Track in-month shortlist congestion to reduce same-house crowding in later buyers.
        monthly_candidate_pressure: Dict[int, float] = {}
        monthly_candidate_quota_used: Dict[int, int] = {}
        candidate_quota_cfg = self._resolve_candidate_quota_controls()
        candidate_two_stage_cfg = self._resolve_two_stage_candidate_controls()
        try:
            shortlist_interest_weight = float(
                self.config.get(
                    "smart_agent.candidate_shortlist_interest_weight",
                    self.config.get("candidate_shortlist_interest_weight", 0.35),
                )
            )
        except Exception:
            shortlist_interest_weight = 0.35
        try:
            selected_interest_weight = float(
                self.config.get(
                    "smart_agent.candidate_selected_interest_weight",
                    self.config.get("candidate_selected_interest_weight", 1.0),
                )
            )
        except Exception:
            selected_interest_weight = 1.0
        try:
            candidate_inmonth_pressure_decay = float(
                self.config.get(
                    "smart_agent.candidate_inmonth_pressure_decay",
                    self.config.get("candidate_inmonth_pressure_decay", 0.92),
                )
            )
        except Exception:
            candidate_inmonth_pressure_decay = 0.92
        shortlist_interest_weight = max(0.0, min(2.0, float(shortlist_interest_weight)))
        selected_interest_weight = max(0.1, min(3.0, float(selected_interest_weight)))
        candidate_inmonth_pressure_decay = max(0.70, min(1.0, float(candidate_inmonth_pressure_decay)))
        for buyer in buyers:
            if monthly_candidate_pressure and candidate_inmonth_pressure_decay < 0.9999:
                decayed_pressure: Dict[int, float] = {}
                for pid, val in monthly_candidate_pressure.items():
                    new_val = float(val or 0.0) * float(candidate_inmonth_pressure_decay)
                    if abs(new_val) > 1e-6:
                        decayed_pressure[int(pid)] = float(new_val)
                monthly_candidate_pressure = decayed_pressure
            setattr(buyer, "_candidate_top_k_override", int(shortlist_ctrl["top_k"]))
            setattr(
                buyer,
                "_candidate_exploration_slots_override",
                int(shortlist_ctrl["exploration_slots"]),
            )
            combined_pressure: Dict[int, float] = dict(history_pressure_map)
            for pid, p in monthly_candidate_pressure.items():
                combined_pressure[int(pid)] = float(combined_pressure.get(int(pid), 0.0)) + float(p or 0.0)
            setattr(buyer, "_candidate_pressure_map", combined_pressure)
            observed_ctx = self._resolve_observed_market_trend(cursor, month, buyer, market_trend)
            observed_trend = str(observed_ctx.get("observed_trend", market_trend))
            observed_delay = int(observed_ctx.get("delay_months", 0))
            observed_month = int(observed_ctx.get("observed_month", month))

            # M16: block unlimited dip-buy accumulation in downtrend for smart agents.
            if (
                downtrend_mode
                and getattr(buyer, "agent_type", "normal") == "smart"
                and len(getattr(buyer, "owned_properties", []) or []) >= max_holdings_downtrend
            ):
                batch_match_logs.append((
                    buyer.id,
                    month,
                    "BUYER_MATCH",
                    "M16_BLOCK_DOWNTREND_ACCUMULATION",
                    f"holdings={len(getattr(buyer, 'owned_properties', []) or [])} cap={max_holdings_downtrend}",
                    json.dumps(
                        {
                            "market_trend": market_trend,
                            "observed_market_trend": observed_trend,
                            "observed_market_month": observed_month,
                            "m14_info_delay_months": observed_delay,
                            "guard": "max_holdings_in_downtrend",
                            "holdings": len(getattr(buyer, "owned_properties", []) or []),
                            "cap": max_holdings_downtrend,
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "m16_downtrend_guard": True,
                            "m16_max_holdings_in_downtrend": max_holdings_downtrend,
                        },
                        ensure_ascii=False,
                    ),
                    False,
                ))
                continue
            if (
                downtrend_mode
                and getattr(buyer, "agent_type", "normal") == "smart"
                and int(downtrend_monthly_buys.get(int(buyer.id), 0)) >= max_downtrend_buys_per_month
            ):
                batch_match_logs.append((
                    buyer.id,
                    month,
                    "BUYER_MATCH",
                    "M16_BLOCK_DOWNTREND_ACCUMULATION",
                    (
                        f"monthly_buys={int(downtrend_monthly_buys.get(int(buyer.id), 0))} "
                        f"cap={max_downtrend_buys_per_month}"
                    ),
                    json.dumps(
                        {
                            "market_trend": market_trend,
                            "observed_market_trend": observed_trend,
                            "observed_market_month": observed_month,
                            "m14_info_delay_months": observed_delay,
                            "guard": "max_downtrend_buys_per_month",
                            "monthly_buys": int(downtrend_monthly_buys.get(int(buyer.id), 0)),
                            "cap": max_downtrend_buys_per_month,
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "m16_downtrend_guard": True,
                            "m16_max_downtrend_buys_per_month": max_downtrend_buys_per_month,
                        },
                        ensure_ascii=False,
                    ),
                    False,
                ))
                continue
            try:
                # Let matcher persist/reuse cross-month traversal memory.
                setattr(buyer, "_current_matching_month", int(month))
                blocked_reflow_ids = set(int(x) for x in (getattr(buyer, "_reflow_blocked_property_ids", set()) or set()) if x is not None)
                listings_for_buyer = []
                for li in active_listings:
                    try:
                        li_owner = int(li.get("owner_id", li.get("seller_id", -1)) or -1)
                    except Exception:
                        li_owner = -1
                    if li_owner == int(getattr(buyer, "id", -1)):
                        continue
                    listings_for_buyer.append(li)
                if blocked_reflow_ids:
                    filtered_rows = []
                    for li in active_listings:
                        try:
                            pid_i = int(li.get("property_id"))
                        except Exception:
                            pid_i = None
                        try:
                            li_owner = int(li.get("owner_id", li.get("seller_id", -1)) or -1)
                        except Exception:
                            li_owner = -1
                        if li_owner == int(getattr(buyer, "id", -1)):
                            continue
                        if pid_i is None or pid_i in blocked_reflow_ids:
                            continue
                        filtered_rows.append(li)
                    listings_for_buyer = filtered_rows
                hard_whitelist, hard_meta = self._resolve_hard_bucket_property_whitelist(
                    cursor=cursor,
                    buyer_id=int(getattr(buyer, "id", -1) or -1),
                )
                if hard_whitelist is not None:
                    strict_unmapped_property = bool(
                        (self._resolve_hard_bucket_context(cursor) or {}).get("strict_unmapped_property", True)
                    )
                    property_bucket_map = (
                        (self._resolve_hard_bucket_context(cursor) or {}).get("property_bucket_map", {}) or {}
                    )
                    before_count = int(len(listings_for_buyer))
                    rows_after_hard = []
                    for li in listings_for_buyer:
                        try:
                            li_pid = int(li.get("property_id"))
                        except Exception:
                            li_pid = None
                        if li_pid is None:
                            continue
                        if strict_unmapped_property and int(li_pid) not in property_bucket_map:
                            continue
                        if int(li_pid) not in hard_whitelist:
                            continue
                        rows_after_hard.append(li)
                    listings_for_buyer = rows_after_hard
                    self._append_order_log(
                        month,
                        "CANDIDATE_HARD_BUCKET_FILTER",
                        {
                            "buyer_id": int(getattr(buyer, "id", -1)),
                            "before_count": int(before_count),
                            "after_count": int(len(listings_for_buyer)),
                            "dropped_count": int(max(0, before_count - len(listings_for_buyer))),
                            "hard_meta": dict(hard_meta or {}),
                        },
                    )
                quota_blocked_ids: List[int] = []
                quota_blocked_meta: List[Dict[str, int]] = []
                if bool(candidate_quota_cfg.get("enabled", True)) and listings_for_buyer:
                    pre_quota_rows = list(listings_for_buyer)
                    quota_filtered_rows = []
                    for li in listings_for_buyer:
                        try:
                            li_pid = int(li.get("property_id"))
                        except Exception:
                            li_pid = None
                        if li_pid is None:
                            quota_filtered_rows.append(li)
                            continue
                        prop_ref = props_map.get(li_pid) or li
                        quota_limit = self._resolve_property_candidate_quota(
                            li,
                            prop_ref or {},
                            buyer,
                            candidate_quota_cfg,
                            pressure_score=float(combined_pressure.get(int(li_pid), 0.0) or 0.0),
                            stage="stage1",
                            blocked_recovery=False,
                        )
                        used_count = int(monthly_candidate_quota_used.get(li_pid, 0) or 0)
                        if used_count >= quota_limit:
                            quota_blocked_ids.append(int(li_pid))
                            quota_blocked_meta.append(
                                {
                                    "property_id": int(li_pid),
                                    "used_count": int(used_count),
                                    "quota_limit": int(quota_limit),
                                    "stage": 1,
                                }
                            )
                            self._append_order_log(
                                month,
                                "CANDIDATE_QUOTA_BLOCKED",
                                {
                                    "buyer_id": int(getattr(buyer, "id", -1)),
                                    "property_id": int(li_pid),
                                    "used_count": int(used_count),
                                    "quota_limit": int(quota_limit),
                                    "stage": 1,
                                },
                            )
                            continue
                        quota_filtered_rows.append(li)
                    listings_for_buyer = quota_filtered_rows
                    if (
                        bool(candidate_two_stage_cfg.get("enabled", True))
                        and pre_quota_rows
                        and len(listings_for_buyer) < int(candidate_two_stage_cfg.get("min_stage1_pool", 4))
                    ):
                        existing_ids = {
                            int(row.get("property_id"))
                            for row in listings_for_buyer
                            if row.get("property_id") is not None
                        }
                        stage2_added = []
                        max_stage2_fill = int(candidate_two_stage_cfg.get("max_stage2_fill", 6) or 6)
                        for li in pre_quota_rows:
                            if len(stage2_added) >= max_stage2_fill:
                                break
                            try:
                                li_pid = int(li.get("property_id"))
                            except Exception:
                                li_pid = None
                            if li_pid is None or li_pid in existing_ids:
                                continue
                            prop_ref = props_map.get(li_pid) or li
                            quota_limit_stage2 = self._resolve_property_candidate_quota(
                                li,
                                prop_ref or {},
                                buyer,
                                candidate_quota_cfg,
                                pressure_score=float(combined_pressure.get(int(li_pid), 0.0) or 0.0),
                                stage="stage2",
                                blocked_recovery=True,
                            )
                            used_count = int(monthly_candidate_quota_used.get(li_pid, 0) or 0)
                            if used_count >= quota_limit_stage2:
                                quota_blocked_ids.append(int(li_pid))
                                quota_blocked_meta.append(
                                    {
                                        "property_id": int(li_pid),
                                        "used_count": int(used_count),
                                        "quota_limit": int(quota_limit_stage2),
                                        "stage": 2,
                                    }
                                )
                                self._append_order_log(
                                    month,
                                    "CANDIDATE_QUOTA_BLOCKED",
                                    {
                                        "buyer_id": int(getattr(buyer, "id", -1)),
                                        "property_id": int(li_pid),
                                        "used_count": int(used_count),
                                        "quota_limit": int(quota_limit_stage2),
                                        "stage": 2,
                                    },
                                )
                                continue
                            listings_for_buyer.append(li)
                            existing_ids.add(int(li_pid))
                            stage2_added.append(int(li_pid))
                        if stage2_added:
                            self._append_order_log(
                                month,
                                "CANDIDATE_TWO_STAGE_REFILL",
                                {
                                    "buyer_id": int(getattr(buyer, "id", -1)),
                                    "stage1_pool_size": int(len(quota_filtered_rows)),
                                    "stage2_added_count": int(len(stage2_added)),
                                    "stage2_added_property_ids": [int(x) for x in stage2_added[:20]],
                                    "min_stage1_pool": int(candidate_two_stage_cfg.get("min_stage1_pool", 4)),
                                    "max_stage2_fill": int(max_stage2_fill),
                                },
                            )
                if quota_blocked_ids and listings_for_buyer:
                    listings_for_buyer = self._reprioritize_quota_replacement_candidates(
                        buyer,
                        listings_for_buyer,
                        props_map,
                        quota_blocked_ids,
                        combined_pressure,
                        monthly_candidate_quota_used,
                    )
                preferred_backup_ids = [
                    int(x) for x in (getattr(buyer, "_backup_slot_preferred_ids", []) or [])
                    if x is not None
                ]
                if preferred_backup_ids and listings_for_buyer:
                    pref_set = set(preferred_backup_ids)
                    preferred_rows = []
                    normal_rows = []
                    for li in listings_for_buyer:
                        try:
                            li_pid = int(li.get("property_id"))
                        except Exception:
                            li_pid = None
                        if li_pid is not None and li_pid in pref_set:
                            preferred_rows.append(li)
                        else:
                            normal_rows.append(li)
                    listings_for_buyer = preferred_rows + normal_rows
                historical_seen_ids = self._load_buyer_seen_property_ids(
                    cursor=cursor,
                    buyer_id=int(getattr(buyer, "id", -1) or -1),
                    through_month=int(month - 1),
                )
                setattr(buyer, "_historical_seen_property_ids", sorted(int(x) for x in historical_seen_ids))
                historical_seen_listing_prices = self._load_buyer_last_seen_listing_prices(
                    cursor=cursor,
                    buyer_id=int(getattr(buyer, "id", -1) or -1),
                    through_month=int(month - 1),
                )
                try:
                    repriced_threshold = float(
                        self.config.get(
                            "smart_agent.candidate_reprice_reentry_threshold",
                            self.config.get("candidate_reprice_reentry_threshold", 0.05),
                        )
                    )
                except Exception:
                    repriced_threshold = 0.05
                repriced_threshold = max(0.01, min(0.30, float(repriced_threshold)))
                repriced_reentry_ids: set[int] = set()
                for li in listings_for_buyer:
                    try:
                        li_pid = int(li.get("property_id"))
                        current_listed_price = float(li.get("listed_price", 0.0) or 0.0)
                        previous_listed_price = float(historical_seen_listing_prices.get(li_pid, 0.0) or 0.0)
                    except Exception:
                        continue
                    if li_pid <= 0 or current_listed_price <= 0 or previous_listed_price <= 0:
                        continue
                    drawdown = (previous_listed_price - current_listed_price) / max(previous_listed_price, 1.0)
                    if drawdown >= repriced_threshold:
                        repriced_reentry_ids.add(int(li_pid))
                setattr(buyer, "_repriced_reentry_property_ids", sorted(repriced_reentry_ids))
                should_focus_unseen = bool(
                    listings_for_buyer
                    and historical_seen_ids
                    and (
                        bool(getattr(buyer, "_must_continue_search", False))
                        or bool(getattr(buyer, "_buy_task_locked", False))
                        or int(getattr(buyer, "waited_months", 0) or 0) > 0
                    )
                )
                if should_focus_unseen:
                    unseen_rows = []
                    repriced_rows = []
                    seen_rows = []
                    for li in listings_for_buyer:
                        try:
                            li_pid = int(li.get("property_id"))
                        except Exception:
                            li_pid = None
                        if li_pid is not None and li_pid in repriced_reentry_ids:
                            repriced_rows.append(li)
                        elif li_pid is not None and li_pid not in historical_seen_ids:
                            unseen_rows.append(li)
                        else:
                            seen_rows.append(li)
                    if unseen_rows or repriced_rows:
                        listings_for_buyer = repriced_rows + unseen_rows
                        self._append_order_log(
                            month,
                            "BUYER_UNSEEN_CANDIDATE_FOCUS",
                            {
                                "buyer_id": int(getattr(buyer, "id", -1)),
                                "historical_seen_count": int(len(historical_seen_ids)),
                                "visible_unseen_count": int(len(unseen_rows)),
                                "repriced_reentry_count": int(len(repriced_rows)),
                                "visible_seen_count": int(len(seen_rows)),
                                "focused_property_ids": [
                                    int(li.get("property_id")) for li in (repriced_rows + unseen_rows)[:12]
                                    if li.get("property_id") is not None
                                ],
                            },
                        )
                if repriced_reentry_ids:
                    self._append_order_log(
                        month,
                        "BUYER_REPRICED_PROPERTY_REENTRY",
                        {
                            "buyer_id": int(getattr(buyer, "id", -1)),
                            "reentry_property_ids": [int(x) for x in sorted(repriced_reentry_ids)[:20]],
                            "threshold": float(repriced_threshold),
                        },
                    )
                matches = match_properties_for_buyer(
                    buyer,
                    listings_for_buyer,
                    props_map,
                    config=self.config,
                    market_trend=observed_trend,
                )
            except TypeError:
                # Backward-compatible for monkey-patched helpers/tests with old signature.
                matches = match_properties_for_buyer(buyer, listings_for_buyer, props_map, config=self.config)

            strategy_profile = getattr(buyer, "_last_strategy_profile", "unknown")
            weight_payload = getattr(buyer, "_last_dynamic_weights", {})
            match_ctx = (getattr(buyer, "_last_buyer_match_context", {}) or {})
            retry_trace = list(getattr(buyer, "_last_buyer_match_retry_trace", []) or [])
            retry_budget = dict(getattr(buyer, "_last_buyer_match_retry_budget", {}) or {})
            pipeline_stage_trace = [
                str(x) for x in (match_ctx.get("pipeline_stage_trace", []) or [])
                if str(x or "").strip()
            ]
            expected_stage_order = {
                "collect_visible_pool": 1,
                "apply_hard_filters": 2,
                "rank_shortlist": 3,
                "apply_crowd_hard_exclude": 4,
                "llm_decision": 5,
                "post_select_guard": 6,
            }
            trace_positions = [expected_stage_order.get(stage, -1) for stage in pipeline_stage_trace]
            non_negative_positions = [x for x in trace_positions if x >= 0]
            pipeline_order_violation = bool(
                len(non_negative_positions) >= 2
                and any(non_negative_positions[idx] > non_negative_positions[idx + 1] for idx in range(len(non_negative_positions) - 1))
            )
            selected_ids = [int(m.get("property_id")) for m in matches if m.get("property_id") is not None]
            no_selection_code = self._derive_no_selection_reason_code(
                listings_for_buyer_count=int(len(listings_for_buyer)),
                selected_ids=selected_ids,
                match_ctx=match_ctx,
                retry_trace=retry_trace,
                retry_budget=retry_budget,
            )
            # Cross-month sticky search state:
            # buyers keep searching next month unless they matched this month or truly exhausted.
            search_exhausted = bool(retry_budget.get("search_exhausted_this_month", False))
            if selected_ids:
                setattr(buyer, "_must_continue_search", False)
                setattr(buyer, "_search_exhausted", False)
                setattr(buyer, "_buy_task_locked", False)
                setattr(buyer, "_buy_task_lock_reason", "")
            else:
                setattr(buyer, "_must_continue_search", not search_exhausted)
                setattr(buyer, "_search_exhausted", bool(search_exhausted))
                if search_exhausted:
                    setattr(buyer, "_buy_task_locked", False)
                    setattr(buyer, "_buy_task_lock_reason", "")
                if bool(getattr(buyer, "_recovery_queue_mode", False)):
                    if bool(search_exhausted):
                        self._mark_recovery_queue_state(
                            cursor=cursor,
                            month=int(month),
                            buyer_id=int(getattr(buyer, "id", -1)),
                            state="exhausted",
                        )
            if pipeline_order_violation:
                self._append_order_log(
                    month,
                    "PIPELINE_ORDER_VIOLATION",
                    {
                        "buyer_id": int(getattr(buyer, "id", -1)),
                        "trace": pipeline_stage_trace,
                        "expected_order": expected_stage_order,
                    },
                )
            shortlist_ids = [
                int(pid)
                for pid in (match_ctx.get("shortlist_property_ids", []) or [])
                if pid is not None
            ]
            self._record_shortlist_exposure_rows(
                cursor=cursor,
                month=int(month),
                buyer=buyer,
                shortlist_context=list(match_ctx.get("shortlist", []) or []),
                selected_ids=selected_ids,
                no_selection_code=str(no_selection_code or ""),
                match_ctx=match_ctx,
            )
            quota_charge_ids = []
            for pid in shortlist_ids + selected_ids:
                try:
                    pid_i = int(pid)
                except Exception:
                    continue
                if pid_i not in quota_charge_ids:
                    quota_charge_ids.append(pid_i)
            batch_match_logs.append(
                self._build_buyer_match_summary_log(
                    buyer=buyer,
                    month=int(month),
                    strategy_profile=strategy_profile,
                    selected_ids=selected_ids,
                    no_selection_code=str(no_selection_code or ""),
                    listings_for_buyer_count=int(len(listings_for_buyer)),
                    shortlist_ids=shortlist_ids,
                    quota_prefilter_enabled=bool(candidate_quota_cfg.get("enabled", True)),
                    quota_blocked_ids=quota_blocked_ids,
                    quota_blocked_meta=quota_blocked_meta,
                    quota_charge_ids=quota_charge_ids,
                    weight_payload=weight_payload,
                    match_ctx=match_ctx,
                    retry_trace=retry_trace,
                    retry_budget=retry_budget,
                    pipeline_stage_trace=pipeline_stage_trace,
                    pipeline_order_violation=bool(pipeline_order_violation),
                    market_trend=market_trend,
                    observed_trend=observed_trend,
                    observed_month=int(observed_month),
                    observed_delay=int(observed_delay),
                )
            )
            for sid in quota_charge_ids:
                monthly_candidate_quota_used[int(sid)] = int(monthly_candidate_quota_used.get(int(sid), 0) or 0) + 1
            for sid in shortlist_ids:
                monthly_candidate_pressure[int(sid)] = float(monthly_candidate_pressure.get(int(sid), 0.0)) + float(shortlist_interest_weight)
                batch_match_logs.append((
                    buyer.id,
                    month,
                    "BUYER_MATCH",
                    strategy_profile,
                    (f"selected={selected_ids}" if selected_ids else f"selected={selected_ids}; no_selection_code={no_selection_code}"),
                    json.dumps(
                        {
                            "strategy_profile": strategy_profile,
                            "selected_property_ids": selected_ids,
                            "no_selection_reason_code": no_selection_code if not selected_ids else "HAS_SELECTION",
                            "listings_for_buyer_count": int(len(listings_for_buyer)),
                            "candidate_quota_prefilter_enabled": bool(candidate_quota_cfg.get("enabled", True)),
                            "candidate_quota_blocked_count": int(len(quota_blocked_ids)),
                            "candidate_quota_blocked_property_ids": quota_blocked_ids,
                            "candidate_quota_blocked_meta": quota_blocked_meta,
                            "candidate_quota_charge_ids": quota_charge_ids,
                            "weights": weight_payload,
                            "persona_snapshot": match_ctx.get("persona_snapshot", {}),
                            "shortlist_context": match_ctx.get("shortlist", []),
                            "selection_reason": match_ctx.get("selection_reason", ""),
                        "selected_in_shortlist": match_ctx.get("selected_in_shortlist", False),
                        "crowd_mode": match_ctx.get("crowd_mode", ""),
                        "crowd_profile_reasons": match_ctx.get("crowd_profile_reasons", []),
                        "retry_attempt": match_ctx.get("retry_attempt", 0),
                        "excluded_property_count": match_ctx.get("excluded_property_count", 0),
                        "retry_trace": retry_trace,
                            "retry_budget": retry_budget,
                            "llm_route_model": match_ctx.get("llm_route_model", ""),
                            "llm_route_reason": match_ctx.get("llm_route_reason", ""),
                            "llm_gray_score": match_ctx.get("llm_gray_score", 0.0),
                            "factor_contract": match_ctx.get("factor_contract", {}),
                            "bucket_plan": match_ctx.get("bucket_plan", {}),
                            "bucket_distribution": match_ctx.get("bucket_distribution", {}),
                            "pipeline_stage_trace": pipeline_stage_trace,
                            "pipeline_order_violation": bool(pipeline_order_violation),
                            "market_trend_global": market_trend,
                            "observed_market_trend": observed_trend,
                            "observed_market_month": observed_month,
                            "m14_info_delay_months": observed_delay,
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        **(weight_payload or {}),
                        "llm_route_model": match_ctx.get("llm_route_model", ""),
                        "llm_route_reason": match_ctx.get("llm_route_reason", ""),
                        "llm_gray_score": match_ctx.get("llm_gray_score", 0.0),
                        "crowd_mode": match_ctx.get("crowd_mode", ""),
                        "retry_attempt": match_ctx.get("retry_attempt", 0),
                        "factor_contract": match_ctx.get("factor_contract", {}),
                        "bucket_plan": match_ctx.get("bucket_plan", {}),
                        "bucket_distribution": match_ctx.get("bucket_distribution", {}),
                        "pipeline_stage_trace": pipeline_stage_trace,
                        "pipeline_order_violation": bool(pipeline_order_violation),
                        "market_trend_global": market_trend,
                        "observed_market_trend": observed_trend,
                        "observed_market_month": observed_month,
                        "m14_info_delay_months": observed_delay,
                    },
                    ensure_ascii=False,
                ),
                bool(match_ctx.get("llm_called", True)),
            ))

            for match in matches:
                buyer_matches.append({'buyer': buyer, 'listing': match})
                matched_buyer_ids.add(buyer.id)
                pid = match.get("property_id")
                if pid is not None:
                    try:
                        ipid = int(pid)
                    except Exception:
                        ipid = None
                    if ipid is not None:
                        monthly_candidate_pressure[ipid] = float(monthly_candidate_pressure.get(ipid, 0.0)) + float(selected_interest_weight)

        if batch_match_logs:
            cursor.executemany(
                """
                INSERT INTO decision_logs
                (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_match_logs,
            )

        # M12: update waited state after matching.
        for buyer in buyers:
            self._update_wait_state(cursor, buyer, matched=(buyer.id in matched_buyer_ids))

        # Optional trigger helper:
        # force a true multi-buyer competition pool on one listing.
        try:
            force_multi_buyer_pool = bool(
                self.config.get(
                    "smart_agent.force_multi_buyer_pool_enabled",
                    self.config.get("force_multi_buyer_pool_enabled", False),
                )
            )
        except Exception:
            force_multi_buyer_pool = False
        try:
            gate_force_multi_buyer_pool = bool(
                self.config.get(
                    "smart_agent.gate_force_multi_buyer_pool_enabled",
                    self.config.get("gate_force_multi_buyer_pool_enabled", False),
                )
            )
        except Exception:
            gate_force_multi_buyer_pool = False
        enable_forced_pooling = bool(force_multi_buyer_pool or gate_force_multi_buyer_pool)
        if enable_forced_pooling and buyer_matches:
            try:
                from collections import defaultdict
                grouped_by_seller = defaultdict(list)
                for bm in buyer_matches:
                    li = bm.get("listing") or {}
                    sid_raw = li.get("seller_id", li.get("owner_id", -1))
                    sid = int(sid_raw) if sid_raw is not None else -1
                    grouped_by_seller[sid].append(bm)
                # pick seller group with max potential competitors
                seller_id, pool = max(grouped_by_seller.items(), key=lambda kv: len(kv[1]))
                if pool and len(pool) >= 3:
                    # anchor on one concrete listing from this seller, preserve seller linkage
                    anchor_listing = dict(pool[0].get("listing") or {})
                    forced_matches = []
                    seen_buyer_ids = set()
                    for bm in pool:
                        buyer_obj = bm.get("buyer")
                        if buyer_obj is None:
                            continue
                        bid_id = int(getattr(buyer_obj, "id", -1))
                        if bid_id in seen_buyer_ids:
                            continue
                        seen_buyer_ids.add(bid_id)
                        forced_matches.append(
                            {
                                "buyer": buyer_obj,
                                "listing": dict(anchor_listing),
                            }
                        )
                    if len(forced_matches) >= 3:
                        buyer_matches = forced_matches
                        self._append_order_log(
                            month,
                            "FORCE_MULTI_BUYER_POOL",
                            {
                                "seller_id": int(seller_id),
                                "property_id": int(anchor_listing.get("property_id", -1)),
                                "forced_buyer_count": int(len(forced_matches)),
                                "gate_mode": bool(gate_force_multi_buyer_pool),
                            },
                        )
            except Exception as e:
                logger.warning(f"FORCE_MULTI_BUYER_POOL skipped: {e}")

        order_entries = self._create_orders_for_matches(cursor, month, buyer_matches, agent_map)
        orders_created_this_pass = int(len(order_entries))
        if order_entries:
            for entry in order_entries:
                buyer_obj = entry.get("buyer")
                if buyer_obj is None:
                    continue
                if bool(getattr(buyer_obj, "_recovery_queue_mode", False)):
                    self._mark_recovery_queue_state(
                        cursor=cursor,
                        month=int(month),
                        buyer_id=int(getattr(buyer_obj, "id", -1)),
                        state="active",
                        progress_round=int(reflow_depth + 1),
                    )

        # ✅ Phase 3.3 Fix: Log Matches to property_buyer_matches
        if order_entries:
            match_records = []
            for entry in order_entries:
                b = entry['buyer']
                listing_data = entry['listing']
                # Initial intent: Buyer is interested at listed price (or max budget)
                # match_property_for_buyer checks affordability, so bid is roughly listed_price
                bid = listing_data['listed_price']
                match_records.append((
                    month, listing_data['property_id'], b.id, listing_data['listed_price'], bid,
                    1,  # is_valid_bid
                    1,  # proceeded_to_negotiation (All matches proceed in current logic)
                    int(entry["order_id"]),
                    json.dumps(entry.get("match_context", {}), ensure_ascii=False) if entry.get("match_context") else None,
                    entry.get("selection_reason"),
                    1 if entry.get("selected_in_shortlist", False) else 0,
                    "ORDER_CREATED",
                    "NEGOTIATION_PENDING",
                    None,
                    None,
                ))

            if self._table_exists(cursor, "property_buyer_matches"):
                try:
                    cursor.executemany("""
                        INSERT INTO property_buyer_matches
                        (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation,
                         order_id, match_context, selection_reason, selected_in_shortlist,
                         final_outcome, failure_stage, failure_reason, final_price)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, match_records)
                except Exception as e:
                    logger.error(f"Failed to log buyer matches: {e}")

        # Build Interest Map
        interest_registry = {}
        for entry in order_entries:
            pid = entry['listing']['property_id']
            if pid not in interest_registry:
                interest_registry[pid] = []
            interest_registry[pid].append(entry)
        if interest_registry:
            queue_sizes = [len(v) for v in interest_registry.values()]
            top_pid, top_entries = max(interest_registry.items(), key=lambda kv: len(kv[1]))
            self._append_order_log(
                month,
                "COMPETITION_DENSITY_SNAPSHOT",
                {
                    "listing_count_with_interest": int(len(interest_registry)),
                    "max_competitors_single_property": int(max(queue_sizes) if queue_sizes else 0),
                    "avg_competitors_per_interested_property": round(float(sum(queue_sizes) / max(1, len(queue_sizes))), 4),
                    "top_competition_property_id": int(top_pid),
                    "top_competition_buyer_ids": [int(x["buyer"].id) for x in top_entries[:20]],
                },
            )

        # --- 2. Negotiation Phase ---
        if interest_registry:
            logger.info(f"Starting {len(interest_registry)} Negotiation Sessions (Parallel)...")

            tasks = []
            session_metadata = []
            reflow_outbid_candidate_ids: set[int] = set()
            backup_promotion_candidate_ids: set[int] = set()
            reflow_blocked_property_map: Dict[int, set[int]] = {}


            # Local imports to avoid circular dependency
            from transaction_engine import handle_failed_negotiation, run_negotiation_session_async, developer_quick_sale

            # V3: Developer Quick Sale Async Wrapper
            async def developer_quick_sale_async(entries: List[Dict], listing: Dict):
                """Async wrapper with rush-buy queue ranking + tie-break."""
                pid = int(listing.get("property_id", -1))
                queue = self._build_developer_priority_queue(month, listing, entries)
                if queue:
                    self._append_order_log(
                        month,
                        "RUSH_BUY_QUEUE",
                        {
                            "property_id": pid,
                            "tie_breaker_mode": self._resolve_tie_breaker_mode(),
                            "queue_size": len(queue),
                            "top3": [
                                {
                                    "buyer_id": q["buyer"].id,
                                    "offered_price": q["offered_price"],
                                    "aggressiveness": q["aggressiveness"],
                                }
                                for q in queue[:3]
                            ],
                        },
                    )
                for row in queue:
                    buyer = row["buyer"]
                    result = developer_quick_sale(buyer, listing, self.config, offered_price=row["offered_price"])
                    if result['outcome'] == 'success':
                        result["offered_price"] = row["offered_price"]
                        return result
                # All buyers failed
                return {"outcome": "failed", "reason": "No affordable buyer", "history": [], "final_price": 0}

            for pid, interested_entries in interest_registry.items():
                listing = next((item for item in active_listings if item['property_id'] == pid), None)
                if not listing:
                    continue

                seller_id = int(listing.get('seller_id', listing.get('owner_id', -1)) or -1)
                ranked_entries = self._sort_classic_buyers(cursor, month, listing, interested_entries)
                # Safety net: strip any accidental self-trade entries that may come from legacy rows.
                if seller_id >= 0:
                    safe_ranked_entries = []
                    for entry in ranked_entries:
                        buyer_obj = entry.get("buyer")
                        if buyer_obj is None:
                            continue
                        if int(getattr(buyer_obj, "id", -1)) == int(seller_id):
                            order_id_raw = entry.get("order_id")
                            if order_id_raw is None:
                                continue
                            self._close_order(
                                cursor,
                                month=month,
                                order_id=int(order_id_raw),
                                buyer=buyer_obj,
                                status="cancelled",
                                close_reason="SELF_TRADE_BLOCKED",
                                release_amount=float(entry.get("deposit_amount", 0.0)),
                                penalty_amount=0.0,
                            )
                            continue
                        safe_ranked_entries.append(entry)
                    ranked_entries = safe_ranked_entries
                interested_buyers = [e["buyer"] for e in ranked_entries]
                if not interested_buyers:
                    continue
                
                # ✅ V3: 开发商房产规则判定（跳过LLM谈判）
                if seller_id == -1:
                    # 开发商房产：使用规则判定
                    tasks.append(developer_quick_sale_async(interested_entries, listing))
                    session_metadata.append({
                        "pid": pid,
                        "seller": None,  # 开发商无需Agent对象
                        "buyers": interested_buyers,
                        "order_entries": ranked_entries,
                        "listing": listing
                    })
                    continue  # 跳过普通谈判逻辑

                seller_agent = agent_map.get(seller_id)
                if not seller_agent:
                    continue

                # Determine Negotiation Mode
                # market_hint = "买家众多" if len(interested_buyers) > 1 else "单一买家"

                # mode = decide_negotiation_format(seller_agent, interested_buyers, market_hint)

                # ✅ Phase 3.3: Pass db_conn to enable bid recording
                tasks.append(run_negotiation_session_async(seller_agent, interested_buyers, listing, market, month, self.config, self.conn))
                session_metadata.append({
                    "pid": pid,
                    "seller": seller_agent,
                    "buyers": interested_buyers,
                    "order_entries": ranked_entries,
                    "listing": listing
                })

            if tasks:
                session_results = await asyncio.gather(*tasks)
            else:
                session_results = []

            # Process Results
            batch_negotiations = []
            batch_m16_logs = []
            batch_round_book = []
            round_book_enabled = self._table_exists(cursor, "negotiation_round_book")

            buyer_win_counter = {}

            for i, session_result in enumerate(session_results):
                meta = session_metadata[i]
                pid = meta['pid']
                seller_agent = meta['seller']
                interested_buyers = meta['buyers']
                order_entries = meta.get('order_entries', [])
                listing = meta['listing']

                outcome = session_result.get('outcome', 'failed')
                history = session_result.get('history', [])
                winner_id = session_result.get('buyer_id')
                winner = agent_map.get(winner_id) if winner_id else None
                first_offer_by_buyer = self._extract_first_buyer_offer_by_id(history)

                # Keep property_buyer_matches.buyer_bid aligned with real first-round OFFER.
                # Without this, buyer_bid can stay at order snapshot price, which may be
                # higher than actual negotiation offer and mislead diagnostics.
                if first_offer_by_buyer and self._table_exists(cursor, "property_buyer_matches"):
                    for entry in order_entries:
                        b = entry.get("buyer")
                        if b is None:
                            continue
                        oid = entry.get("order_id")
                        if oid is None:
                            continue
                        bid = first_offer_by_buyer.get(int(getattr(b, "id", -1)))
                        if bid is None:
                            continue
                        try:
                            cursor.execute(
                                "UPDATE property_buyer_matches SET buyer_bid=? WHERE order_id=?",
                                (float(bid), int(oid)),
                            )
                        except Exception:
                            # Keep simulation robust even if legacy schema misses this row.
                            pass
                route_model = str(session_result.get("negotiation_route_model", "") or "")
                route_reason = str(session_result.get("negotiation_route_reason", "") or "")
                route_gray = float(session_result.get("negotiation_gray_score", 0.0) or 0.0)
                if isinstance(history, list):
                    route_entry = next(
                        (
                            h for h in history
                            if isinstance(h, dict)
                            and str(h.get("party", "")).lower() == "router"
                            and str(h.get("action", "")).upper() == "MODEL_ROUTE"
                        ),
                        None,
                    )
                    if route_entry:
                        route_model = str(route_entry.get("model", route_model) or route_model)
                        route_reason = str(route_entry.get("content", route_reason) or route_reason)
                        try:
                            route_gray = float(route_entry.get("gray_score", route_gray))
                        except Exception:
                            route_gray = float(route_gray)
                m16_clamp_count = sum(
                    1 for h in history
                    if isinstance(h, dict) and str(h.get("action", "")).upper() == "M16_CLAMP"
                )

                if round_book_enabled and isinstance(history, list):
                    session_mode = str(session_result.get("mode", "classic") or "classic")
                    session_reason = str(session_result.get("reason", "") or "")
                    session_seller_id = int(
                        getattr(seller_agent, "id", listing.get("seller_id", listing.get("owner_id", -1)) or -1)
                    )
                    candidate_buyer_count = int(len(interested_buyers))
                    last_buyer_id = None

                    for idx, event in enumerate(history, start=1):
                        if not isinstance(event, dict):
                            continue

                        try:
                            round_no = int(event.get("round", idx) or idx)
                        except Exception:
                            round_no = int(idx)
                        party = str(event.get("party", "") or "")
                        action = str(event.get("action", "") or "")

                        event_agent_id = event.get("agent_id")
                        try:
                            event_agent_id = int(event_agent_id) if event_agent_id is not None else None
                        except Exception:
                            event_agent_id = None

                        buyer_id_val = None
                        for key in ("buyer_id", "buyer"):
                            if key in event and event.get(key) is not None:
                                try:
                                    buyer_id_val = int(event.get(key))
                                except Exception:
                                    buyer_id_val = None
                                break
                        if buyer_id_val is None and party.lower() == "buyer" and event_agent_id is not None:
                            buyer_id_val = int(event_agent_id)
                        if buyer_id_val is None and party.lower() == "seller" and last_buyer_id is not None:
                            buyer_id_val = int(last_buyer_id)
                        if buyer_id_val is None and winner_id is not None:
                            try:
                                buyer_id_val = int(winner_id)
                            except Exception:
                                buyer_id_val = None
                        if party.lower() == "buyer" and buyer_id_val is not None:
                            last_buyer_id = int(buyer_id_val)

                        seller_id_val = int(session_seller_id)
                        if party.lower() == "seller" and event_agent_id is not None:
                            seller_id_val = int(event_agent_id)

                        quoted_price = None
                        for key in ("price", "quoted_price", "counter_price", "offer_price", "final_price", "clamped_bid"):
                            if key in event and event.get(key) is not None:
                                try:
                                    quoted_price = float(event.get(key))
                                except Exception:
                                    quoted_price = None
                                break

                        message = str(event.get("content", event.get("reason", "")) or "")
                        llm_called = event.get("llm_called")
                        if llm_called is None:
                            upper_action = action.upper()
                            if upper_action in {"MODEL_ROUTE", "M16_CLAMP", "WIN_BID"}:
                                llm_called = 0
                            elif party.lower() in {"buyer", "seller", "arbiter"}:
                                llm_called = 1
                            else:
                                llm_called = 0

                        batch_round_book.append((
                            int(month),
                            int(pid),
                            int(seller_id_val),
                            int(buyer_id_val) if buyer_id_val is not None else None,
                            int(candidate_buyer_count),
                            int(round_no),
                            party,
                            action,
                            quoted_price,
                            message,
                            session_mode,
                            str(outcome),
                            session_reason,
                            route_model,
                            float(route_gray),
                            route_reason,
                            int(bool(llm_called)),
                            json.dumps(event, ensure_ascii=False),
                        ))

                batch_m16_logs.append((
                    winner_id if winner_id is not None else (interested_buyers[0].id if interested_buyers else -1),
                    month,
                    "NEGOTIATION_ROUTE",
                    str(route_model or "unknown").upper(),
                    f"property={pid}; route={route_model}; gray={route_gray:.3f}",
                    json.dumps(
                        {
                            "property_id": int(pid),
                            "negotiation_route_model": route_model,
                            "negotiation_route_reason": route_reason,
                            "negotiation_gray_score": float(route_gray),
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "negotiation_route_model": route_model,
                            "negotiation_gray_score": float(route_gray),
                        },
                        ensure_ascii=False,
                    ),
                    True,
                ))
                if m16_clamp_count > 0:
                    batch_m16_logs.append((
                        winner_id if winner_id is not None else (interested_buyers[0].id if interested_buyers else -1),
                        month,
                        "M16_GUARD",
                        "OFFER_CLAMP",
                        f"property={pid}, clamp_count={m16_clamp_count}",
                        json.dumps({"property_id": pid, "clamp_count": m16_clamp_count, "history": history}, ensure_ascii=False),
                        json.dumps({"m16_offer_clamp_count": m16_clamp_count}, ensure_ascii=False),
                        False,
                    ))

                # Context Metrics from Negotiation?
                # The negotiation history contains reason/thought process.
                # If we want specific metrics (like bid/ask spread), we can parse history or return it.
                # Currently negotiation returns a simple dict.
                # We can enhance it later. For now, we log history.

                if outcome == 'success' and winner:
                    buy_cap = get_buyer_monthly_buy_cap(winner, config=self.config)
                    already_won = buyer_win_counter.get(winner.id, 0)
                    if (
                        downtrend_mode
                        and getattr(winner, "agent_type", "normal") == "smart"
                        and int(downtrend_monthly_buys.get(int(winner.id), 0)) >= max_downtrend_buys_per_month
                    ):
                        failed_negotiations += 1
                        for entry in order_entries:
                            if entry["buyer"].id == winner.id:
                                self._close_order(
                                    cursor,
                                    month=month,
                                    order_id=entry["order_id"],
                                    buyer=winner,
                                    status="cancelled",
                                    close_reason=(
                                        "M16 guard: downtrend monthly buy cap reached "
                                        f"({max_downtrend_buys_per_month})"
                                    ),
                                    release_amount=float(entry.get("deposit_amount", 0.0)),
                                    penalty_amount=0.0,
                                )
                                break
                        batch_m16_logs.append((
                            winner.id,
                            month,
                            "BUYER_MATCH",
                            "M16_BLOCK_DOWNTREND_ACCUMULATION",
                            (
                                f"monthly_buys={int(downtrend_monthly_buys.get(int(winner.id), 0))} "
                                f"cap={max_downtrend_buys_per_month}"
                            ),
                            json.dumps(
                                {
                                    "guard": "max_downtrend_buys_per_month",
                                    "market_trend": market_trend,
                                    "monthly_buys": int(downtrend_monthly_buys.get(int(winner.id), 0)),
                                    "cap": max_downtrend_buys_per_month,
                                    "property_id": pid,
                                },
                                ensure_ascii=False,
                            ),
                            json.dumps(
                                {
                                    "m16_downtrend_guard": True,
                                    "m16_max_downtrend_buys_per_month": max_downtrend_buys_per_month,
                                },
                                ensure_ascii=False,
                            ),
                            False,
                        ))
                        batch_negotiations.append((
                            winner.id,
                            seller_agent.id if seller_agent else -1,
                            pid,
                            len(history),
                            0,
                            False,
                            f"M16 downtrend monthly buy cap reached ({max_downtrend_buys_per_month})",
                            json.dumps(history)
                        ))
                        self._register_buyer_failure(
                            cursor,
                            winner,
                            month,
                            f"M16 downtrend monthly buy cap reached ({max_downtrend_buys_per_month})",
                        )
                        continue
                    if already_won >= buy_cap:
                        failed_negotiations += 1
                        for entry in order_entries:
                            if entry["buyer"].id == winner.id:
                                self._close_order(
                                    cursor,
                                    month=month,
                                    order_id=entry["order_id"],
                                    buyer=winner,
                                    status="cancelled",
                                    close_reason=f"Buy cap reached ({buy_cap})",
                                    release_amount=float(entry.get("deposit_amount", 0.0)),
                                    penalty_amount=0.0,
                                )
                                break
                        batch_negotiations.append((
                            winner.id,
                            seller_agent.id if seller_agent else -1,
                            pid,
                            len(history),
                            0,
                            False,
                            f"Buyer reached monthly buy cap ({buy_cap})",
                            json.dumps(history)
                        ))
                        self._register_buyer_failure(cursor, winner, month, f"Buy cap reached ({buy_cap})")
                        continue

                    # Get actual seller_id for display/chain progression (normalized int above).
                    actual_seller_id = seller_id

                    # Keep winner deposit frozen until settlement month.
                    winner_order = next((e for e in order_entries if e["buyer"].id == winner.id), None)
                    
                    # Display
                    exchange_display.show_deal_result(True, winner.id, actual_seller_id, pid, session_result['final_price'])

                    # Record agreement first; actual transfer/ownership happens on settlement month (M19).
                    final_price = session_result['final_price']
                    if winner_order:
                        # Final hard gate: keep precheck and pending-settlement admission consistent.
                        ok_final_gate, reason_code_final_gate, gate_metrics = self._precheck_order_affordability(
                            buyer=winner,
                            listing=listing,
                            offer_price=float(final_price),
                            available_cash_credit=float(winner_order.get("deposit_amount", 0.0) or 0.0),
                        )
                        if not ok_final_gate:
                            fail_reason = self._map_affordability_reason_to_close_reason(reason_code_final_gate)
                            self._append_order_log(
                                month,
                                "FINAL_PRESETTLEMENT_GATE_REJECTED",
                                {
                                    "property_id": int(pid),
                                    "winner_buyer_id": int(winner.id),
                                    "reason_code": str(reason_code_final_gate),
                                    "fail_reason": str(fail_reason),
                                    "metrics": gate_metrics,
                                },
                            )
                            failed_negotiations += 1
                            for entry in order_entries:
                                fail_buyer = entry["buyer"]
                                self._register_buyer_failure(cursor, fail_buyer, month, fail_reason)
                                self._close_order(
                                    cursor=cursor,
                                    month=month,
                                    order_id=entry["order_id"],
                                    buyer=fail_buyer,
                                    status="cancelled",
                                    close_reason=str(fail_reason),
                                    release_amount=float(entry.get("deposit_amount", 0.0)),
                                    penalty_amount=0.0,
                                )
                                try:
                                    if int(getattr(fail_buyer, "id", -1)) == int(winner.id):
                                        pool = list(getattr(fail_buyer, "_backup_slot_pool", []) or [])
                                        if pool:
                                            backup_promotion_candidate_ids.add(int(fail_buyer.id))
                                            blocked_set = reflow_blocked_property_map.setdefault(int(fail_buyer.id), set())
                                            blocked_set.add(int(pid))
                                except Exception:
                                    pass
                                batch_negotiations.append((
                                    fail_buyer.id,
                                    seller_agent.id if seller_agent else -1,
                                    pid,
                                    len(history),
                                    0,
                                    False,
                                    str(fail_reason),
                                    json.dumps(history),
                                ))
                            continue

                        lock_ok = self._lock_property_for_pending_settlement(cursor, month, int(pid))
                        if not lock_ok:
                            fail_reason = "Settlement failed: property unavailable (lock conflict)"
                            failed_negotiations += 1
                            for entry in order_entries:
                                fail_buyer = entry["buyer"]
                                self._register_buyer_failure(cursor, fail_buyer, month, fail_reason)
                                self._close_order(
                                    cursor=cursor,
                                    month=month,
                                    order_id=entry["order_id"],
                                    buyer=fail_buyer,
                                    status="cancelled",
                                    close_reason=fail_reason,
                                    release_amount=float(entry.get("deposit_amount", 0.0)),
                                    penalty_amount=0.0,
                                )
                                batch_negotiations.append((
                                    fail_buyer.id,
                                    seller_agent.id if seller_agent else -1,
                                    pid,
                                    len(history),
                                    0,
                                    False,
                                    fail_reason,
                                    json.dumps(history),
                                ))
                                try:
                                    pool = list(getattr(fail_buyer, "_backup_slot_pool", []) or [])
                                    if pool:
                                        backup_promotion_candidate_ids.add(int(fail_buyer.id))
                                        blocked_set = reflow_blocked_property_map.setdefault(int(fail_buyer.id), set())
                                        blocked_set.add(int(pid))
                                except Exception:
                                    pass
                            continue

                        moved = self._move_order_to_pending_settlement(
                            cursor=cursor,
                            month=month,
                            order_id=winner_order["order_id"],
                            agreed_price=float(final_price),
                            negotiation_rounds=len(history),
                        )
                        if not moved:
                            # Roll back property lock when winner order is no longer pending.
                            cursor.execute(
                                "UPDATE properties_market SET status='for_sale' WHERE property_id=? AND status='pending_settlement'",
                                (pid,),
                            )
                            if pid in props_map:
                                props_map[pid]["status"] = "for_sale"
                            fail_reason = "Settlement failed: order state conflict"
                            failed_negotiations += 1
                            for entry in order_entries:
                                fail_buyer = entry["buyer"]
                                self._register_buyer_failure(cursor, fail_buyer, month, fail_reason)
                                self._close_order(
                                    cursor=cursor,
                                    month=month,
                                    order_id=entry["order_id"],
                                    buyer=fail_buyer,
                                    status="cancelled",
                                    close_reason=fail_reason,
                                    release_amount=float(entry.get("deposit_amount", 0.0)),
                                    penalty_amount=0.0,
                                )
                                batch_negotiations.append((
                                    fail_buyer.id,
                                    seller_agent.id if seller_agent else -1,
                                    pid,
                                    len(history),
                                    0,
                                    False,
                                    fail_reason,
                                    json.dumps(history),
                                ))
                                try:
                                    pool = list(getattr(fail_buyer, "_backup_slot_pool", []) or [])
                                    if pool:
                                        backup_promotion_candidate_ids.add(int(fail_buyer.id))
                                        blocked_set = reflow_blocked_property_map.setdefault(int(fail_buyer.id), set())
                                        blocked_set.add(int(pid))
                                except Exception:
                                    pass
                            continue
                        self._persist_order_prequalification_snapshot(
                            cursor=cursor,
                            order_id=int(winner_order["order_id"]),
                            buyer=winner,
                        )

                        if pid in props_map:
                            props_map[pid]["status"] = "pending_settlement"
                        # Close all losing orders and release deposits.
                        for entry in order_entries:
                            b = entry["buyer"]
                            if b.id == winner.id:
                                continue
                            refined_outbid_reason = self._derive_outbid_reason(
                                winner,
                                b,
                                listing,
                                order_entries,
                                history=history,
                                session_mode=str(session_result.get("mode", "") or ""),
                            )
                            self._register_buyer_failure(cursor, b, month, refined_outbid_reason)
                            self._close_order(
                                cursor,
                                month=month,
                                order_id=entry["order_id"],
                                buyer=b,
                                status="cancelled",
                                close_reason=refined_outbid_reason,
                                release_amount=float(entry.get("deposit_amount", 0.0)),
                                penalty_amount=0.0,
                            )
                            try:
                                motive = str(getattr(getattr(b, "story", None), "purchase_motive_primary", "") or "").strip().lower()
                                need_school = bool(getattr(getattr(b, "preference", None), "need_school_district", False))
                                hard_need = motive in {"starter_entry", "starter_home", "education_driven", "chain_replacement"} or need_school
                                if hard_need and str(refined_outbid_reason or "").lower().startswith("outbid"):
                                    reflow_outbid_candidate_ids.add(int(b.id))
                                    blocked_set = reflow_blocked_property_map.setdefault(int(b.id), set())
                                    blocked_set.add(int(pid))
                                pool = list(getattr(b, "_backup_slot_pool", []) or [])
                                if pool and str(refined_outbid_reason or "").lower().startswith("outbid"):
                                    backup_promotion_candidate_ids.add(int(b.id))
                                    blocked_set = reflow_blocked_property_map.setdefault(int(b.id), set())
                                    blocked_set.add(int(pid))
                            except Exception:
                                pass
                        buyer_win_counter[winner.id] = already_won + 1
                        if downtrend_mode and getattr(winner, "agent_type", "normal") == "smart":
                            downtrend_monthly_buys[int(winner.id)] = int(downtrend_monthly_buys.get(int(winner.id), 0)) + 1
                        setattr(winner, "_must_continue_search", False)
                        setattr(winner, "_search_exhausted", False)
                        self._register_buyer_success(cursor, winner.id, month=month)
                        self._register_seller_chain_progress(cursor, actual_seller_id)
                        batch_negotiations.append(
                            (winner.id, actual_seller_id, pid, len(history), final_price, True, "Deal pending settlement", json.dumps(history))
                        )
                    else:
                        failed_negotiations += 1
                        batch_negotiations.append((
                            winner.id,
                            seller_agent.id if seller_agent else -1,
                            pid,
                            len(history),
                            0,
                            False,
                            "Winner order missing",
                            json.dumps(history)
                        ))

                else:
                    failed_negotiations += 1
                    # Negotiation failed: cancel all orders and release full deposits.
                    session_fail_reason = str(session_result.get('reason', 'Negotiation Failed') or 'Negotiation Failed')
                    fail_reason_by_buyer: Dict[int, str] = {}
                    for entry in order_entries:
                        entry_buyer = entry["buyer"]
                        entry_buyer_id = int(getattr(entry_buyer, "id", -1))
                        buyer_fail_reason = self._derive_negotiation_failure_reason(
                            session_reason=session_fail_reason,
                            history=history,
                            buyer=entry_buyer,
                            listing=listing,
                        )
                        fail_reason_by_buyer[entry_buyer_id] = str(buyer_fail_reason)
                        self._register_buyer_failure(
                            cursor,
                            entry_buyer,
                            month,
                            str(buyer_fail_reason),
                        )
                        self._close_order(
                            cursor,
                            month=month,
                            order_id=entry["order_id"],
                            buyer=entry_buyer,
                            status="cancelled",
                            close_reason=str(buyer_fail_reason),
                            release_amount=float(entry.get("deposit_amount", 0.0)),
                            penalty_amount=0.0,
                        )
                        try:
                            pool = list(getattr(entry_buyer, "_backup_slot_pool", []) or [])
                            if pool:
                                backup_promotion_candidate_ids.add(int(entry_buyer.id))
                                blocked_set = reflow_blocked_property_map.setdefault(int(entry_buyer.id), set())
                                blocked_set.add(int(pid))
                        except Exception:
                            pass
                    # Log failed（修复：seller_agent可能为None，如开发商房产）
                    for buyer in interested_buyers:
                        buyer_fail_reason = fail_reason_by_buyer.get(
                            int(getattr(buyer, "id", -1)),
                            self._derive_negotiation_failure_reason(
                                session_reason=session_fail_reason,
                                history=history,
                                buyer=buyer,
                                listing=listing,
                            ),
                        )
                        batch_negotiations.append((
                            buyer.id, 
                            seller_agent.id if seller_agent else -1,  # 开发商房产seller_id=-1
                            pid, 
                            len(history),
                            0, False,
                            str(buyer_fail_reason),
                            json.dumps(history)
                        ))

                    # Handle failed (Price Cut)
                    potential_buyers_est = len(interested_buyers)
                    if seller_agent is not None:
                        try:
                            adjusted = handle_failed_negotiation(seller_agent, listing, market, potential_buyers_count=potential_buyers_est)
                            if adjusted:
                                cursor.execute("UPDATE properties_market SET listed_price=?, min_price=? WHERE property_id=?",
                                               (listing['listed_price'], listing['min_price'], pid))
                        except Exception as e:
                            logger.warning(f"Failed to adjust price after failure: {e}")

            if batch_negotiations:
                # Need to handle table columns match.
                # negotiations table might strictly be (buyer_id, seller_id, property_id, round_count, final_price, success, reason, log)
                cursor.executemany("INSERT INTO negotiations (buyer_id, seller_id, property_id, round_count, final_price, success, reason, log) VALUES (?,?,?,?,?,?,?,?)", batch_negotiations)
            if batch_m16_logs:
                cursor.executemany(
                    """
                    INSERT INTO decision_logs
                    (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch_m16_logs,
                )
            if round_book_enabled and batch_round_book:
                cursor.executemany(
                    """
                    INSERT INTO negotiation_round_book
                    (
                        month, property_id, seller_id, buyer_id, candidate_buyer_count,
                        round_no, party, action, quoted_price, message, session_mode,
                        session_outcome, session_reason, route_model, route_gray_score,
                        route_reason, llm_called, raw_event_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch_round_book,
                )

        # Same-month reflow pass:
        # hard-need buyers who lost via outbid can immediately retry other listings.
        try:
            reflow_enabled_raw = self.config.get(
                "smart_agent.same_month_outbid_reflow_enabled",
                self.config.get("same_month_outbid_reflow_enabled", True),
            )
        except Exception:
            reflow_enabled_raw = True
        if isinstance(reflow_enabled_raw, bool):
            reflow_enabled = reflow_enabled_raw
        else:
            reflow_enabled = str(reflow_enabled_raw).strip().lower() in {"1", "true", "yes", "y", "on"}

        try:
            reflow_max_rounds_raw = self.config.get(
                "smart_agent.same_month_outbid_reflow_max_rounds",
                self.config.get("same_month_outbid_reflow_max_rounds", 3),
            )
            reflow_max_rounds = int(reflow_max_rounds_raw)
        except Exception:
            reflow_max_rounds = 3
        reflow_max_rounds = max(1, min(16, reflow_max_rounds))

        reflow_candidates = []
        reflow_new_candidate_count = 0
        recovery_combined_pressure: Dict[int, float] = dict(history_pressure_map)
        for pid, pressure_val in monthly_candidate_pressure.items():
            recovery_combined_pressure[int(pid)] = float(recovery_combined_pressure.get(int(pid), 0.0)) + float(pressure_val or 0.0)
        can_spawn_next_reflow = bool(reflow_enabled and int(reflow_depth) < int(reflow_max_rounds - 1))
        if can_spawn_next_reflow:
            backup_property_live_cache: Dict[int, Tuple[str, int]] = {}
            recovery_promotion_usage: Dict[int, int] = {}
            outbid_ids = set(reflow_outbid_candidate_ids) if 'reflow_outbid_candidate_ids' in locals() else set()
            backup_ids = set(backup_promotion_candidate_ids) if 'backup_promotion_candidate_ids' in locals() else set()
            locked_ids = set()
            for a_id, a_obj in agent_map.items():
                try:
                    deferred_month = int(getattr(a_obj, "_recovery_deferred_month", -1) or -1)
                    if deferred_month == int(month):
                        continue
                    if bool(getattr(a_obj, "_buy_task_locked", False)) and (not bool(getattr(a_obj, "_search_exhausted", False))) and (not bool(getattr(a_obj, "buy_completed", 0))):
                        locked_ids.add(int(a_id))
                except Exception:
                    continue
            local_reflow_ids = sorted(list(outbid_ids | backup_ids | locked_ids))
            for bid in local_reflow_ids:
                buyer_obj = agent_map.get(int(bid))
                if buyer_obj is None:
                    continue
                self._upsert_recovery_queue(
                    cursor=cursor,
                    month=int(month),
                    buyer_id=int(bid),
                    lock_reason=str(getattr(buyer_obj, "_buy_task_lock_reason", "") or ""),
                )

            active_queue_rows = []
            if local_reflow_ids:
                cursor.execute(
                    """
                    SELECT buyer_id, rounds_used
                    FROM buyer_recovery_queue
                    WHERE month=? AND state='active'
                    ORDER BY buyer_id
                    """,
                    (int(month),),
                )
                active_queue_rows = cursor.fetchall() or []
            queue_round_buyer_ids: List[int] = []
            for row in active_queue_rows:
                try:
                    bid = int(row[0])
                    rounds_used = int(row[1] or 0)
                except Exception:
                    continue
                if rounds_used >= int(reflow_max_rounds):
                    self._mark_recovery_queue_state(cursor, month, bid, "exhausted")
                    continue
                buyer_obj = agent_map.get(int(bid))
                if buyer_obj is None:
                    continue
                if bool(getattr(buyer_obj, "buy_completed", 0)):
                    self._mark_recovery_queue_state(cursor, month, bid, "completed")
                    continue
                if bool(getattr(buyer_obj, "_search_exhausted", False)):
                    self._mark_recovery_queue_state(cursor, month, bid, "exhausted")
                    continue

                recovery_reason = str(getattr(buyer_obj, "_buy_task_lock_reason", "") or "")

                blocked_set = set((reflow_blocked_property_map.get(int(bid), set()) if 'reflow_blocked_property_map' in locals() else set()) or set())
                if blocked_set:
                    existing = set(int(x) for x in (getattr(buyer_obj, "_reflow_blocked_property_ids", set()) or set()) if x is not None)
                    setattr(buyer_obj, "_reflow_blocked_property_ids", existing | blocked_set)

                attempted_ids = self._load_recovery_attempted_property_ids(cursor, int(month), int(bid))
                backup_pool = list(getattr(buyer_obj, "_backup_slot_pool", []) or [])
                preferred_ids = []
                cleaned_pool = []
                for item in backup_pool:
                    try:
                        pid_i = int(item.get("property_id", -1))
                    except Exception:
                        pid_i = -1
                    if pid_i < 0:
                        continue
                    if pid_i in blocked_set or pid_i in attempted_ids:
                        continue
                    if pid_i not in backup_property_live_cache:
                        cursor.execute(
                            "SELECT status, owner_id FROM properties_market WHERE property_id=?",
                            (int(pid_i),),
                        )
                        srow = cursor.fetchone()
                        if srow:
                            p_status = str(srow[0]) if srow[0] is not None else "MISSING"
                            try:
                                p_owner = int(srow[1]) if srow[1] is not None else -1
                            except Exception:
                                p_owner = -1
                        else:
                            p_status = "MISSING"
                            p_owner = -1
                        backup_property_live_cache[int(pid_i)] = (str(p_status), int(p_owner))
                    p_status, p_owner = backup_property_live_cache.get(int(pid_i), ("MISSING", -1))
                    if str(p_status) != "for_sale":
                        continue
                    if int(p_owner) == int(getattr(buyer_obj, "id", -1)):
                        continue
                    cleaned_pool.append(item)
                    preferred_ids.append(int(pid_i))
                if len(cleaned_pool) != len(backup_pool):
                    setattr(buyer_obj, "_backup_slot_pool", cleaned_pool)
                    self._append_order_log(
                        month,
                        "BACKUP_SLOT_POOL_CLEANED",
                        {
                            "buyer_id": int(bid),
                            "original_size": int(len(backup_pool)),
                            "cleaned_size": int(len(cleaned_pool)),
                            "removed_size": int(max(0, len(backup_pool) - len(cleaned_pool))),
                        },
                    )

                ranked_backup_candidates: List[Dict] = []
                if preferred_ids:
                    ranked_backup_candidates = self._build_recovery_refill_candidates(
                        cursor=cursor,
                        month=int(month),
                        buyer=buyer_obj,
                        active_listings=active_listings,
                        props_map=props_map,
                        blocked_property_ids=blocked_set,
                        attempted_property_ids=attempted_ids,
                        combined_pressure=recovery_combined_pressure,
                        monthly_candidate_quota_used=monthly_candidate_quota_used,
                        candidate_quota_cfg=candidate_quota_cfg,
                        limit=max(6, len(preferred_ids)),
                        candidate_id_whitelist=set(int(x) for x in preferred_ids),
                        emit_log=False,
                        recovery_reason=recovery_reason,
                    )

                refill_candidates = self._build_recovery_refill_candidates(
                    cursor=cursor,
                    month=int(month),
                    buyer=buyer_obj,
                    active_listings=active_listings,
                    props_map=props_map,
                    blocked_property_ids=blocked_set,
                    attempted_property_ids=attempted_ids,
                    combined_pressure=recovery_combined_pressure,
                    monthly_candidate_quota_used=monthly_candidate_quota_used,
                    candidate_quota_cfg=candidate_quota_cfg,
                    limit=6,
                    emit_log=False,
                    recovery_reason=recovery_reason,
                )

                if refill_candidates:
                    existing_pool = list(getattr(buyer_obj, "_backup_slot_pool", []) or [])
                    existing_ids = set()
                    for item in existing_pool:
                        try:
                            existing_ids.add(int(item.get("property_id", -1)))
                        except Exception:
                            continue
                    for item in refill_candidates:
                        pid_i = int(item.get("property_id", -1) or -1)
                        if pid_i < 0 or pid_i in existing_ids:
                            continue
                        existing_pool.append(
                            {
                                "property_id": int(pid_i),
                                "candidate_rank": 99,
                                "listed_price": float(item.get("listed_price", 0.0) or 0.0),
                                "owner_id": int(item.get("owner_id", -1) or -1),
                            }
                        )
                        existing_ids.add(int(pid_i))
                    setattr(buyer_obj, "_backup_slot_pool", existing_pool)

                if ranked_backup_candidates or refill_candidates:
                    combined_candidate_map: Dict[int, Dict] = {}
                    for item in ranked_backup_candidates + refill_candidates:
                        try:
                            pid_i = int(item.get("property_id", -1) or -1)
                        except Exception:
                            pid_i = -1
                        if pid_i < 0:
                            continue
                        existing_item = combined_candidate_map.get(int(pid_i))
                        if existing_item is None:
                            combined_candidate_map[int(pid_i)] = dict(item)
                            continue
                        if self._recovery_candidate_sort_key(item, recovery_promotion_usage) > self._recovery_candidate_sort_key(existing_item, recovery_promotion_usage):
                            combined_candidate_map[int(pid_i)] = dict(item)

                    prioritized_candidates = sorted(
                        list(combined_candidate_map.values()),
                        key=lambda item: self._recovery_candidate_sort_key(item, recovery_promotion_usage),
                        reverse=True,
                    )
                    preferred_ids = [int(item["property_id"]) for item in prioritized_candidates[:6]]
                else:
                    preferred_ids = []

                if preferred_ids:
                    reflow_new_candidate_count += 1
                    queue_round_buyer_ids.append(int(bid))
                    setattr(buyer_obj, "_must_continue_search", True)
                    setattr(buyer_obj, "_search_exhausted", False)
                    setattr(buyer_obj, "_recovery_deferred_month", -1)
                    setattr(buyer_obj, "_backup_slot_preferred_ids", preferred_ids[:6])
                    setattr(buyer_obj, "_recovery_queue_mode", True)
                    setattr(buyer_obj, "_recovery_round", int(reflow_depth + 1))
                    reflow_candidates.append(buyer_obj)
                    self._append_order_log(
                        month,
                        "BACKUP_SLOT_PROMOTED",
                        {
                            "buyer_id": int(bid),
                            "reflow_round": int(reflow_depth + 1),
                            "preferred_property_ids": [int(x) for x in preferred_ids[:6]],
                            "backup_pool_size": int(len(backup_pool)),
                            "attempted_size": int(len(attempted_ids)),
                            "ranked_backup_ids": [int(item["property_id"]) for item in ranked_backup_candidates[:6]],
                            "fresh_refill_ids": [int(item["property_id"]) for item in refill_candidates[:6]],
                        },
                    )
                    for promoted_pid in preferred_ids[:2]:
                        recovery_promotion_usage[int(promoted_pid)] = int(recovery_promotion_usage.get(int(promoted_pid), 0)) + 1
                else:
                    self._mark_recovery_queue_state(
                        cursor,
                        month,
                        int(bid),
                        "deferred_next_month",
                        progress_round=int(reflow_depth + 1),
                    )
                    setattr(buyer_obj, "_must_continue_search", True)
                    setattr(buyer_obj, "_search_exhausted", False)
                    setattr(buyer_obj, "_recovery_deferred_month", int(month))
                    self._append_order_log(
                        month,
                        "BUYER_RECOVERY_DEFERRED",
                        {
                            "buyer_id": int(bid),
                            "reflow_round": int(reflow_depth + 1),
                            "reason": "NO_SAME_MONTH_RECOVERY_CANDIDATES",
                            "blocked_property_count": int(len(blocked_set)),
                            "attempted_property_count": int(len(attempted_ids)),
                        },
                    )

            self._bump_recovery_rounds(cursor, int(month), queue_round_buyer_ids)
        else:
            self._append_order_log(
                month,
                "BUYER_REFLOW_PASS_SKIPPED",
                {
                    "reflow_enabled": bool(reflow_enabled),
                    "reflow_round": int(reflow_depth + 1),
                    "reflow_max_rounds": int(reflow_max_rounds),
                    "skip_reason": (
                        "DISABLED"
                        if not bool(reflow_enabled)
                        else "MAX_ROUNDS_REACHED"
                    ),
                    "outbid_candidate_count": int(len(reflow_outbid_candidate_ids)) if 'reflow_outbid_candidate_ids' in locals() else 0,
                    "backup_promotion_candidate_count": int(len(backup_promotion_candidate_ids)) if 'backup_promotion_candidate_ids' in locals() else 0,
                },
            )

        if reflow_candidates:
            self._append_order_log(
                month,
                "BUYER_REFLOW_PASS",
                {
                    "reflow_round": int(reflow_depth + 1),
                    "reflow_max_rounds": int(reflow_max_rounds),
                    "candidate_count": len(reflow_candidates),
                    "new_candidate_buyers": int(reflow_new_candidate_count),
                    "orders_created_this_pass": int(orders_created_this_pass),
                    "buyer_ids": [int(getattr(b, "id", -1)) for b in reflow_candidates[:40]],
                    "backup_promotion_candidate_count": int(len(backup_promotion_candidate_ids)) if 'backup_promotion_candidate_ids' in locals() else 0,
                },
            )
            reflow_tx, reflow_failed = await self.process_monthly_transactions(
                month=month,
                buyers=reflow_candidates,
                listings_by_zone=listings_by_zone,
                active_listings=active_listings,
                props_map=props_map,
                agent_map=agent_map,
                market=market,
                wf_logger=wf_logger,
                exchange_display=exchange_display,
                reflow_pass=True,
                reflow_depth=int(reflow_depth + 1),
            )
            transactions_count += int(reflow_tx)
            failed_negotiations += int(reflow_failed)
            for b in reflow_candidates:
                if hasattr(b, "_recovery_queue_mode"):
                    setattr(b, "_recovery_queue_mode", False)
                if hasattr(b, "_recovery_round"):
                    setattr(b, "_recovery_round", 0)
        elif can_spawn_next_reflow:
            self._append_order_log(
                month,
                "BUYER_REFLOW_PASS_SKIPPED",
                {
                    "reflow_enabled": bool(reflow_enabled),
                    "reflow_round": int(reflow_depth + 1),
                    "reflow_max_rounds": int(reflow_max_rounds),
                    "skip_reason": (
                        "NO_PROGRESS_OR_CANDIDATES"
                        if int(orders_created_this_pass) <= 0 and int(reflow_new_candidate_count) <= 0
                        else "NO_OUTBID_CANDIDATES"
                    ),
                    "outbid_candidate_count": int(len(reflow_outbid_candidate_ids)) if 'reflow_outbid_candidate_ids' in locals() else 0,
                },
            )

        # Deadline hard-clear:
        # listings that reached deadline month can be force-cleared (config-gated)
        # to keep simulation behavior explicit and observable.
        if not reflow_pass:
            forced_sales = self._force_sell_due_listings(
                cursor=cursor,
                month=int(month),
                buyers=buyers,
                agent_map=agent_map,
                props_map=props_map,
                market=market,
            )
            transactions_count += int(forced_sales)

        # cleanup temporary per-buyer blocked ids only at root pass
        if int(reflow_depth) == 0:
            for b in buyers:
                if hasattr(b, "_reflow_blocked_property_ids"):
                    try:
                        delattr(b, "_reflow_blocked_property_ids")
                    except Exception:
                        pass
            for b in agent_map.values():
                if hasattr(b, "_recovery_queue_mode"):
                    setattr(b, "_recovery_queue_mode", False)
                if hasattr(b, "_recovery_round"):
                    setattr(b, "_recovery_round", 0)

        # Process settlements due this month after negotiations/reflow are recorded.
        # Skip settlement inside reflow pass; outer pass will settle once.
        settled_now = 0
        if not reflow_pass:
            settled_now = self._process_due_settlements(cursor, month, agent_map, props_map, market)
            transactions_count += settled_now

        # Flush buffered order logs once per month to keep I/O overhead low.
        self._flush_order_log_buffer()
        self.conn.commit()
        return transactions_count, failed_negotiations
