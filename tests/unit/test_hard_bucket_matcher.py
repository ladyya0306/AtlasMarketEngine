import sqlite3
import unittest

from services.transaction_service import TransactionService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}
        self.mortgage = {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestHardBucketMatcher(unittest.TestCase):
    def test_resolve_hard_bucket_whitelist(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE profiled_market_agent_buckets (
                agent_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE profiled_market_property_buckets (
                property_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL
            )
            """
        )
        cur.executemany(
            "INSERT INTO profiled_market_agent_buckets(agent_id,bucket_id) VALUES (?,?)",
            [(1, "B_SCHOOL_JUST_ENTRY")],
        )
        cur.executemany(
            "INSERT INTO profiled_market_property_buckets(property_id,bucket_id) VALUES (?,?)",
            [(101, "B_SCHOOL_JUST"), (102, "B_NOSCHOOL_JUST"), (201, "A_SCHOOL_IMPROVE")],
        )
        conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.profiled_market_mode": {
                    "enabled": True,
                    "hard_bucket_matcher_enabled": True,
                    "hard_bucket_strict_unmapped_property": True,
                    "profile_pack": {
                        "bucket_alignment_rules": [
                            {
                                "agent_bucket_id": "B_SCHOOL_JUST_ENTRY",
                                "eligible_property_buckets": ["B_SCHOOL_JUST"],
                                "soft_property_buckets": ["B_NOSCHOOL_JUST"],
                            }
                        ]
                    },
                }
            }
        )
        svc = TransactionService(cfg, conn)

        whitelist, meta = svc._resolve_hard_bucket_property_whitelist(cur, buyer_id=1)
        self.assertIsNotNone(whitelist)
        self.assertEqual(whitelist, {101})
        self.assertEqual(meta["buyer_bucket"], "B_SCHOOL_JUST_ENTRY")

    def test_missing_buyer_bucket_passthrough_when_not_required(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE profiled_market_property_buckets (property_id INTEGER PRIMARY KEY, bucket_id TEXT NOT NULL)"
        )
        conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.profiled_market_mode": {
                    "enabled": True,
                    "hard_bucket_matcher_enabled": True,
                    "hard_bucket_require_profiled_buyer": False,
                    "profile_pack": {"bucket_alignment_rules": []},
                }
            }
        )
        svc = TransactionService(cfg, conn)

        whitelist, meta = svc._resolve_hard_bucket_property_whitelist(cur, buyer_id=999)
        self.assertIsNone(whitelist)
        self.assertIn("passthrough", meta.get("reason", ""))

    def test_split_fallback_property_uses_canonical_target_for_whitelist(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE profiled_market_agent_buckets (
                agent_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE profiled_market_property_buckets (
                property_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                metadata_json TEXT
            )
            """
        )
        cur.execute(
            "INSERT INTO profiled_market_agent_buckets(agent_id,bucket_id) VALUES (?,?)",
            (1, "UPGRADE_YOUNG_COMFORT"),
        )
        cur.execute(
            "INSERT INTO profiled_market_property_buckets(property_id,bucket_id,metadata_json) VALUES (?,?,?)",
            (
                101,
                "FALLBACK_SUPPLY_CORE_NONSCHOOL_IMPROVE_AREA_ABOVE_PRICE_ABOVE",
                '{"canonical_target_bucket_id":"CORE_NONSCHOOL_IMPROVE","bucket_class":"fallback"}',
            ),
        )
        conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.profiled_market_mode": {
                    "enabled": True,
                    "hard_bucket_matcher_enabled": True,
                    "hard_bucket_strict_unmapped_property": True,
                    "profile_pack": {
                        "bucket_alignment_rules": [
                            {
                                "agent_bucket_id": "UPGRADE_YOUNG_COMFORT",
                                "eligible_property_buckets": ["CORE_NONSCHOOL_IMPROVE"],
                                "soft_property_buckets": [],
                            }
                        ]
                    },
                }
            }
        )
        svc = TransactionService(cfg, conn)

        whitelist, meta = svc._resolve_hard_bucket_property_whitelist(cur, buyer_id=1)
        self.assertEqual(whitelist, {101})
        self.assertEqual(meta["buyer_bucket"], "UPGRADE_YOUNG_COMFORT")


if __name__ == "__main__":
    unittest.main()
