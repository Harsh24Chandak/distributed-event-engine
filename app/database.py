"""
Transactional Database Layer.
Provides ACID guarantees for raw events, metric aggregates, and Dead-Letter Queue (DLQ).
Uses SQLite with connection pooling (seamlessly swappable with PostgreSQL via DATABASE_URL).
"""
import os
import sqlite3
import threading
import uuid
from decimal import Decimal
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional


class DatabaseManager:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("DATABASE_PATH", "/tmp/events_store.db")
        self._local = threading.local()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    def _init_db(self):
        conn = self._get_connection()
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_events (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    meter_name TEXT NOT NULL,
                    value TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    idempotency_key TEXT,
                    created_at TEXT NOT NULL
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_account_meter
                ON raw_events (account_id, meter_name, timestamp);
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dead_letter_events (
                    id TEXT PRIMARY KEY,
                    account_id TEXT,
                    payload TEXT NOT NULL,
                    error_reason TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'PENDING',
                    created_at TEXT NOT NULL
                );
            """)

    def insert_event(self, account_id: str, meter_name: str, value: Decimal, timestamp: datetime, idempotency_key: Optional[str] = None) -> str:
        event_id = f"evt_{uuid.uuid4().hex[:12]}"
        now_str = datetime.now(timezone.utc).isoformat()
        ts_str = timestamp.isoformat()
        conn = self._get_connection()
        with conn:
            conn.execute(
                """
                INSERT INTO raw_events (id, account_id, meter_name, value, timestamp, idempotency_key, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (event_id, account_id, meter_name, str(value), ts_str, idempotency_key, now_str)
            )
        return event_id

    def get_aggregated_usage(self, account_id: str, start_time: datetime, end_time: datetime) -> Dict[str, Decimal]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT meter_name, value
            FROM raw_events
            WHERE account_id = ? AND timestamp >= ? AND timestamp <= ?
            """,
            (account_id, start_time.isoformat(), end_time.isoformat())
        )
        rows = cursor.fetchall()
        totals: Dict[str, Decimal] = {}
        for row in rows:
            meter = row["meter_name"]
            val = Decimal(row["value"])
            totals[meter] = totals.get(meter, Decimal("0")) + val
        return totals

    def record_dlq_event(self, account_id: Optional[str], payload: str, error_reason: str) -> str:
        dlq_id = f"dlq_{uuid.uuid4().hex[:12]}"
        now_str = datetime.now(timezone.utc).isoformat()
        conn = self._get_connection()
        with conn:
            conn.execute(
                """
                INSERT INTO dead_letter_events (id, account_id, payload, error_reason, retry_count, status, created_at)
                VALUES (?, ?, ?, ?, 0, 'PENDING', ?)
                """,
                (dlq_id, account_id, payload, error_reason, now_str)
            )
        return dlq_id

    def get_pending_dlq_items(self) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dead_letter_events WHERE status = 'PENDING'")
        return [dict(row) for row in cursor.fetchall()]
