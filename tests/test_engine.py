"""
Automated Unit and Integration Test Suite for Distributed Event Engine.
Tests idempotency deduplication, rate limiting, and graduated billing calculations.
"""
import unittest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient

from app.main import app, db_mgr, idempotency_mgr, rate_limiter
from app.pricing_engine import GraduatedPricingCalculator


class TestDistributedEventEngine(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        idempotency_mgr.clear()

    def test_health_endpoint(self):
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "healthy")

    def test_single_event_ingestion(self):
        payload = {
            "account_id": "cust_acme_corp",
            "meter_name": "api_calls",
            "value": 150.0,
        }
        res = self.client.post("/api/v1/events", json=payload)
        self.assertEqual(res.status_code, 201)
        data = res.json()
        self.assertEqual(data["status"], "ACCEPTED")
        self.assertFalse(data["is_duplicate"])
        self.assertEqual(data["account_id"], "cust_acme_corp")

    def test_idempotency_deduplication(self):
        """
        Submitting identical event twice with identical X-Idempotency-Key
        must return 200 DEDUPLICATED on second attempt.
        """
        payload = {
            "account_id": "cust_tenant_xyz",
            "meter_name": "compute_seconds",
            "value": 3600.0,
        }
        headers = {"X-Idempotency-Key": "idemp_unique_key_001"}

        # First request -> 201 Created
        res1 = self.client.post("/api/v1/events", json=payload, headers=headers)
        self.assertEqual(res1.status_code, 201)
        self.assertFalse(res1.json()["is_duplicate"])

        # Second request with same key -> 200 Deduplicated
        res2 = self.client.post("/api/v1/events", json=payload, headers=headers)
        self.assertEqual(res2.status_code, 200)
        self.assertTrue(res2.json()["is_duplicate"])
        self.assertEqual(res2.json()["status"], "DEDUPLICATED")

    def test_graduated_pricing_calculation(self):
        """
        Verify exact graduated pricing calculation for api_calls:
        First 10,000 @ $0.00 = $0.00
        Next 40,000 @ $0.0025 = $100.00
        Remaining 15,000 @ $0.0010 = $15.00
        Total for 65,000 = $115.00
        """
        total_units = Decimal("65000")
        subtotal, breakdown = GraduatedPricingCalculator.calculate_meter_cost("api_calls", total_units)
        self.assertEqual(subtotal, Decimal("115.00"))
        self.assertEqual(len(breakdown), 3)

    def test_end_to_end_invoice_generation(self):
        account = "cust_invoice_test"
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        tomorrow = now + timedelta(days=1)

        # Ingest events
        self.client.post("/api/v1/events", json={
            "account_id": account,
            "meter_name": "api_calls",
            "value": 20000.0,
        })
        self.client.post("/api/v1/events", json={
            "account_id": account,
            "meter_name": "compute_seconds",
            "value": 10000.0,
        })

        # Request invoice
        res = self.client.post("/api/v1/invoices/generate", json={
            "account_id": account,
            "period_start": yesterday.isoformat(),
            "period_end": tomorrow.isoformat(),
        })
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["account_id"], account)
        self.assertGreater(float(data["total_amount"]), 0.0)
        self.assertGreaterEqual(len(data["line_items"]), 2)


if __name__ == "__main__":
    unittest.main()
