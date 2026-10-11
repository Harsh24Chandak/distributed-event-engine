"""
Distributed Event & Metering Engine API.
Main FastAPI server handling high-throughput event ingestion and billing calculations.
"""
import uuid
from decimal import Decimal
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.responses import JSONResponse

from app.models import (
    EventIngestRequest,
    EventIngestResponse,
    InvoiceGenerateRequest,
    InvoiceResponse,
    InvoiceLineItem,
)
from app.idempotency import IdempotencyManager, TokenBucketRateLimiter
from app.database import DatabaseManager
from app.pricing_engine import GraduatedPricingCalculator

app = FastAPI(
    title="MeterPulse - Distributed Usage-Based Metering & Billing Engine",
    description="High-throughput event ingestion API with Redis-backed idempotency, rate limiting, and graduated billing calculations.",
    version="1.0.0",
)

# Global in-memory components (swappable with Redis/PostgreSQL via env in production)
idempotency_mgr = IdempotencyManager(default_ttl_seconds=86400)
rate_limiter = TokenBucketRateLimiter(capacity=100, refill_rate_per_sec=25.0)
db_mgr = DatabaseManager()


@app.get("/health", tags=["System"])
def health_check():
    return {"status": "healthy", "service": "MeterPulse Event Engine", "version": "1.0.0"}


@app.post(
    "/api/v1/events",
    response_model=EventIngestResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Ingestion"],
)
def ingest_event(
    event: EventIngestRequest,
    x_idempotency_key: Optional[str] = Header(None, description="Client-provided idempotency key for deduplication"),
):
    # 1. Rate Limiting Check
    if not rate_limiter.allow_request(event.account_id):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded for account: {event.account_id}",
        )

    # 2. Idempotency Deduplication Check
    if x_idempotency_key:
        is_new, cached_result = idempotency_mgr.acquire_or_get(
            key=f"{event.account_id}:{x_idempotency_key}",
            value={"meter_name": event.meter_name, "value": str(event.value)},
            ttl=86400,
        )
        if not is_new:
            # Return cached acknowledgment with 200 OK
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "status": "DEDUPLICATED",
                    "event_id": "cached",
                    "account_id": event.account_id,
                    "meter_name": event.meter_name,
                    "value": str(event.value),
                    "is_duplicate": True,
                    "message": "Duplicate event suppressed via idempotency key.",
                },
            )

    # 3. Insert into Database
    try:
        event_id = db_mgr.insert_event(
            account_id=event.account_id,
            meter_name=event.meter_name,
            value=event.value,
            timestamp=event.timestamp,
            idempotency_key=x_idempotency_key,
        )
    except Exception as e:
        payload_str = event.model_dump_json() if hasattr(event, "model_dump_json") else event.json()
        db_mgr.record_dlq_event(
            account_id=event.account_id,
            payload=payload_str,
            error_reason=str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist event; routed to dead-letter queue.",
        )

    return EventIngestResponse(
        status="ACCEPTED",
        event_id=event_id,
        account_id=event.account_id,
        meter_name=event.meter_name,
        value=event.value,
        is_duplicate=False,
        message="Usage event successfully ingested and indexed.",
    )


@app.post(
    "/api/v1/invoices/generate",
    response_model=InvoiceResponse,
    status_code=status.HTTP_200_OK,
    tags=["Billing"],
)
def generate_invoice(request: InvoiceGenerateRequest):
    usage_totals = db_mgr.get_aggregated_usage(
        account_id=request.account_id,
        start_time=request.period_start,
        end_time=request.period_end,
    )

    line_items = []
    total_invoice_amount = Decimal("0.00")

    for meter_name, total_units in usage_totals.items():
        subtotal, breakdown = GraduatedPricingCalculator.calculate_meter_cost(meter_name, total_units)
        line_items.append(
            InvoiceLineItem(
                meter_name=meter_name,
                total_units=total_units,
                tiered_breakdown=breakdown,
                subtotal=subtotal,
            )
        )
        total_invoice_amount += subtotal

    return InvoiceResponse(
        invoice_id=f"inv_{uuid.uuid4().hex[:12]}",
        account_id=request.account_id,
        billing_period={
            "start": request.period_start.isoformat(),
            "end": request.period_end.isoformat(),
        },
        line_items=line_items,
        total_amount=total_invoice_amount,
        currency="USD",
    )


@app.get("/api/v1/dlq", tags=["Reliability"])
def list_dlq_items():
    return {"dead_letter_items": db_mgr.get_pending_dlq_items()}
