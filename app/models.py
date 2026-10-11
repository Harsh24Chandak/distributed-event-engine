"""
Data models and schemas for Distributed Event Engine (MeterPulse).
Universal compatibility across Pydantic v1 and Pydantic v2 with exact Decimal financial arithmetic.
"""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, Optional, List
import pydantic
from pydantic import BaseModel, Field

if hasattr(pydantic, "field_validator"):
    from pydantic import field_validator
    def validate_fields(*fields):
        return field_validator(*fields)
else:
    from pydantic import validator
    def validate_fields(*fields):
        return validator(*fields)


class EventIngestRequest(BaseModel):
    account_id: str = Field(..., description="Unique customer/tenant account identifier", min_length=3, max_length=64)
    meter_name: str = Field(..., description="Metric being metered (e.g. api_calls, compute_seconds, storage_gb)", min_length=2, max_length=64)
    value: Decimal = Field(..., description="Quantitative metric value consumed (must be positive)", gt=0)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp of the event in UTC")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Arbitrary event metadata")

    @validate_fields("account_id", "meter_name")
    @classmethod
    def clean_identifier(cls, v: str) -> str:
        return v.strip().lower()


class EventIngestResponse(BaseModel):
    status: str
    event_id: str
    account_id: str
    meter_name: str
    value: Decimal
    is_duplicate: bool = False
    message: str


class PricingTier(BaseModel):
    tier_name: str
    up_to: Optional[Decimal] = Field(None, description="Upper volume bound. None denotes unbounded/infinity.")
    unit_price: Decimal = Field(..., description="Cost per unit in USD", ge=0)


class InvoiceGenerateRequest(BaseModel):
    account_id: str
    period_start: datetime
    period_end: datetime


class InvoiceLineItem(BaseModel):
    meter_name: str
    total_units: Decimal
    tiered_breakdown: List[Dict[str, Any]]
    subtotal: Decimal


class InvoiceResponse(BaseModel):
    invoice_id: str
    account_id: str
    billing_period: Dict[str, str]
    line_items: List[InvoiceLineItem]
    total_amount: Decimal
    currency: str = "USD"
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
