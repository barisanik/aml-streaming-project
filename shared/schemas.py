"""    
# ============================================================================ #
    Script: Schemas
# ============================================================================ #
    Purpose: Includes shared pydantic schemas and category enums for the AML/Fraud platform to keep single source of truth and avoid conflicts.    
    Rules: 
    - All enum values are lowercase.
# ============================================================================ #
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


#-- Category: Enums

# Transaction Type
class TxnType(str, Enum):
    TRANSFER_IN = "transfer_in"
    TRANSFER_OUT = "transfer_out"
    CARD_PAYMENT = "card_payment"
    CASH_DEPOSIT = "cash_deposit"
    CASH_WITHDRAWAL = "cash_withdrawal"

# Transaction Channel
class Channel(str, Enum):
    MOBILE = "mobile"
    WEB = "web"
    ATM = "atm"
    BRANCH = "branch"

# Fraud Scenario
class ScenarioType(str, Enum):
    STRUCTURING = "structuring"
    SMURFING = "smurfing"
    MULE_FAN_IN = "mule_fan_in"
    ACCOUNT_TAKEOVER = "account_takeover"
    DORMANT_ACTIVATION = "dormant_activation"

# Severity
class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

# Income Segment of Customer
class IncomeSegment(str, Enum):
    """Income bracket used to parametrize a profile's typical transaction size."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

# Customer Entity Type
class EntitySegment(str, Enum):
    """Customer entity type. Thresholds are looked up per segment."""
    INDIVIDUAL = "individual"


#-- Category: Core message schemas

class Transaction(BaseModel):
    """A single bank transaction event, produced to the transactions topic."""

    transaction_id: str
    account_id: str
    counterparty_id: str
    amount: Decimal
    currency: str
    txn_type: TxnType
    merchant_category: Optional[str] = None
    channel: Channel
    city: str
    country: str
    event_time: datetime  # simulated clock (UTC), used for rule windows
    produced_at: datetime  # real wall-clock (UTC), used for latency measurement
    device_id: Optional[str] = None


class Alert(BaseModel):
    """A rule-engine alert, written to Postgres and to the alerts topic."""

    alert_id: str
    transaction_id: str
    account_id: str
    rule_id: str
    rule_name: str
    severity: Severity
    window_summary: dict = Field(
        description="Explainability payload: window counters/totals that triggered the rule."
    )
    event_time: datetime
    alert_time: datetime  # wall-clock
    detection_latency_ms: int  # alert_time - produced_at, in milliseconds


class AnswerKey(BaseModel):
    """Ground-truth label for an injected fraud/AML scenario transaction. Written directly to Postgres by the producer; never sent through the transactions topic."""

    transaction_id: str
    scenario_id: str
    scenario_type: ScenarioType
    injected_at: datetime


class ConsumerHeartbeat(BaseModel):
    """Periodic consumer progress record, feeding the Grafana lag panel."""

    consumer_group: str
    topic: str
    partition: int
    committed_offset: int
    messages_processed: int
    heartbeat_at: datetime  # wall-clock


class Profile(BaseModel):
    """A synthetic customer profile, generated once by profile_gen.py.

    Read by both the producer (to shape realistic transaction generation) and the detection side (profile-deviation rules reuse the same fields, e.g. active hours, average amount, home city).
    """

    account_id: str
    home_city: str
    home_country: str
    income_segment: IncomeSegment
    avg_amount_mu: float  # lognormal location parameter for this account's transaction amounts
    avg_amount_sigma: float  # lognormal scale parameter
    active_hour_start: int  # 0-23, inclusive
    active_hour_end: int  # 0-23, inclusive
    weekly_txn_frequency: float
    preferred_merchant_categories: list[str]
    last_activity_at: datetime  # used to seed dormant-account scenarios
    segment: EntitySegment = EntitySegment.INDIVIDUAL