"""Core table handles for critical writes; migrations own the complete frozen schema."""

from sqlalchemy import BigInteger, Column, DateTime, MetaData, Numeric, String, Table, Uuid
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()
instruments = Table(
    "instruments",
    metadata,
    Column("instrument_id", Uuid, primary_key=True),
    Column("exchange", String),
    Column("market_type", String),
    Column("symbol", String),
    Column("contract_type", String),
    Column("observed_at", DateTime(timezone=True)),
)
risk_snapshots = Table(
    "risk_snapshots",
    metadata,
    Column("snapshot_id", Uuid, primary_key=True),
    Column("as_of", DateTime(timezone=True)),
    Column("payload", JSONB),
)
decisions = Table(
    "decisions",
    metadata,
    Column("decision_id", Uuid, primary_key=True),
    Column("instrument_id", Uuid),
    Column("side", String),
    Column("state", String),
    Column("observed_at", DateTime(timezone=True)),
    Column("expires_at", DateTime(timezone=True)),
    Column("snapshot", String),
    Column("snapshot_hash", String),
)
decision_reasons = Table(
    "decision_reasons",
    metadata,
    Column("decision_id", Uuid),
    Column("reason_code", String),
)
execution_intents = Table(
    "execution_intents",
    metadata,
    Column("intent_id", Uuid, primary_key=True),
    Column("client_order_id", String),
    Column("decision_id", Uuid),
    Column("instrument_id", Uuid),
    Column("risk_snapshot_id", Uuid),
    Column("side", String),
    Column("qty", Numeric),
    Column("price", Numeric),
    Column("created_at", DateTime(timezone=True)),
    Column("expires_at", DateTime(timezone=True)),
    Column("snapshot", JSONB),
)
outbox = Table(
    "outbox",
    metadata,
    Column("message_id", Uuid, primary_key=True),
    Column("intent_id", Uuid),
    Column("topic", String),
    Column("payload", JSONB),
    Column("created_at", DateTime(timezone=True)),
    Column("delivered_at", DateTime(timezone=True)),
    Column("version", BigInteger),
)
orders = Table(
    "orders",
    metadata,
    Column("intent_id", Uuid, primary_key=True),
    Column("status", String),
    Column("version", BigInteger),
    Column("updated_at", DateTime(timezone=True)),
)
audit_log = Table(
    "audit_log",
    metadata,
    Column("event_type", String),
    Column("payload", JSONB),
)
consumer_receipts = Table(
    "consumer_receipts",
    metadata,
    Column("consumer", String, primary_key=True),
    Column("message_id", Uuid, primary_key=True),
)
