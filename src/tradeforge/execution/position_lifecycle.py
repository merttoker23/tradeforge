"""Independent exchange reconciliation, durable exposure health and protection repair."""

import json
import time
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from threading import RLock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from tradeforge.domain.common import utc
from tradeforge.domain.execution import ExecutionIntent
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.execution.account_sync import AccountView, ExchangePosition, read_account
from tradeforge.execution.binance_client import BinanceTestnetClient
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.diagnostics_transport import DiagnosticClient, diagnostic_client
from tradeforge.execution.emergency import emergency_close
from tradeforge.execution.protective_orders import ProtectiveOrders, matches
from tradeforge.execution.storage import audit, intent_from, persist_report
from tradeforge.persistence.db import Database, EntryHalt, PersistenceError
from tradeforge.persistence.repositories import add_instrument
from tradeforge.risk.storage import RiskService

RECOVERY_LOCK = 84120515


@dataclass(frozen=True)
class ProtectionProof:
    source_at: datetime | None
    reasons: frozenset[str]
    exposure: tuple[tuple[str, tuple[Decimal, datetime]], ...]


class PositionLifecycle:
    def __init__(
        self,
        db: Database,
        client: BinanceTestnetClient,
        *,
        clock: Callable[[], datetime] | None = None,
        latch_failure: bool = True,
    ) -> None:
        self.db, self.client = db, client
        self.clock = clock or (lambda: datetime.now(UTC))
        self.latch_failure = latch_failure
        self.protection = ProtectiveOrders(db, client)
        self.lock = RLock()
        self.last_success: datetime | None = None
        self.reasons = {"STARTUP_RECONCILIATION_REQUIRED"}
        self.exposure: dict[str, tuple[Decimal, datetime]] = {}
        self._in_progress = False
        self._started_at: datetime | None = None
        self._completed_at: datetime | None = None
        self._duration_ms: float | None = None
        self.account_requests: list[dict[str, object]] = []
        self.last_failure: str | None = None
        with db.transaction() as c:
            prior = c.scalar(
                text(
                    "SELECT payload FROM audit_log "
                    "WHERE event_type='protection.health.v1' "
                    "ORDER BY audit_id DESC LIMIT 1"
                )
            )
        if prior is not None:
            try:
                self.exposure = {
                    x["symbol"]: (Decimal(x["qty"]), utc(datetime.fromisoformat(x["since"])))
                    for x in prior["unprotected"]
                }
            except (KeyError, TypeError, ValueError):
                raise EntryHalt("PROTECTION_HEALTH_CORRUPT") from None
        self._publish()

    def _publish(self) -> None:
        # Readers never take the network/side-effect lock or see partially reset proof.
        self._proof = ProtectionProof(
            self.last_success, frozenset(self.reasons), tuple(sorted(self.exposure.items()))
        )

    def _reject(self, reason: str) -> None:
        self.reasons.add(reason)
        self.last_success = None
        self._publish()

    @property
    def ready(self) -> bool:
        proof = self._proof
        return bool(
            proof.source_at is not None
            and not proof.reasons
            and 0 <= (utc(self.clock()) - proof.source_at).total_seconds() <= 15
        )

    def health(self) -> dict[str, object]:
        at = utc(self.clock())
        proof = self._proof
        age = (at - proof.source_at).total_seconds() if proof.source_at else None
        ready = age is not None and 0 <= age <= 15 and not proof.reasons
        return {
            "ready": ready,
            "reconciled_at": proof.source_at.isoformat() if proof.source_at else None,
            "reconciled_age_seconds": age,
            "reconciliation_in_progress": self._in_progress,
            "last_reconciliation_started_at": (
                self._started_at.isoformat() if self._started_at else None
            ),
            "last_reconciliation_completed_at": (
                self._completed_at.isoformat() if self._completed_at else None
            ),
            "last_reconciliation_duration_ms": self._duration_ms,
            "account_requests": list(self.account_requests),
            "last_failure": self.last_failure,
            "reasons": sorted(
                proof.reasons
                if proof.reasons
                else (set() if ready else {"ACCOUNT_RECONCILIATION_STALE"})
            ),
            "unprotected": [
                {
                    "symbol": symbol,
                    "qty": str(qty),
                    "since": since.isoformat(),
                    "seconds": max(0.0, (at - since).total_seconds()),
                }
                for symbol, (qty, since) in proof.exposure
            ],
        }

    def invalidate(self) -> None:
        with self.lock:
            self._reject("PRIVATE_STREAM_GAP")
            RiskService(self.db).set_kill_switch(True, reason="PRIVATE_STREAM_GAP")

    def reconcile(self) -> None:
        with self.lock:
            started = time.monotonic()
            self._in_progress = True
            self._started_at = utc(self.clock())
            self.reasons = set()
            self.account_requests = []
            self.last_failure = None
            try:
                # Session lock serializes side effects across guardian processes,
                # without holding a database transaction open during network I/O.
                with self.db.sql_engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as lease:
                    if not lease.scalar(
                        text("SELECT pg_try_advisory_lock(:id)"), {"id": RECOVERY_LOCK}
                    ):
                        self._reject("RECOVERY_BUSY")
                    else:
                        try:
                            scope = (
                                diagnostic_client(self.client)
                                if isinstance(self.client, BinanceTestnetClient)
                                else nullcontext(self.client)
                            )
                            with scope as reader:
                                try:
                                    self._reconcile(reader)
                                finally:
                                    if isinstance(reader, DiagnosticClient):
                                        self.account_requests = list(reader.account_requests)
                        finally:
                            lease.execute(
                                text("SELECT pg_advisory_unlock(:id)"), {"id": RECOVERY_LOCK}
                            )
            except (
                AckUnknown,
                ExchangeRejected,
                PersistenceError,
                ValueError,
                SQLAlchemyError,
            ) as error:
                from tradeforge.runtime.progress import safe_failure

                self.last_failure = safe_failure(error)
                self._reject("ACCOUNT_RECONCILIATION_UNAVAILABLE")
            except Exception:
                self._reject("ACCOUNT_RECONCILIATION_UNAVAILABLE")
                raise
            finally:
                self._duration_ms = round((time.monotonic() - started) * 1000, 3)
                self._completed_at = utc(self.clock())
                self._in_progress = False
                self._publish()
            if self.reasons and self.latch_failure:
                RiskService(self.db).set_kill_switch(True, reason="ACCOUNT_PROTECTION_UNSAFE")
            with self.db.transaction() as c:
                audit(c, "protection.health.v1", self.health())

    def _orders(self, at: datetime) -> tuple[ExecutionIntent, ...]:
        with self.db.transaction() as c:
            identifiers = (
                c.execute(
                    text(
                        "SELECT intent_id FROM orders o WHERE (status IN "
                        "('NEW','ACK_UNKNOWN','ACCEPTED','PARTIALLY_FILLED','UNKNOWN') "
                        "OR filled_qty>0) "
                        "AND NOT EXISTS (SELECT 1 FROM audit_log a "
                        "WHERE a.event_type='protection.flat.v1' "
                        "AND a.payload->>'intent_id'=o.intent_id::text) ORDER BY updated_at"
                    )
                )
                .scalars()
                .all()
            )
            intents = tuple(intent_from(c, x) for x in identifiers)
        for intent in intents:
            try:
                report = self.client.query(intent)
                if report is None:
                    raise EntryHalt("ORDER_UNKNOWN")
                with self.db.transaction() as c:
                    persist_report(c, intent, report, utc(self.clock()))
                with self.db.transaction() as c:
                    pending_exit = c.scalar(
                        text(
                            "SELECT 1 FROM audit_log "
                            "WHERE event_type='protection.exit_reserved.v1' "
                            "AND payload->>'client_id'=:key LIMIT 1"
                        ),
                        {"key": "ec_" + intent.intent_id.hex},
                    )
                if pending_exit:
                    emergency_close(self.db, self.client, intent, at, clock=self.clock)
            except (AckUnknown, ExchangeRejected, PersistenceError, ValueError):
                self._reject("ORDER_RECONCILIATION_UNKNOWN")
        return intents

    def _expected(self, intent: ExecutionIntent) -> Decimal:
        with self.db.transaction() as c:
            filled = c.scalar(
                text("SELECT filled_qty FROM orders WHERE intent_id=:id"), {"id": intent.intent_id}
            )
            exit_body = c.scalar(
                text(
                    "SELECT payload FROM audit_log "
                    "WHERE event_type='protection.exit_observed.v1' "
                    "AND payload->>'client_id'=:key ORDER BY audit_id DESC LIMIT 1"
                ),
                {"key": "ec_" + intent.intent_id.hex},
            )
        return Decimal(filled) - (Decimal(exit_body["filled_qty"]) if exit_body else Decimal(0))

    def _position(self, position: ExchangePosition, at: datetime) -> UUID:
        key = InstrumentKey(
            symbol=position.symbol,
            exchange="BINANCE",
            market_type="USD_M",
            quote="USDT",
            margin_asset="USDT",
            contract_type="PERPETUAL",
        )
        with self.db.transaction() as c:
            instrument = add_instrument(c, key)
            identifier = uuid5(NAMESPACE_URL, f"tradeforge/position/{instrument}/{position.side}")
            c.execute(
                text(
                    "INSERT INTO positions "
                    "(position_id,instrument_id,side,qty,entry_price,mark_price,as_of) "
                    "VALUES (:id,:instrument,:side,:qty,:entry,:mark,:at) "
                    "ON CONFLICT (position_id) DO UPDATE SET qty=:qty,entry_price=:entry,"
                    "mark_price=:mark,protected_qty=0,version=positions.version+1,as_of=:at"
                ),
                {
                    "id": identifier,
                    "instrument": instrument,
                    "side": position.side,
                    "qty": abs(position.amount),
                    "entry": position.entry,
                    "mark": position.mark,
                    "at": at,
                },
            )
            audit(
                c,
                "protection.position.v1",
                {
                    "symbol": position.symbol,
                    "side": position.side,
                    "qty": str(abs(position.amount)),
                    "entry": str(position.entry),
                    "mark": str(position.mark),
                    "margin": str(position.margin),
                    "as_of": at.isoformat(),
                },
            )
        return identifier

    def _reconcile(self, reader: BinanceTestnetClient) -> None:
        started = time.monotonic()
        at = utc(self.clock())
        intents = self._orders(at)
        view = read_account(reader, at)
        owned = {i.symbol: i for i in intents}
        with self.db.transaction() as c:
            exit_keys = set(
                c.execute(
                    text(
                        "SELECT payload->>'client_id' FROM audit_log "
                        "WHERE event_type='protection.exit_reserved.v1'"
                    )
                ).scalars()
            )
        if any(
            o.get("clientOrderId") not in {i.client_order_id for i in intents}
            and o.get("clientOrderId") not in exit_keys
            for o in view.orders
        ):
            self._reject("UNKNOWN_EXCHANGE_ORDER")
        active: set[UUID] = set()
        uncovered = {
            p.symbol: (abs(p.amount), self.exposure.get(p.symbol, (Decimal(0), at))[1])
            for p in view.positions
        }
        self.exposure = uncovered
        for position in view.positions:
            identifier = self._position(position, at)
            active.add(identifier)
            intent = owned.get(position.symbol)
            since = self.exposure.get(position.symbol, (Decimal(0), at))[1]
            uncovered[position.symbol] = (abs(position.amount), since)
            if intent is None or intent.side.value != position.side:
                self._reject("POSITION_MISMATCH")
                continue
            with self.db.transaction() as c:
                decision = c.scalar(
                    text("SELECT snapshot FROM decisions WHERE decision_id=:id"),
                    {"id": intent.decision_id},
                )
            if self._expected(intent) != abs(position.amount):
                self._reject("POSITION_MISMATCH")
                continue
            try:
                if intent.stop_plan is None:
                    raise EntryHalt("STOP_PLAN_MISSING")
                self.protection.ensure(
                    identifier, position, intent, intent.stop_plan.stop_price, "STOP_MARKET", at
                )
                # Stop is independently useful even if the optional TP cannot be established.
                with self.db.transaction() as c:
                    c.execute(
                        text(
                            "UPDATE positions SET protected_qty=qty,version=version+1 "
                            "WHERE position_id=:id"
                        ),
                        {"id": identifier},
                    )
                uncovered.pop(position.symbol)
                target = json.loads(decision).get("take_profit_proposal") if decision else None
                if target is not None:
                    self.protection.ensure(
                        identifier, position, intent, Decimal(target), "TAKE_PROFIT_MARKET", at
                    )
            except (AckUnknown, ExchangeRejected, EntryHalt, ValueError):
                self._reject("PROTECTION_FAILED")
                RiskService(self.db).set_kill_switch(True, reason="PROTECTION_FAILED")
                try:
                    if emergency_close(self.db, self.client, intent, at, clock=self.clock):
                        uncovered.pop(position.symbol, None)
                except (AckUnknown, ExchangeRejected, EntryHalt, ValueError):
                    self._reject("EMERGENCY_CLOSE_UNRESOLVED")
        self.exposure = uncovered
        self.protection.retire(active, at)
        # A second authoritative read catches fills/closures during protection work.
        final = read_account(reader, at)
        self._confirm(view, final, active, at)
        if time.monotonic() - started > 10:
            self._reject("ACCOUNT_RECONCILIATION_STALE")
        # Account reads describe this source interval, not a newly observed account
        # at the end of network and database work. Never renew proof by finishing late.
        self.last_success = at if not self.reasons else None

    def _confirm(
        self, before: AccountView, final: AccountView, active: set[UUID], at: datetime
    ) -> None:
        for position in final.positions:
            previous = next((p for p in before.positions if p.symbol == position.symbol), None)
            if previous is None or previous.amount != position.amount:
                self._reject("ACCOUNT_CHANGED_DURING_RECONCILIATION")
                self._position(position, at)
                self.exposure[position.symbol] = (
                    abs(position.amount),
                    self.exposure.get(position.symbol, (Decimal(0), at))[1],
                )
            with self.db.transaction() as c:
                saved_stops = c.execute(
                    text(
                        "SELECT p.position_id,s.client_order_id,s.stop_price "
                        "FROM positions p JOIN instruments i USING(instrument_id) "
                        "JOIN protective_orders s USING(position_id) WHERE i.symbol=:symbol "
                        "AND p.side=:side AND s.status='NEW'"
                    ),
                    {"symbol": position.symbol, "side": position.side},
                ).all()
            protected = any(
                matches(algo, position, price, "STOP_MARKET", key)
                for _, key, price in saved_stops
                if key.startswith("sl_")
                for algo in final.algos
            )
            targets = [(key, price) for _, key, price in saved_stops if key.startswith("tp_")]
            if targets and not any(
                matches(algo, position, price, "TAKE_PROFIT_MARKET", key)
                for key, price in targets
                for algo in final.algos
            ):
                self._reject("TAKE_PROFIT_MISSING")
            if not protected:
                self._reject("PROTECTION_MISSING")
                self.exposure[position.symbol] = (
                    abs(position.amount),
                    self.exposure.get(position.symbol, (Decimal(0), at))[1],
                )
                with self.db.transaction() as c:
                    c.execute(
                        text(
                            "UPDATE positions SET protected_qty=0,version=version+1 "
                            "WHERE position_id IN (SELECT p.position_id FROM positions p "
                            "JOIN instruments i USING(instrument_id) WHERE i.symbol=:symbol)"
                        ),
                        {"symbol": position.symbol},
                    )
        remaining = {p.symbol for p in final.positions}
        remaining_sides = {(p.symbol, p.side) for p in final.positions}
        self.exposure = {k: v for k, v in self.exposure.items() if k in remaining}
        with self.db.transaction() as c:
            saved = c.execute(
                text(
                    "SELECT p.position_id,i.symbol,p.side FROM positions p "
                    "JOIN instruments i USING(instrument_id) WHERE p.qty>0"
                )
            ).all()
            for identifier, symbol, side in saved:
                if (symbol, side) not in remaining_sides:
                    c.execute(
                        text(
                            "UPDATE positions SET qty=0,protected_qty=0,version=version+1,"
                            "as_of=:at WHERE position_id=:id"
                        ),
                        {"id": identifier, "at": at},
                    )
                    active.discard(identifier)
        self.protection.retire(active, at)
        known_algos: set[str]
        with self.db.transaction() as c:
            known_algos = set(
                c.execute(text("SELECT client_order_id FROM protective_orders")).scalars()
            )
            c.execute(
                text(
                    "INSERT INTO account_snapshots(snapshot_id,as_of,payload) "
                    "VALUES (:id,:at,CAST(:body AS jsonb))"
                ),
                {
                    "id": uuid4(),
                    "at": at,
                    "body": json.dumps(
                        {
                            "kind": "guardian.account.v1",
                            "symbols": sorted(remaining),
                            "orders": len(final.orders),
                        }
                    ),
                },
            )
        if any(x.get("clientAlgoId") not in known_algos for x in final.algos):
            self._reject("UNKNOWN_PROTECTIVE_ORDER")
        with self.db.transaction() as c:
            known_orders = set(c.execute(text("SELECT client_order_id FROM orders")).scalars())
            known_exits = set(
                c.execute(
                    text(
                        "SELECT payload->>'client_id' FROM audit_log "
                        "WHERE event_type='protection.exit_reserved.v1'"
                    )
                ).scalars()
            )
        if any(x.get("clientOrderId") not in known_orders | known_exits for x in final.orders):
            self._reject("UNKNOWN_EXCHANGE_ORDER")
        if not self.reasons:
            with self.db.transaction() as c:
                terminal = c.execute(
                    text(
                        "SELECT o.intent_id,i.snapshot->>'symbol' AS symbol "
                        "FROM orders o JOIN execution_intents i USING(intent_id) "
                        "WHERE o.status IN ('FILLED','CANCELED','EXPIRED') AND o.filled_qty>0 "
                        "AND NOT EXISTS (SELECT 1 FROM audit_log a "
                        "WHERE a.event_type='protection.flat.v1' "
                        "AND a.payload->>'intent_id'=o.intent_id::text)"
                    )
                ).all()
                for identifier, symbol in terminal:
                    if symbol not in remaining and not any(
                        x.get("symbol") == symbol for x in final.orders
                    ):
                        audit(
                            c,
                            "protection.flat.v1",
                            {
                                "intent_id": str(identifier),
                                "symbol": symbol,
                                "as_of": at.isoformat(),
                            },
                        )
