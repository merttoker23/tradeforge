"""One portfolio policy, independently evaluated by analyst and guardian."""

from datetime import datetime
from typing import Literal

from tradeforge.domain.common import StrictModel, Timestamp, utc
from tradeforge.domain.decision import Side
from tradeforge.intelligence.models import digest
from tradeforge.risk.cluster import clusters
from tradeforge.risk.exposure import PortfolioExposure, exposure
from tradeforge.risk.kill_switch import RiskState
from tradeforge.risk.limits import RiskLimits
from tradeforge.risk.models import AccountSnapshot, EntryRequest
from tradeforge.risk.position_sizer import SizedEntry, size_entry


class RiskAssessment(StrictModel):
    at: Timestamp
    stage: Literal["quant", "guardian"] = "quant"
    policy: RiskLimits
    request: EntryRequest
    account: AccountSnapshot
    state: RiskState
    sized: SizedEntry | None
    portfolio: PortfolioExposure | None
    reason_codes: tuple[str, ...]

    @property
    def allowed(self) -> bool:
        return not self.reason_codes and self.sized is not None and self.portfolio is not None

    @property
    def can_reduce_risk(self) -> Literal[True]:
        return True

    @property
    def live_approval(self) -> Literal[False]:
        return False

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class RiskGovernor:
    def __init__(self, policy: RiskLimits | None = None) -> None:
        self.policy = policy or RiskLimits()

    def assess(
        self,
        request: EntryRequest,
        account: AccountSnapshot,
        state: RiskState,
        *,
        at: datetime,
        db_writable: bool = False,
        alpha_score: float | None = None,
    ) -> RiskAssessment:
        # Alpha evidence cannot change any risk cap. It is deliberately not used.
        utc(at)
        p, a = self.policy, account
        reasons: list[str] = []
        if not db_writable:
            reasons.append("DB_UNAVAILABLE")
        if state.as_of > at:
            reasons.append("FUTURE_RISK_STATE")
        if state.kill_switch:
            reasons.append("KILL_SWITCH")
        if state.consecutive_losses >= p.loss_streak_limit:
            reasons.append("LOSS_STREAK_LIMIT")
        if state.cooldown_until is not None and at < state.cooldown_until:
            reasons.append("COOLDOWN")
        if state.daily_loss_day == at.date():
            reasons.append("DAILY_LOSS_LIMIT")
        if not 0 <= (at - a.as_of).total_seconds() <= p.account_age_seconds:
            reasons.append("ACCOUNT_STALE")
        if a.trading_day != at.date():
            reasons.append("UTC_DAY_MISMATCH")
        if not a.positions_complete:
            reasons.append("ACCOUNT_INCOMPLETE")
        if not a.protection_ready:
            reasons.append("PROTECTION_NOT_READY")
        if (
            a.equity is None
            or a.available_balance is None
            or a.day_start_equity is None
            or a.realized_pnl_today is None
            or a.unrealized_pnl is None
        ):
            reasons.append("ACCOUNT_UNKNOWN")
        elif a.realized_pnl_today + a.unrealized_pnl <= -a.day_start_equity * p.daily_loss_cap:
            reasons.append("DAILY_LOSS_LIMIT")
        if (
            not request.observed_at <= at < request.expires_at
            or (at - request.observed_at).total_seconds() > p.account_age_seconds
        ):
            reasons.append("REQUEST_STALE")
        if request.leverage > p.max_leverage:
            reasons.append("LEVERAGE_LIMIT")
        if request.spread_bps > p.max_spread_bps or request.liquidity_scale == 0:
            reasons.append("LIQUIDITY_UNSAFE")
        positions = (*a.positions, *a.pending)
        for pos in positions:
            if not 0 <= (at - pos.as_of).total_seconds() <= p.account_age_seconds:
                reasons.append("POSITION_STALE")
            if (
                pos.stop_price is None
                or not pos.protected
                or (
                    pos.mark_price <= pos.stop_price
                    if pos.side is Side.LONG
                    else pos.mark_price >= pos.stop_price
                )
            ):
                reasons.append("PROTECTION_NOT_READY")
        symbols = {request.instrument.symbol, *(pos.symbol for pos in positions)}
        factors = {f.symbol: f for f in a.factors}
        for symbol in sorted(symbols):
            fact = factors.get(symbol)
            if (
                fact is None
                or fact.beta is None
                or fact.realized_vol is None
                or not 0 <= (at - fact.as_of).total_seconds() <= p.factor_age_seconds
            ):
                reasons.append("FACTOR_UNKNOWN")
        groups = clusters(symbols, a.correlations, at=at, policy=p)
        if groups is None:
            reasons.append("CORRELATION_UNKNOWN")
        sized, portfolio = None, None
        if not reasons:
            assert a.equity is not None and a.available_balance is not None and groups is not None
            vol = factors[request.instrument.symbol].realized_vol
            assert vol is not None
            sized = size_entry(request, equity=a.equity, realized_vol=vol, policy=p)
            if sized is None:
                reasons.append("ROUNDING_OR_FILTER_LIMIT")
            else:
                if sized.margin + sized.costs > a.available_balance:
                    reasons.append("BALANCE_LIMIT")
                portfolio = exposure(request, a, sized, groups)
                for amount, limit, code in (
                    (portfolio.heat, p.max_heat, "HEAT_LIMIT"),
                    (portfolio.symbol_notional, p.max_symbol_notional, "SYMBOL_LIMIT"),
                    (portfolio.side_notional, p.max_side_notional, "SIDE_LIMIT"),
                    (portfolio.gross_notional, p.max_gross_notional, "GROSS_LIMIT"),
                    (abs(portfolio.delta), p.max_delta, "DELTA_LIMIT"),
                    (abs(portfolio.beta_delta), p.max_beta_delta, "BETA_LIMIT"),
                    (portfolio.cluster_notional, p.max_cluster_notional, "CLUSTER_LIMIT"),
                ):
                    if amount > a.equity * limit:
                        reasons.append(code)
                if portfolio.positions > p.max_positions:
                    reasons.append("MAX_POSITIONS")
        return RiskAssessment(
            at=at,
            policy=p,
            request=request,
            account=a,
            state=state,
            sized=sized,
            portfolio=portfolio,
            reason_codes=tuple(dict.fromkeys(reasons)),
        )

    def guardian_recheck(
        self,
        quant: RiskAssessment,
        account: AccountSnapshot,
        state: RiskState,
        *,
        at: datetime,
        db_writable: bool = False,
    ) -> RiskAssessment:
        current = self.assess(quant.request, account, state, at=at, db_writable=db_writable)
        reasons = list(current.reason_codes)
        if quant.policy != self.policy:
            reasons.append("POLICY_MISMATCH")
        if not quant.allowed or quant.stage != "quant" or quant.at > at:
            reasons.append("QUANT_NOT_ELIGIBLE")
        # Guardian may reduce the analyst's size, but never enlarge its proposal.
        if current.sized is not None and quant.sized is not None:
            if current.sized.qty > quant.sized.qty:
                reasons.append("QUANT_SIZE_EXCEEDED")
        return RiskAssessment.model_validate(
            {
                **current.model_dump(),
                "stage": "guardian",
                "reason_codes": tuple(dict.fromkeys(reasons)),
            }
        )
