"""Fail-closed readiness evidence; the portfolio risk engine is a later phase."""

from tradeforge.domain.common import DataQuality, ReasonCode, Reasons, StrictModel


class TradingReadiness(StrictModel):
    data_quality: DataQuality = DataQuality.UNKNOWN
    db_writable: bool = False
    outbox_ready: bool = False
    account_ready: bool = False
    risk_ready: bool = False
    protection_ready: bool = False
    edge_validated: bool = False
    mode_authorized: bool = False
    kill_switch: bool = True
    reason_codes: Reasons = (ReasonCode.FOUNDATION_ONLY,)

    @property
    def can_open_entries(self) -> bool:
        return (
            self.data_quality is DataQuality.OK
            and not self.kill_switch
            and all(
                (
                    self.db_writable,
                    self.outbox_ready,
                    self.account_ready,
                    self.risk_ready,
                    self.protection_ready,
                    self.edge_validated,
                    self.mode_authorized,
                )
            )
        )

    @property
    def can_reduce_risk(self) -> bool:
        # Entry gates do not revoke the independent guardian's repair/exit path.
        return True
