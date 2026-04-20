from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TradeLeg:
    """Single leg of an option trade (entry or spread leg)."""
    action: str                        # BUY | SELL | EXIT_ALL | TRAIL_SL | RESTORE_SL
    official_symbol: str               # Underlying root e.g. SENSEX, WAAREEENER
    trading_symbol: str                # Full broker symbol e.g. SENSEX2640976500CE
    strike: str
    option_type: str                   # CE | PE
    expiry_day: str
    expiry_month: str
    expiry_year: str
    entry_range: list[float] = field(default_factory=lambda: [0.0, 0.0])
    stop_loss: float = 0.0
    target: float = 0.0
    size_override_mode: str = ""
    size_override_reason: str = ""
    spread_leg: bool = False

    @property
    def buy_range_min(self) -> float:
        return self.entry_range[0] if self.entry_range else 0.0

    @property
    def buy_range_max(self) -> float:
        return self.entry_range[1] if len(self.entry_range) > 1 else 0.0


@dataclass
class TradeSignal:
    """
    Canonical event object produced by the AI parser and consumed by the state engine.
    Represents a single parsed intent from a Telegram message.
    """
    action: str                                    # BUY | SELL | EXIT_ALL | TRAIL_SL | RESTORE_SL
    is_spread: bool = False
    legs: list[TradeLeg] = field(default_factory=list)

    # Single-leg fields (populated when is_spread=False)
    official_symbol: str = ""
    trading_symbol: str = ""
    strike: str = ""
    option_type: str = ""
    expiry_day: str = ""
    expiry_month: str = ""
    expiry_year: str = ""
    entry_range: list[float] = field(default_factory=lambda: [0.0, 0.0])
    stop_loss: float = 0.0
    target: float = 0.0
    size_override_mode: str = ""
    size_override_reason: str = ""

    # Parser metadata
    raw_child_text: str = ""
    raw_parent_text: str = ""
    is_reply: bool = False
    intent_reason: str = ""            # Why this intent was chosen (for logs)

    @property
    def buy_range_min(self) -> float:
        return self.entry_range[0] if self.entry_range else 0.0

    @property
    def buy_range_max(self) -> float:
        return self.entry_range[1] if len(self.entry_range) > 1 else 0.0

    @classmethod
    def from_dict(cls, data: dict, *, raw_child: str = "", raw_parent: str = "", is_reply: bool = False) -> "TradeSignal":
        """Build a TradeSignal from the legacy dict format returned by assistant."""
        if data.get("spread") and data.get("legs"):
            legs = []
            for leg_dict in data["legs"]:
                legs.append(TradeLeg(
                    action=str(leg_dict.get("action", "BUY")).upper(),
                    official_symbol=str(leg_dict.get("official_symbol", leg_dict.get("stock", ""))),
                    trading_symbol=str(leg_dict.get("stock", leg_dict.get("trading_symbol", ""))),
                    strike=str(leg_dict.get("strike", "")),
                    option_type=str(leg_dict.get("option_type", "")).upper(),
                    expiry_day=str(leg_dict.get("expiry_day", "")),
                    expiry_month=str(leg_dict.get("expiry_month", "")),
                    expiry_year=str(leg_dict.get("expiry_year", "")),
                    entry_range=list(leg_dict.get("entry_range", [0.0, 0.0])),
                    stop_loss=float(leg_dict.get("stop_loss", 0) or 0),
                    target=float(leg_dict.get("target", 0) or 0),
                    size_override_mode=str(leg_dict.get("size_override_mode", data.get("size_override_mode", "")) or ""),
                    size_override_reason=str(leg_dict.get("size_override_reason", data.get("size_override_reason", "")) or ""),
                    spread_leg=True,
                ))
            return cls(
                action="SPREAD",
                is_spread=True,
                legs=legs,
                raw_child_text=raw_child,
                raw_parent_text=raw_parent,
                is_reply=is_reply,
            )

        return cls(
            action=str(data.get("action", "BUY")).upper(),
            is_spread=False,
            official_symbol=str(data.get("official_symbol", data.get("stock", ""))),
            trading_symbol=str(data.get("stock", data.get("trading_symbol", ""))),
            strike=str(data.get("strike", "")),
            option_type=str(data.get("option_type", "")).upper(),
            expiry_day=str(data.get("expiry_day", "")),
            expiry_month=str(data.get("expiry_month", "")),
            expiry_year=str(data.get("expiry_year", "")),
            entry_range=list(data.get("entry_range", [0.0, 0.0])),
            stop_loss=float(data.get("stop_loss", 0) or 0),
            target=float(data.get("target", 0) or 0),
            size_override_mode=str(data.get("size_override_mode", "") or ""),
            size_override_reason=str(data.get("size_override_reason", "") or ""),
            raw_child_text=raw_child,
            raw_parent_text=raw_parent,
            is_reply=is_reply,
        )

    def to_legacy_dict(self) -> dict:
        """Convert back to the dict format expected by Trader for backward compatibility."""
        if self.is_spread:
            return {
                "spread": True,
                "legs": [
                    {
                        "action": leg.action,
                        "official_symbol": leg.official_symbol,
                        "stock": leg.trading_symbol,
                        "trading_symbol": leg.trading_symbol,
                        "strike": leg.strike,
                        "option_type": leg.option_type,
                        "expiry_day": leg.expiry_day,
                        "expiry_month": leg.expiry_month,
                        "expiry_year": leg.expiry_year,
                        "entry_range": leg.entry_range,
                        "stop_loss": leg.stop_loss,
                        "target": leg.target,
                        "spread_leg": leg.spread_leg,
                        "buy_range_min": leg.buy_range_min,
                        "buy_range_max": leg.buy_range_max,
                        "is_trade": True,
                        "size_override_mode": self.size_override_mode,
                        "size_override_reason": self.size_override_reason,
                    }
                    for leg in self.legs
                ],
            }
        return {
            "action": self.action,
            "official_symbol": self.official_symbol,
            "stock": self.trading_symbol,
            "trading_symbol": self.trading_symbol,
            "strike": self.strike,
            "option_type": self.option_type,
            "expiry_day": self.expiry_day,
            "expiry_month": self.expiry_month,
            "expiry_year": self.expiry_year,
            "entry_range": self.entry_range,
            "buy_range_min": self.buy_range_min,
            "buy_range_max": self.buy_range_max,
            "stop_loss": self.stop_loss,
            "target": self.target,
            "is_trade": True,
            "size_override_mode": self.size_override_mode,
            "size_override_reason": self.size_override_reason,
        }
