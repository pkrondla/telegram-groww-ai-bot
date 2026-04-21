import json
import logging
import uuid
import os
import re
import csv
import copy
import time
import datetime
import math
import pytz
from .config import Config

logger = logging.getLogger(__name__)

class Trader:
    """
    Handles order execution, instrument lookup, and local state management for Groww.

    When dry_run is True, only *entry* placement is skipped (MARKET/LIMIT BUY or SELL that
    opens a position, plus OCO attach after entry, and broker LIMIT rows for that path).
    LTP/quotes, instrument resolution, and pending-queue LTP polls still call Groww when
    the key is set.

    EXIT_ALL / TRAIL_SL / RESTORE_SL / UPDATE_TGT: if active_trades.json holds a real broker
    smart-order id, those APIs run against Groww. If the id is DRY_RUN_PLACEHOLDER_ORDER_ID
    (entry was simulated), management is local-only — no broker call, since there is no
    position/OCO.
    """
    
    # Stored in active_trades.json when a BUY was validated in dry run but not sent to Groww.
    DRY_RUN_PLACEHOLDER_ORDER_ID = "DRY_RUN_NO_ORDER"
    SMART_SEGMENT = "FNO"
    INDEX_UNDERLYINGS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"})
    
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.active_trades_file = "active_trades.json"
        self.pending_trades_file = "pending_trades.json"
        self.pending_management_file = "pending_management.json"
        self.completed_trades_dir = "completed_trades"
        self._instrument_cache = None  # lazy-loaded dict[(exchange,trading_symbol)] -> row dict
        self.place_entry_orders = not dry_run
        self._access_token = None
        
        self.client = None
        if Config.GROWW_API_KEY:
            try:
                from growwapi import GrowwAPI

                # Prefer exchanging API key + secret for an access token.
                # growwapi expects Authorization: Bearer <token> for API calls.
                token = None
                if Config.GROWW_API_SECRET:
                    try:
                        token = GrowwAPI.get_access_token(
                            api_key=Config.GROWW_API_KEY, secret=Config.GROWW_API_SECRET
                        )
                    except Exception as e:
                        logger.error(f"Groww access token fetch failed: {e}")

                # Fall back to using GROWW_API_KEY directly (some users store bearer tokens there)
                self._access_token = token or Config.GROWW_API_KEY
                self.client = GrowwAPI(self._access_token)
                if self.place_entry_orders:
                    logger.info("Trader initialized in LIVE mode (entry orders enabled).")
                else:
                    logger.info(
                        "Trader initialized in DRY RUN mode (entry BUY/SELL placement disabled; "
                        "quotes / LTP / exit-modify APIs still active if key is set)."
                    )
            except Exception as e:
                logger.error(f"Groww client init failed: {e}")
        else:
            logger.warning("GROWW_API_KEY missing. Groww client not initialized.")

    @property
    def execute_orders(self) -> bool:
        """Alias for place_entry_orders (entry BUY/SELL placement only)."""
        return self.place_entry_orders

    @execute_orders.setter
    def execute_orders(self, value: bool) -> None:
        self.place_entry_orders = bool(value)
        self.dry_run = not bool(value)

    def check_connection(self):
        """
        Verifies login with Groww.
        """
        if self.client is None:
            return False
        try:
            # Simple profile check to verify token (growwapi doesn't expose get_balance)
            self.client.get_user_profile()
            logger.info("Groww connection verified.")
            return True
        except Exception as e:
            logger.error(f"Groww Connection Error: {str(e)}")
            return False

    def check_trading_access(self) -> bool:
        """
        Verifies that trading-related API endpoints are accessible from current IP.
        This catches static-IP whitelist issues early at startup.
        """
        if self.client is None:
            logger.error("Groww trading access check failed: client not initialized.")
            return False
        try:
            # Read-only trading endpoint, but guarded by the same infra controls.
            self.client.get_positions_for_user(segment="FNO")
            logger.info("Groww trading endpoint access verified (FNO positions API).")
            return True
        except Exception as e:
            msg = str(e)
            if "unregistered ip" in msg.lower():
                logger.error(
                    "Groww trading access denied: Request from unregistered IP address. "
                    "Update static IP in Groww API dashboard (primary/secondary) and retry."
                )
            else:
                logger.error(f"Groww trading access check failed: {msg}")
            return False

    def reconcile_trades(self):
        """
        Synchronizes local active_trades.json with the broker's live state.
        Removes trades that were closed manually or were cancelled.
        """
        try:
            if self.client is None:
                logger.warning("Reconciliation skipped: Groww client not initialized.")
                return

            local_trades = self._load_active_trades()
            if not local_trades:
                return

            logger.info(f"Reconciling {len(local_trades)} active trades with Groww...")
            positions_map = self._active_positions_map()
            active_symbols = set(positions_map.keys())

            updated_trades = {}
            for symbol, rec in local_trades.items():
                if symbol in active_symbols:
                    updated_trades[symbol] = self._normalize_active_trade_record(rec)
                else:
                    logger.info(f"Reconciliation: Found manual close for {symbol}. Removing from bot memory.")
            
            self._save_all_trades(updated_trades)
            logger.info("Trade reconciliation complete.")
        except Exception as e:
            logger.error(f"Reconciliation Error: {str(e)}")

    @staticmethod
    def _today_ist_iso() -> str:
        # Keep date logic simple and deterministic without extra deps.
        return (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).date().isoformat()

    @staticmethod
    def _now_ist() -> datetime.datetime:
        return datetime.datetime.now(pytz.timezone("Asia/Kolkata"))

    @classmethod
    def _now_ist_iso(cls) -> str:
        return cls._now_ist().isoformat(timespec="seconds")

    @staticmethod
    def _parse_hhmm_ist(s: str) -> tuple[int, int] | None:
        raw = str(s or "").strip()
        m = re.match(r"^(\d{1,2}):(\d{2})$", raw)
        if not m:
            return None
        h, mm = int(m.group(1)), int(m.group(2))
        if h < 0 or h > 23 or mm < 0 or mm > 59:
            return None
        return h, mm

    def _completed_trades_file(self, date_iso: str | None = None) -> str:
        d = str(date_iso or self._today_ist_iso())
        os.makedirs(self.completed_trades_dir, exist_ok=True)
        return os.path.join(self.completed_trades_dir, f"completed_trades_{d}.json")

    def _load_completed_trades_book(self, date_iso: str | None = None) -> dict:
        d = str(date_iso or self._today_ist_iso())
        path = self._completed_trades_file(d)
        if not os.path.exists(path):
            return {
                "date": d,
                "timezone": "Asia/Kolkata",
                "trades": [],
                "eod": {"summary_sent_at": ""},
            }
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("invalid completed trades schema")
            data.setdefault("date", d)
            data.setdefault("timezone", "Asia/Kolkata")
            data.setdefault("trades", [])
            data.setdefault("eod", {"summary_sent_at": ""})
            return data
        except Exception:
            return {
                "date": d,
                "timezone": "Asia/Kolkata",
                "trades": [],
                "eod": {"summary_sent_at": ""},
            }

    def _save_completed_trades_book(self, book: dict, date_iso: str | None = None):
        d = str(date_iso or book.get("date") or self._today_ist_iso())
        path = self._completed_trades_file(d)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(book, f, indent=4)

    @staticmethod
    def _num_or(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    @classmethod
    def _point_move(cls, side: str, entry: float, exit_: float) -> float:
        if str(side or "").upper() == "SELL":
            return entry - exit_
        return exit_ - entry

    @classmethod
    def _trade_pnl(cls, side: str, entry: float, exit_: float, qty: int) -> tuple[float, float]:
        move = cls._point_move(side, entry, exit_)
        return move, move * float(qty or 0)

    @classmethod
    def _walk_for_numeric_key(cls, payload, keys: set[str]) -> float | None:
        if isinstance(payload, dict):
            for k, v in payload.items():
                if str(k).lower() in keys:
                    try:
                        return float(v)
                    except Exception:
                        pass
            for v in payload.values():
                got = cls._walk_for_numeric_key(v, keys)
                if got is not None:
                    return got
        elif isinstance(payload, list):
            for item in payload:
                got = cls._walk_for_numeric_key(item, keys)
                if got is not None:
                    return got
        return None

    def _fetch_order_fill(self, *, segment: str, groww_order_id: str, fallback_price: float, fallback_qty: int) -> dict:
        info = {
            "avg_price": float(fallback_price or 0),
            "filled_qty": int(fallback_qty or 0),
            "source": "fallback",
        }
        if not self.client or not groww_order_id:
            return info
        try:
            st = self.client.get_order_status(segment=segment, groww_order_id=groww_order_id)
            avg = self._walk_for_numeric_key(
                st,
                {
                    "average_price",
                    "avg_price",
                    "avgprice",
                    "averageprice",
                    "traded_price",
                    "trade_price",
                    "executed_price",
                    "fill_price",
                },
            )
            qty = self._walk_for_numeric_key(
                st,
                {
                    "filled_quantity",
                    "filled_qty",
                    "traded_quantity",
                    "traded_qty",
                    "executed_quantity",
                    "executed_qty",
                    "quantity",
                    "qty",
                },
            )
            if avg is not None and avg > 0:
                info["avg_price"] = float(avg)
            if qty is not None and qty > 0:
                info["filled_qty"] = int(qty)
            info["source"] = "order_status"
        except Exception as e:
            logger.debug(f"Fill fetch failed for {groww_order_id}: {e}")
        return info

    def _record_completed_trade(
        self,
        *,
        symbol: str,
        active_rec: dict | None,
        close_reason: str,
        exit_price: float,
        exit_qty: int,
        close_order_id: str = "",
    ):
        rec = self._normalize_active_trade_record(active_rec or {})
        side = str(rec.get("entry_side") or "BUY").upper()
        entry_price = self._num_or(rec.get("entry_avg_price"), 0.0)
        qty = int(self._num_or(rec.get("entry_qty"), 0))
        if qty <= 0:
            qty = int(abs(exit_qty or 0))
        lot_size = int(self._num_or(rec.get("lot_size"), 0))
        if lot_size <= 0:
            lot_size = max(1, qty)
        lots = max(1.0, float(qty) / float(lot_size))
        group_id = str(rec.get("spread_group_id") or "")
        is_spread_leg = bool(group_id)

        points_per_lot, pnl_inr = self._trade_pnl(side, entry_price, float(exit_price or 0), qty)
        entry = {
            "trade_id": str(uuid.uuid4())[:12],
            "group_id": group_id,
            "is_spread_leg": is_spread_leg,
            "symbol": str(symbol),
            "side": side,
            "quantity": int(qty),
            "lot_size": int(lot_size),
            "lots": float(lots),
            "entry_avg_price": float(entry_price),
            "exit_avg_price": float(exit_price or 0),
            "pnl_points_per_lot": float(points_per_lot),
            "pnl_inr": float(pnl_inr),
            "opened_at_ist": str(rec.get("opened_at_ist") or ""),
            "closed_at_ist": self._now_ist_iso(),
            "close_reason": str(close_reason),
            "entry_order_id": str(rec.get("entry_order_id") or ""),
            "exit_order_id": str(close_order_id or ""),
            "smart_order_id": str(rec.get("smart_order_id") or ""),
        }

        book = self._load_completed_trades_book()
        trades = book.get("trades") or []
        trades.append(entry)
        book["trades"] = trades
        self._save_completed_trades_book(book)

    def _build_eod_summary_text(self, *, date_iso: str | None = None) -> str:
        d = str(date_iso or self._today_ist_iso())
        book = self._load_completed_trades_book(d)
        rows = list(book.get("trades") or [])
        active_count = len(self._load_active_trades())

        singles = [r for r in rows if not r.get("is_spread_leg")]
        spread_legs = [r for r in rows if r.get("is_spread_leg")]
        spread_groups: dict[str, dict] = {}
        for r in spread_legs:
            gid = str(r.get("group_id") or "")
            if not gid:
                continue
            g = spread_groups.setdefault(gid, {"pnl_inr": 0.0, "pnl_points_per_lot": 0.0, "symbols": []})
            g["pnl_inr"] += float(r.get("pnl_inr") or 0.0)
            g["pnl_points_per_lot"] += float(r.get("pnl_points_per_lot") or 0.0)
            g["symbols"].append(str(r.get("symbol") or ""))

        spread_summaries = []
        for gid, g in spread_groups.items():
            spread_summaries.append(
                {
                    "id": gid,
                    "label": " + ".join(sorted(set([s for s in g["symbols"] if s]))),
                    "pnl_inr": float(g["pnl_inr"]),
                    "pnl_points_per_lot": float(g["pnl_points_per_lot"]),
                }
            )

        all_trade_summaries = [
            {
                "label": str(r.get("symbol") or ""),
                "pnl_inr": float(r.get("pnl_inr") or 0.0),
                "pnl_points_per_lot": float(r.get("pnl_points_per_lot") or 0.0),
            }
            for r in singles
        ] + spread_summaries

        wins = sum(1 for r in all_trade_summaries if float(r.get("pnl_inr") or 0.0) > 0)
        losses = sum(1 for r in all_trade_summaries if float(r.get("pnl_inr") or 0.0) < 0)
        total_inr = sum(float(r.get("pnl_inr") or 0.0) for r in all_trade_summaries)
        total_pts = sum(float(r.get("pnl_points_per_lot") or 0.0) for r in all_trade_summaries)

        top_winner = max(all_trade_summaries, key=lambda x: float(x.get("pnl_inr") or 0.0), default=None)
        top_loser = min(all_trade_summaries, key=lambda x: float(x.get("pnl_inr") or 0.0), default=None)

        lines = []
        for r in all_trade_summaries:
            lines.append(
                f"• {r['label']}: INR {float(r['pnl_inr']):.2f} | {float(r['pnl_points_per_lot']):.2f} pts/lot"
            )
        per_trade_section = "\n".join(lines) if lines else "• No completed trades."

        winner_text = (
            f"{top_winner['label']} (INR {float(top_winner['pnl_inr']):.2f})" if top_winner else "N/A"
        )
        loser_text = (
            f"{top_loser['label']} (INR {float(top_loser['pnl_inr']):.2f})" if top_loser else "N/A"
        )
        return (
            f"📊 **EOD Trading Summary ({d} IST)**\n\n"
            f"Trades closed: {len(singles)} (single), {len(spread_summaries)} (spreads)\n"
            f"Realized P&L INR: `{total_inr:.2f}`\n"
            f"Premium points per lot: `{total_pts:.2f}` pts\n"
            f"Wins/Losses: `{wins}/{losses}`\n"
            f"Top winner: {winner_text}\n"
            f"Top loser: {loser_text}\n"
            f"Open carry-forward positions: `{active_count}`\n\n"
            f"**Per-trade P&L**\n{per_trade_section}\n\n"
            f"**Final day P/L:** INR `{total_inr:.2f}` | `{total_pts:.2f}` pts/lot"
        )

    def run_eod_summary_tick(self) -> str | None:
        if not Config.EOD_SUMMARY_ENABLED:
            return None
        now = self._now_ist()
        if Config.EOD_SUMMARY_SKIP_WEEKENDS and now.weekday() >= 5:
            return None
        t = self._parse_hhmm_ist(Config.EOD_SUMMARY_TIME_IST)
        if not t:
            return "⚠️ EOD summary skipped: invalid EOD_SUMMARY_TIME_IST (expected HH:MM IST)."
        if now.strftime("%H:%M") != f"{t[0]:02d}:{t[1]:02d}":
            return None
        d = now.date().isoformat()
        book = self._load_completed_trades_book(d)
        eod = book.get("eod") or {}
        sent_at = str(eod.get("summary_sent_at") or "")
        if sent_at.startswith(d):
            return None
        summary = self._build_eod_summary_text(date_iso=d)
        book.setdefault("eod", {})
        book["eod"]["summary_sent_at"] = self._now_ist_iso()
        self._save_completed_trades_book(book, date_iso=d)
        return summary

    @staticmethod
    def _float_or(rec: dict, key: str, default: float = 0.0) -> float:
        try:
            return float(rec.get(key) or default)
        except Exception:
            return default

    @staticmethod
    def _bool_or(rec: dict, key: str, default: bool = False) -> bool:
        v = rec.get(key, default)
        if isinstance(v, bool):
            return v
        if v is None:
            return default
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off", ""):
            return False
        return default

    @staticmethod
    def _option_type_from_symbol(symbol: str) -> str:
        u = str(symbol or "").upper().strip()
        if u.endswith("CE"):
            return "CE"
        if u.endswith("PE"):
            return "PE"
        return ""

    @classmethod
    def _widened_sl_from_true(cls, symbol: str, sl_true: float, widen_pct: float) -> float | None:
        try:
            st = float(sl_true)
            p = float(widen_pct)
        except Exception:
            return None
        if st <= 0 or p <= 0:
            return None
        opt = cls._option_type_from_symbol(symbol)
        delta = st * p
        if opt == "PE":
            widened = st + delta
        else:
            # Default CE (and unknown): widen by moving SL farther from zero strike-side → lower trigger.
            widened = st - delta
        if widened <= 0:
            return None
        return widened

    @staticmethod
    def _position_quantity(position: dict) -> int:
        for key in ("quantity", "net_quantity", "netQuantity", "net_qty", "qty"):
            try:
                q = int(float(position.get(key) or 0))
                if q != 0:
                    return q
            except Exception:
                continue
        return 0

    def _active_positions_map(self) -> dict[str, dict]:
        if self.client is None:
            return {}
        fno_positions = self.client.get_positions_for_user(segment="FNO")
        positions = fno_positions.get("positions", []) if isinstance(fno_positions, dict) else []
        out = {}
        for p in positions:
            sym = p.get("trading_symbol")
            if not sym:
                continue
            if self._position_quantity(p) != 0:
                out[str(sym)] = p
        return out

    def _get_open_position_for_symbol(self, symbol: str) -> tuple[dict | None, int]:
        """Return (position_dict, qty) for symbol from broker live positions."""
        try:
            pmap = self._active_positions_map()
        except Exception:
            return None, 0
        pos = pmap.get(symbol)
        if not pos:
            return None, 0
        return pos, self._position_quantity(pos)

    def _rearm_oco_for_open_position(self, symbol: str, rec: dict, position: dict) -> tuple[bool, str | None]:
        """
        Recreate OCO once per day for carry-forward open positions.
        Returns True when a new OCO was created.
        """
        if self.client is None:
            return False, None

        order_id = str(rec.get("smart_order_id") or "")
        if order_id == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
            return False, None

        today = self._today_ist_iso()
        if str(rec.get("protection_armed_date") or "") == today:
            return False, None

        try:
            raw_sl = float(rec.get("stop_loss") or 0)
            raw_tgt = float(rec.get("target") or 0)
        except Exception:
            return False, None
        eff_tgt = self._take_profit_order_price(raw_tgt)
        if raw_sl <= 0 or eff_tgt <= 0:
            logger.warning(
                f"[REARM] Skip {symbol}: invalid stored levels (sl={raw_sl}, target={raw_tgt})."
            )
            return False, f"⚠️ OCO re-arm skipped for {symbol}: invalid stored SL/target (sl={raw_sl}, tgt={raw_tgt})."

        qty = abs(self._position_quantity(position))
        if qty <= 0:
            return False, None

        exchange = str(position.get("exchange") or "NSE")
        segment = str(position.get("segment") or self.SMART_SEGMENT)
        product_type = str(
            position.get("product")
            or position.get("product_type")
            or position.get("productType")
            or "NRML"
        ).upper()

        rearm_ref = str(uuid.uuid4())[:15]
        logger.info(
            f"[REARM] Creating OCO for {symbol}: qty={qty}, sl={raw_sl}, target(signal={raw_tgt}, broker={eff_tgt})"
        )
        try:
            resp = self.client.create_smart_order(
                smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                reference_id=rearm_ref,
                segment=segment,
                trading_symbol=symbol,
                quantity=qty,
                net_position_quantity=qty,
                transaction_type="SELL",
                product_type=product_type,
                exchange=exchange,
                duration="DAY",
                target={
                    "trigger_price": eff_tgt,
                    "price": eff_tgt,
                    "order_type": "LIMIT",
                },
                stop_loss={
                    "trigger_price": raw_sl,
                    "price": raw_sl,
                    "order_type": "SL",
                },
            )
        except Exception as e:
            msg = str(e)
            if "cannot exceed position quantity" in msg.lower():
                # Broker indicates protective quantity is already attached.
                # Mark today's armed date to avoid repeated re-arm attempts/spam.
                trades = self._load_active_trades()
                nrec = self._normalize_active_trade_record(trades.get(symbol))
                nrec["protection_armed_date"] = today
                trades[symbol] = nrec
                self._save_all_trades(trades)
                logger.info(f"[REARM] Skip {symbol}: broker reports protection already present.")
                return (
                    False,
                    f"ℹ️ OCO re-arm skipped for {symbol}: broker reports protection already attached.",
                )
            raise
        new_id = self._extract_smart_order_id(resp, fallback=rearm_ref)
        self._save_active_trade(
            symbol,
            new_id,
            stop_loss=raw_sl,
            target=raw_tgt,
            oco_legs=[
                {
                    "smart_order_id": str(new_id or ""),
                    "qty": int(qty),
                    "target_signal": float(raw_tgt),
                    "target_broker": float(eff_tgt),
                    "target_index": 1,
                    "status": "ACTIVE",
                }
            ],
        )
        trades = self._load_active_trades()
        nrec = self._normalize_active_trade_record(trades.get(symbol))
        nrec["protection_armed_date"] = today
        trades[symbol] = nrec
        self._save_all_trades(trades)
        logger.info(f"[REARM] OCO re-armed for {symbol} with smart_order_id={new_id}.")
        msg = (
            f"✅ OCO re-armed for {symbol}\n"
            f"Qty: {qty}\n"
            f"SL: {raw_sl}\n"
            f"Target: signal {raw_tgt} → broker {eff_tgt}\n"
            f"Smart Order ID: {new_id}"
        )
        return True, msg

    def maintain_active_trades(self) -> dict:
        """
        Unified active-trade maintenance:
        1) Reconcile local active trades with broker open positions.
        2) Optionally re-arm OCO once per day for carry-forward open positions.
        """
        stats = {"active": 0, "removed": 0, "rearmed": 0, "alerts": []}
        if self.client is None:
            return stats

        local = self._load_active_trades()
        if not local:
            return stats

        positions_map = self._active_positions_map()
        stats["active"] = len(positions_map)
        active_symbols = set(positions_map.keys())

        # Reconcile first: remove symbols not open on broker.
        updated = {}
        for symbol, rec in local.items():
            if symbol in active_symbols:
                updated[symbol] = self._normalize_active_trade_record(rec)
            else:
                stats["removed"] += 1
                try:
                    nrec = self._normalize_active_trade_record(rec)
                    fallback_exit = self._num_or(nrec.get("entry_avg_price"), 0.0)
                    fallback_qty = int(self._num_or(nrec.get("entry_qty"), 0))
                    self._record_completed_trade(
                        symbol=symbol,
                        active_rec=nrec,
                        close_reason="RECONCILE_FLAT",
                        exit_price=fallback_exit,
                        exit_qty=fallback_qty,
                    )
                except Exception as e:
                    logger.warning(f"[RECONCILE] Could not journal closed trade for {symbol}: {e}")
                logger.info(f"[RECONCILE] Removing stale local trade {symbol} (not active on broker).")
        self._save_all_trades(updated)

        if not Config.AUTO_REARM_PROTECTION:
            return stats

        # Re-arm protection for carry-forward open positions.
        for symbol, rec in list(updated.items()):
            try:
                ok, alert = self._rearm_oco_for_open_position(symbol, rec, positions_map[symbol])
                if ok:
                    stats["rearmed"] += 1
                if alert:
                    stats["alerts"].append(alert)
            except Exception as e:
                logger.warning(f"[REARM] Failed for {symbol}: {e}")
                stats["alerts"].append(f"❌ OCO re-arm failed for {symbol}: {e}")
        return stats

    def admin_rearm_protection_for_active_trades(self) -> str:
        """
        Admin-triggered OCO re-arm sweep.
        Tries to re-arm protection for all locally active trades that still have
        an open broker position. Safe to run multiple times.
        """
        if self.client is None:
            return "⚠️ REARM skipped: Groww client not initialized."

        local = self._load_active_trades()
        if not local:
            return "ℹ️ REARM: no active trades in local book."

        positions_map = self._active_positions_map()
        if not positions_map:
            return "ℹ️ REARM: no open broker positions found."

        rearmed = 0
        already = 0
        skipped = 0
        failed = 0
        details: list[str] = []

        for symbol, raw in local.items():
            rec = self._normalize_active_trade_record(raw)
            if rec.get("smart_order_id") == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                skipped += 1
                details.append(f"• {symbol}: skipped (dry-run placeholder)")
                continue
            pos = positions_map.get(symbol)
            if not pos:
                skipped += 1
                details.append(f"• {symbol}: skipped (not open on broker)")
                continue
            try:
                # Force an attempt now, regardless of today's protection_armed_date.
                rec_force = dict(rec)
                rec_force["protection_armed_date"] = ""
                ok, alert = self._rearm_oco_for_open_position(symbol, rec_force, pos)
                if ok:
                    rearmed += 1
                    details.append(f"• {symbol}: re-armed")
                    continue
                if alert and "already attached" in alert.lower():
                    already += 1
                    details.append(f"• {symbol}: protection already present")
                elif alert:
                    failed += 1
                    details.append(f"• {symbol}: {alert}")
                else:
                    skipped += 1
                    details.append(f"• {symbol}: no action")
            except Exception as e:
                failed += 1
                details.append(f"• {symbol}: failed ({e})")

        summary = (
            "🛡️ REARM sweep completed\n"
            f"Re-armed: {rearmed}\n"
            f"Already protected: {already}\n"
            f"Skipped: {skipped}\n"
            f"Failed: {failed}"
        )
        if details:
            return summary + "\n\n" + "\n".join(details[:25])
        return summary

    def reconcile_trade_symbol(self, symbol: str) -> bool:
        """
        Reconcile a single active-trade symbol against broker positions.
        Returns True when symbol is still active on broker, else False.
        """
        if not symbol:
            return False
        local = self._load_active_trades()
        if symbol not in local:
            return False

        # Dry-run placeholder has no broker-side position/OCO; keep local row
        # until explicit EXIT_ALL so management messages still work predictably.
        rec = self._normalize_active_trade_record(local.get(symbol))
        if rec.get("smart_order_id") == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
            return True

        if self.client is None:
            logger.warning(f"Symbol reconcile skipped for {symbol}: Groww client not initialized.")
            return True

        try:
            fno_positions = self.client.get_positions_for_user(segment="FNO")
            positions = fno_positions.get("positions", []) if isinstance(fno_positions, dict) else []
            active_symbols = {p.get("trading_symbol") for p in positions if p.get("quantity", 0) != 0}
            if symbol in active_symbols:
                return True
            logger.info(f"Reconcile symbol: {symbol} is not active on broker. Removing local active trade row.")
            fallback_exit = self._num_or(rec.get("entry_avg_price"), 0.0)
            fallback_qty = int(self._num_or(rec.get("entry_qty"), 0))
            self._record_completed_trade(
                symbol=symbol,
                active_rec=rec,
                close_reason="RECONCILE_FLAT",
                exit_price=fallback_exit,
                exit_qty=fallback_qty,
            )
            self._remove_active_trade(symbol)
            return False
        except Exception as e:
            logger.warning(f"Symbol reconcile failed for {symbol}: {e}")
            # Fail-open: don't delete local trade on transient broker/API error.
            return True

    def _load_active_trades(self):
        if not os.path.exists(self.active_trades_file):
            return {}
        try:
            with open(self.active_trades_file, "r") as f:
                return json.load(f)
        except:
            return {}

    def _save_all_trades(self, trades_dict):
        with open(self.active_trades_file, "w") as f:
            json.dump(trades_dict, f, indent=4)

    @staticmethod
    def _normalize_active_trade_record(record) -> dict:
        """
        Backward-compatible active-trade schema:
        - Legacy: "SYMBOL": "smart_order_id"
        - New:    "SYMBOL": {"smart_order_id": "...", "stop_loss": x, "target": y}
        """
        if isinstance(record, dict):
            out = dict(record)
            out["smart_order_id"] = str(out.get("smart_order_id") or out.get("order_id") or "")
            legs = out.get("oco_legs")
            norm_legs = []
            if isinstance(legs, list):
                for item in legs:
                    if not isinstance(item, dict):
                        continue
                    oid = str(item.get("smart_order_id") or item.get("order_id") or "")
                    if not oid:
                        continue
                    try:
                        qty = int(float(item.get("qty") or 0))
                    except Exception:
                        qty = 0
                    try:
                        tgt_sig = float(item.get("target_signal") or 0)
                    except Exception:
                        tgt_sig = 0.0
                    try:
                        tgt_b = float(item.get("target_broker") or 0)
                    except Exception:
                        tgt_b = 0.0
                    norm_legs.append(
                        {
                            "smart_order_id": oid,
                            "qty": max(0, qty),
                            "target_signal": tgt_sig,
                            "target_broker": tgt_b,
                            "target_index": int(item.get("target_index") or 0),
                            "status": str(item.get("status") or "ACTIVE").upper(),
                        }
                    )
            if norm_legs:
                out["oco_legs"] = norm_legs
                if not out.get("smart_order_id"):
                    out["smart_order_id"] = str(norm_legs[0].get("smart_order_id") or "")
            if "stop_loss" in out:
                try:
                    out["stop_loss"] = float(out.get("stop_loss") or 0)
                except Exception:
                    out["stop_loss"] = 0.0
            if "target" in out:
                try:
                    out["target"] = float(out.get("target") or 0)
                except Exception:
                    out["target"] = 0.0
            for k in ("sl_true", "sl_widened", "sl_widen_pct"):
                if k in out:
                    try:
                        out[k] = float(out.get(k) or 0)
                    except Exception:
                        out[k] = 0.0
            for k in ("entry_avg_price",):
                if k in out:
                    try:
                        out[k] = float(out.get(k) or 0)
                    except Exception:
                        out[k] = 0.0
            for k in ("entry_qty", "lot_size"):
                if k in out:
                    try:
                        out[k] = int(float(out.get(k) or 0))
                    except Exception:
                        out[k] = 0
            for k in ("entry_side", "opened_at_ist", "entry_order_id", "spread_group_id"):
                if k in out and out.get(k) is not None:
                    out[k] = str(out.get(k) or "")
            if "sl_mode" in out:
                out["sl_mode"] = str(out.get("sl_mode") or "").upper().strip() or "NORMAL"
            for k in ("vol_sl_widen_done_for", "vol_sl_restore_done_for"):
                if k in out and out.get(k) is not None:
                    out[k] = str(out.get(k) or "").strip()
            if "vol_sl_needs_restore" in out:
                v = out.get("vol_sl_needs_restore")
                if isinstance(v, bool):
                    out["vol_sl_needs_restore"] = v
                else:
                    sv = str(v or "").strip().lower()
                    out["vol_sl_needs_restore"] = sv in ("1", "true", "yes", "y", "on")
            # Policy bookkeeping: if sl_true missing, treat current stop_loss as authoritative channel SL.
            st = float(out.get("sl_true") or 0)
            if st <= 0 and float(out.get("stop_loss") or 0) > 0:
                out["sl_true"] = float(out.get("stop_loss") or 0)
            return out
        return {"smart_order_id": str(record or "")}

    def _get_active_trade_record(self, symbol: str) -> dict | None:
        rec = self._load_active_trades().get(symbol)
        if rec is None:
            return None
        return self._normalize_active_trade_record(rec)

    def _save_active_trade(
        self,
        symbol,
        order_id,
        *,
        stop_loss: float | None = None,
        target: float | None = None,
        entry_side: str | None = None,
        entry_qty: int | None = None,
        lot_size: int | None = None,
        entry_avg_price: float | None = None,
        opened_at_ist: str | None = None,
        entry_order_id: str | None = None,
        spread_group_id: str | None = None,
        oco_legs: list[dict] | None = None,
    ):
        trades = self._load_active_trades()
        current = self._normalize_active_trade_record(trades.get(symbol))
        oid = str(order_id or "")
        current["smart_order_id"] = oid
        if stop_loss is not None:
            current["stop_loss"] = float(stop_loss)
            # Fresh protection uses channel SL as the authoritative true SL unless already explicitly tracked.
            if float(current.get("sl_true") or 0) <= 0:
                current["sl_true"] = float(stop_loss)
            current.setdefault("sl_mode", "NORMAL")
        if target is not None:
            current["target"] = float(target)
        if entry_side is not None:
            current["entry_side"] = str(entry_side).upper()
        if entry_qty is not None:
            current["entry_qty"] = int(entry_qty)
        if lot_size is not None:
            current["lot_size"] = int(lot_size)
        if entry_avg_price is not None:
            current["entry_avg_price"] = float(entry_avg_price)
        if opened_at_ist is not None:
            current["opened_at_ist"] = str(opened_at_ist)
        if entry_order_id is not None:
            current["entry_order_id"] = str(entry_order_id)
        if spread_group_id is not None:
            current["spread_group_id"] = str(spread_group_id)
        if oco_legs is not None:
            current["oco_legs"] = []
            for leg in oco_legs:
                if not isinstance(leg, dict):
                    continue
                oid_leg = str(leg.get("smart_order_id") or "")
                if not oid_leg:
                    continue
                current["oco_legs"].append(
                    {
                        "smart_order_id": oid_leg,
                        "qty": int(float(leg.get("qty") or 0)),
                        "target_signal": float(leg.get("target_signal") or 0),
                        "target_broker": float(leg.get("target_broker") or 0),
                        "target_index": int(leg.get("target_index") or 0),
                        "status": str(leg.get("status") or "ACTIVE").upper(),
                    }
                )
            if current.get("oco_legs") and not current.get("smart_order_id"):
                current["smart_order_id"] = str(current["oco_legs"][0].get("smart_order_id") or "")
        if oid and oid != self.DRY_RUN_PLACEHOLDER_ORDER_ID:
            current["protection_armed_date"] = self._today_ist_iso()
        trades[symbol] = current
        self._save_all_trades(trades)

    def _remove_active_trade(self, symbol):
        trades = self._load_active_trades()
        if symbol in trades:
            del trades[symbol]
            self._save_all_trades(trades)

    def _get_order_id(self, symbol):
        rec = self._get_active_trade_record(symbol)
        if not rec:
            return None
        return rec.get("smart_order_id")

    @staticmethod
    def _oco_ids_from_record(rec: dict | None) -> list[str]:
        if not rec:
            return []
        ids: list[str] = []
        legs = rec.get("oco_legs")
        if isinstance(legs, list):
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                oid = str(leg.get("smart_order_id") or "")
                if oid and oid not in ids:
                    ids.append(oid)
        primary = str(rec.get("smart_order_id") or "")
        if primary and primary not in ids:
            ids.append(primary)
        return ids

    def _update_active_trade_levels(
        self,
        symbol: str,
        *,
        stop_loss: float | None = None,
        target: float | None = None,
        sl_true: float | None = None,
        sl_widened: float | None = None,
        sl_widen_pct: float | None = None,
        sl_mode: str | None = None,
        vol_sl_needs_restore: bool | None = None,
        vol_sl_widen_done_for: str | None = None,
        vol_sl_restore_done_for: str | None = None,
    ):
        trades = self._load_active_trades()
        if symbol not in trades:
            return
        rec = self._normalize_active_trade_record(trades[symbol])
        if stop_loss is not None:
            rec["stop_loss"] = float(stop_loss)
        if target is not None:
            rec["target"] = float(target)
            if isinstance(rec.get("oco_legs"), list):
                for leg in rec["oco_legs"]:
                    if isinstance(leg, dict):
                        leg["target_signal"] = float(target)
        if sl_true is not None:
            rec["sl_true"] = float(sl_true)
        if sl_widened is not None:
            if float(sl_widened) <= 0:
                rec.pop("sl_widened", None)
            else:
                rec["sl_widened"] = float(sl_widened)
        if sl_widen_pct is not None:
            try:
                rec["sl_widen_pct"] = float(sl_widen_pct)
            except Exception:
                rec.pop("sl_widen_pct", None)
        if sl_mode is not None:
            sm = str(sl_mode or "").upper().strip()
            rec["sl_mode"] = sm or "NORMAL"
        if vol_sl_needs_restore is not None:
            rec["vol_sl_needs_restore"] = bool(vol_sl_needs_restore)
        if vol_sl_widen_done_for is not None:
            rec["vol_sl_widen_done_for"] = str(vol_sl_widen_done_for or "").strip()
        if vol_sl_restore_done_for is not None:
            rec["vol_sl_restore_done_for"] = str(vol_sl_restore_done_for or "").strip()
        trades[symbol] = rec
        self._save_all_trades(trades)

    def _modify_oco_sl(self, symbol: str, order_id: str, new_sl: float) -> float:
        if not self.client:
            raise RuntimeError("Groww client not initialized")
        sl = self._round_price_to_tick(new_sl, mode="nearest")
        if sl <= 0:
            raise RuntimeError(f"Invalid SL after tick-rounding: {new_sl}")
        self.client.modify_smart_order(
            smart_order_id=order_id,
            smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
            segment=self.SMART_SEGMENT,
            stop_loss={
                "trigger_price": float(sl),
                "price": float(sl),
                "order_type": "SL",
            },
        )
        return float(sl)

    def _modify_oco_target(self, order_id: str, new_target: float) -> float:
        if not self.client:
            raise RuntimeError("Groww client not initialized")
        tgt = self._round_price_to_tick(new_target, mode="down")
        if tgt <= 0:
            raise RuntimeError(f"Invalid target after tick-rounding: {new_target}")
        self.client.modify_smart_order(
            smart_order_id=order_id,
            smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
            segment=self.SMART_SEGMENT,
            target={
                "trigger_price": float(tgt),
                "price": float(tgt),
                "order_type": "LIMIT",
            },
        )
        return float(tgt)

    def restore_channel_sl(self, symbol: str, *, source: str) -> str:
        """
        Restore broker OCO SL to persisted sl_true (Policy 1 source of truth).
        Marks scheduled restore idempotency for today when source is admin/channel restore.
        """
        rec = self._get_active_trade_record(symbol) or {}
        order_ids = self._oco_ids_from_record(rec)
        order_id = order_ids[0] if order_ids else None
        if not order_id:
            return f"⚠️ NO ACTIVE TRADE: Could not find an open Order ID for {symbol}.{self._ltp_suffix_for_symbol(symbol)}"
        if order_id == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
            sl_true = float(rec.get("sl_true") or rec.get("stop_loss") or 0)
            if sl_true <= 0:
                return f"⚠️ RESTORE_SL skipped for {symbol}: missing sl_true in local book.{self._ltp_suffix_for_symbol(symbol)}"
            today = self._today_ist_iso()
            self._update_active_trade_levels(
                symbol,
                stop_loss=sl_true,
                sl_true=sl_true,
                sl_mode="NORMAL",
                sl_widened=0.0,
                vol_sl_needs_restore=False,
                vol_sl_restore_done_for=today,
            )
            return (
                f"🧪 [DRY RUN] RESTORE_SL for {symbol}: broker SL would be set to sl_true={sl_true} "
                f"(simulated; no OCO on broker)."
            )

        if not self.client:
            return f"⚠️ NO GROWW CLIENT: Cannot RESTORE_SL for {symbol}. Configure GROWW_API_KEY.{self._ltp_suffix_for_symbol(symbol)}"

        sl_true = float(rec.get("sl_true") or rec.get("stop_loss") or 0)
        if sl_true <= 0:
            return f"⚠️ RESTORE_SL skipped for {symbol}: missing sl_true in local book.{self._ltp_suffix_for_symbol(symbol)}"

        logger.info(
            f"[STAGE 4] RESTORE_SL ({source}): modify_smart_order {symbol} "
            f"ids={order_ids} → sl_true={sl_true}."
        )
        try:
            for oid in order_ids:
                sl_true = self._modify_oco_sl(symbol, oid, sl_true)
        except Exception as e:
            err = str(e or "")
            low = err.lower()
            # Broker-side OCO can be cancelled/expired overnight while position remains open.
            # In that case, create a fresh OCO for the live position instead of failing restore.
            if (
                "cannot be modified" in low
                or "does not exist" in low
                or "not exist" in low
                or "not found" in low
            ):
                pos, qty = self._get_open_position_for_symbol(symbol)
                if qty == 0:
                    return (
                        f"⚠️ RESTORE_SL failed for {symbol}: broker order is stale and no open position exists. "
                        f"Error: {err}{self._ltp_suffix_for_symbol(symbol)}"
                    )
                rec_rearm = dict(rec)
                rec_rearm["stop_loss"] = float(sl_true)
                # Force immediate re-arm regardless of today's armed date.
                rec_rearm["protection_armed_date"] = ""
                ok, alert = self._rearm_oco_for_open_position(symbol, rec_rearm, pos or {})
                if ok:
                    today = self._today_ist_iso()
                    self._update_active_trade_levels(
                        symbol,
                        stop_loss=sl_true,
                        sl_true=sl_true,
                        sl_mode="NORMAL",
                        sl_widened=0.0,
                        vol_sl_needs_restore=False,
                        vol_sl_restore_done_for=today,
                    )
                    return (
                        f"🔁 RESTORE_SL RE-ARMED: {symbol} had no modifiable OCO; "
                        f"created fresh protection at sl_true={sl_true} ({source}).{self._ltp_suffix_for_symbol(symbol)}"
                    )
                if alert:
                    return f"⚠️ RESTORE_SL re-arm attempt for {symbol}: {alert}{self._ltp_suffix_for_symbol(symbol)}"
            raise
        today = self._today_ist_iso()
        self._update_active_trade_levels(
            symbol,
            stop_loss=sl_true,
            sl_true=sl_true,
            sl_mode="NORMAL",
            sl_widened=0.0,
            vol_sl_needs_restore=False,
            vol_sl_restore_done_for=today,
        )
        return f"🔁 RESTORE_SL SUCCESS: {symbol} broker SL restored to sl_true={sl_true} ({source}).{self._ltp_suffix_for_symbol(symbol)}"

    def run_volatility_sl_schedule_tick(self) -> list[str]:
        """
        IST widen/restore tick driven by Config.VOL_SL_*.
        Returns human-readable lines suitable for admin notifications/logging.
        """
        lines: list[str] = []
        if not Config.VOL_SL_SCHEDULE_ENABLED:
            return lines

        now = self._now_ist()
        if Config.VOL_SL_SKIP_WEEKENDS and now.weekday() >= 5:
            return lines

        widen_t = self._parse_hhmm_ist(Config.VOL_SL_WIDEN_TIME_IST)
        restore_t = self._parse_hhmm_ist(Config.VOL_SL_RESTORE_TIME_IST)
        if not widen_t or not restore_t:
            lines.append("⚠️ VOL_SL schedule: invalid VOL_SL_WIDEN_TIME_IST / VOL_SL_RESTORE_TIME_IST (expected HH:MM IST).")
            return lines

        hm = now.strftime("%H:%M")
        want_widen = hm == f"{widen_t[0]:02d}:{widen_t[1]:02d}"
        want_restore = hm == f"{restore_t[0]:02d}:{restore_t[1]:02d}"
        if not want_widen and not want_restore:
            return lines

        today = now.date().isoformat()
        pct = float(Config.VOL_SL_WIDEN_PCT or 0.0)
        trades = self._load_active_trades()
        for symbol, raw in trades.items():
            rec = self._normalize_active_trade_record(raw)
            oid = str(rec.get("smart_order_id") or "")
            if not oid or oid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                continue

            _, qty = self._get_open_position_for_symbol(symbol)
            if qty == 0:
                continue

            sl_true = float(rec.get("sl_true") or rec.get("stop_loss") or 0)
            if sl_true <= 0:
                continue

            if want_widen:
                if str(rec.get("vol_sl_widen_done_for") or "") == today:
                    continue
                widened = self._widened_sl_from_true(symbol, sl_true, pct)
                if widened is None:
                    lines.append(f"⚠️ VOL_SL widen skipped for {symbol}: could not compute widened SL.")
                    continue
                if not self.client:
                    lines.append("⚠️ VOL_SL widen skipped: Groww client not initialized.")
                    return lines
                try:
                    logger.info(
                        f"[VOL_SL] Widen schedule: {symbol} sl_true={sl_true} pct={pct} → broker_sl={widened} "
                        f"(smart_order_id={oid})."
                    )
                    widened = self._modify_oco_sl(symbol, oid, widened)
                    self._update_active_trade_levels(
                        symbol,
                        stop_loss=widened,
                        sl_true=sl_true,
                        sl_widened=widened,
                        sl_widen_pct=pct,
                        sl_mode="WIDENED",
                        vol_sl_needs_restore=True,
                        vol_sl_widen_done_for=today,
                    )
                    lines.append(
                        f"🛡️ VOL_SL WIDEN: `{symbol}` broker SL → `{widened}` "
                        f"(sl_true `{sl_true}`, pct `{pct}`)."
                    )
                except Exception as e:
                    logger.error(f"[VOL_SL] Widen failed for {symbol}: {e}", exc_info=True)
                    lines.append(f"❌ VOL_SL widen failed for `{symbol}`: {e}")
                rec = self._get_active_trade_record(symbol) or rec

            if want_restore:
                if str(rec.get("vol_sl_restore_done_for") or "") == today:
                    continue
                if not bool(rec.get("vol_sl_needs_restore")):
                    continue
                if not self.client:
                    lines.append("⚠️ VOL_SL restore skipped: Groww client not initialized.")
                    return lines
                try:
                    msg = self.restore_channel_sl(symbol, source="schedule")
                    lines.append(msg)
                except Exception as e:
                    logger.error(f"[VOL_SL] Restore failed for {symbol}: {e}", exc_info=True)
                    lines.append(f"❌ VOL_SL restore failed for `{symbol}`: {e}")

        return lines


    @staticmethod
    def _extract_smart_order_id(resp: dict, fallback: str | None = None) -> str | None:
        """
        Best-effort smart-order id extraction across SDK response shapes.
        """
        if not isinstance(resp, dict):
            return fallback
        for k in ("smart_order_id", "smartOrderId", "groww_order_id", "growwOrderId", "id"):
            v = resp.get(k)
            if v:
                return str(v)
        payload = resp.get("payload")
        if isinstance(payload, dict):
            for k in ("smart_order_id", "smartOrderId", "groww_order_id", "growwOrderId", "id"):
                v = payload.get(k)
                if v:
                    return str(v)
        return fallback

    @staticmethod
    def _parse_split_config(raw: str, expected_parts: int) -> list[int]:
        parts = [p.strip() for p in str(raw or "").split(":") if p.strip()]
        if len(parts) != expected_parts:
            raise ValueError(f"Expected {expected_parts} ratio parts, got {len(parts)}")
        vals = [int(p) for p in parts]
        if any(v <= 0 for v in vals):
            raise ValueError("Ratio values must be positive")
        if sum(vals) != 100:
            raise ValueError("Ratio values must sum to 100")
        return vals

    @classmethod
    def _signal_targets(cls, trade_data: dict) -> list[float]:
        out: list[float] = []
        raw_many = trade_data.get("targets")
        if isinstance(raw_many, list):
            for x in raw_many:
                try:
                    v = float(x)
                except Exception:
                    continue
                if v > 0:
                    out.append(v)
        if not out:
            try:
                single = float(trade_data.get("target") or 0)
                if single > 0:
                    out = [single]
            except Exception:
                out = []
        return out[:3]

    @classmethod
    def _build_oco_legs_plan(
        cls,
        *,
        total_qty: int,
        lot_size: int,
        signal_targets: list[float],
    ) -> list[dict]:
        qty = int(total_qty or 0)
        if qty <= 0:
            return []
        targets = [float(t) for t in (signal_targets or []) if float(t or 0) > 0][:3]
        if not targets:
            return []
        if not Config.MULTI_TARGET_ENABLED or len(targets) == 1:
            return [{"qty": qty, "target_signal": float(targets[0]), "target_index": 1}]

        unit = int(lot_size or 0)
        if unit <= 0:
            unit = 1
        total_lots = max(1, qty // unit)
        if len(targets) == 2:
            ratios = cls._parse_split_config(Config.TARGET_SPLIT_2, 2)
            t1 = min(int(math.ceil(total_lots * ratios[0] / 100.0)), total_lots)
            t2 = total_lots - t1
            lot_splits = [t1, t2]
        else:
            ratios = cls._parse_split_config(Config.TARGET_SPLIT_3, 3)
            t1 = min(int(math.ceil(total_lots * ratios[0] / 100.0)), total_lots)
            rem = total_lots - t1
            t2 = min(int(math.ceil(total_lots * ratios[1] / 100.0)), rem)
            t3 = total_lots - (t1 + t2)
            lot_splits = [t1, t2, t3]

        plans: list[dict] = []
        used_qty = 0
        for i, lots in enumerate(lot_splits):
            q = int(lots) * unit
            if q <= 0:
                continue
            plans.append({"qty": q, "target_signal": float(targets[i]), "target_index": i + 1})
            used_qty += q
        if not plans:
            return [{"qty": qty, "target_signal": float(targets[0]), "target_index": 1}]
        if used_qty < qty:
            plans[0]["qty"] = int(plans[0]["qty"]) + int(qty - used_qty)
        elif used_qty > qty:
            overflow = used_qty - qty
            plans[-1]["qty"] = max(0, int(plans[-1]["qty"]) - int(overflow))
            plans = [p for p in plans if int(p.get("qty") or 0) > 0]
        return plans

    def _load_pending_management(self) -> list[dict]:
        if not os.path.exists(self.pending_management_file):
            return []
        try:
            with open(self.pending_management_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _save_pending_management(self, items: list[dict]):
        with open(self.pending_management_file, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=4)

    def _market_window_state_ist(self) -> str:
        now = self._now_ist()
        if now.weekday() >= 5:
            return "closed"
        ot = self._parse_hhmm_ist(Config.MARKET_OPEN_TIME_IST)
        ct = self._parse_hhmm_ist(Config.MARKET_CLOSE_TIME_IST)
        if not ot or not ct:
            return "open"
        open_dt = now.replace(hour=ot[0], minute=ot[1], second=0, microsecond=0)
        close_dt = now.replace(hour=ct[0], minute=ct[1], second=0, microsecond=0)
        if now < open_dt:
            return "preopen"
        if now > close_dt:
            return "closed"
        return "open"

    @staticmethod
    def _mgmt_priority(action: str) -> int:
        a = str(action or "").upper().strip()
        if a == "EXIT_ALL":
            return 0
        if a in ("TRAIL_SL", "RESTORE_SL"):
            return 1
        return 2

    @staticmethod
    def _is_transient_mgmt_error(err_msg: str) -> bool:
        t = str(err_msg or "").lower()
        signals = (
            "market",
            "session",
            "closed",
            "open",
            "pre-open",
            "preopen",
            "timed out",
            "timeout",
            "too many requests",
            "temporarily",
            "try again",
            "network",
            "connection",
            "502",
            "503",
            "504",
        )
        return any(s in t for s in signals)

    @staticmethod
    def _is_nonretryable_mgmt_error(err_msg: str) -> bool:
        t = str(err_msg or "").lower()
        hard = (
            "no active trade",
            "could not find an open order id",
            "missing",
            "invalid",
            "not found",
            "rejected",
            "insufficient",
        )
        return any(s in t for s in hard)

    def _build_mgmt_queue_item(self, *, symbol: str, action: str, payload: dict, reason: str) -> dict:
        now_ts = time.time()
        action_u = str(action or "").upper().strip()
        normalize_value = ""
        if action_u == "TRAIL_SL":
            normalize_value = f"sl:{self._num_or(payload.get('stop_loss'), 0.0):.4f}"
        elif action_u == "UPDATE_TGT":
            normalize_value = f"tgt:{self._num_or(payload.get('target'), 0.0):.4f}"
        dedupe_key = f"{symbol}|{action_u}|{normalize_value}|{self._today_ist_iso()}"
        return {
            "id": str(uuid.uuid4())[:12],
            "symbol": str(symbol),
            "action": action_u,
            "payload": copy.deepcopy(payload),
            "dedupe_key": dedupe_key,
            "priority": self._mgmt_priority(action_u),
            "attempt_count": 0,
            "created_at": now_ts,
            "next_attempt_at": now_ts,
            "expires_at": now_ts + (Config.MGMT_QUEUE_TTL_MIN * 60),
            "last_error": str(reason or ""),
        }

    def _enqueue_management_action(self, *, symbol: str, action: str, payload: dict, reason: str) -> str:
        if not Config.MGMT_QUEUE_ENABLED:
            return ""
        action_u = str(action or "").upper().strip()
        item = self._build_mgmt_queue_item(symbol=symbol, action=action_u, payload=payload, reason=reason)
        items = self._load_pending_management()
        for it in items:
            if str(it.get("dedupe_key") or "") == str(item.get("dedupe_key") or ""):
                return str(it.get("id") or "")

        # Supersession per symbol for mutable actions.
        filtered = []
        for it in items:
            same_symbol = str(it.get("symbol") or "") == str(symbol)
            ia = str(it.get("action") or "").upper()
            if same_symbol and action_u == "EXIT_ALL":
                # EXIT supersedes all other queued management intents for symbol.
                continue
            if same_symbol and action_u in ("TRAIL_SL", "UPDATE_TGT") and ia == action_u:
                continue
            filtered.append(it)
        filtered.append(item)
        filtered.sort(key=lambda x: (int(x.get("priority", 9)), float(x.get("created_at", 0))))
        self._save_pending_management(filtered)
        return item["id"]

    def process_management_queue(self) -> list[str]:
        out: list[str] = []
        if not Config.MGMT_QUEUE_ENABLED:
            return out
        items = self._load_pending_management()
        if not items:
            return out
        now_ts = time.time()
        state = self._market_window_state_ist()
        kept: list[dict] = []
        in_flight_symbols: set[str] = set()

        items.sort(key=lambda x: (int(x.get("priority", 9)), float(x.get("created_at", 0))))
        for it in items:
            symbol = str(it.get("symbol") or "")
            action = str(it.get("action") or "").upper()
            if not symbol or not action:
                continue
            if symbol in in_flight_symbols:
                kept.append(it)
                continue
            if now_ts < float(it.get("next_attempt_at", 0)):
                kept.append(it)
                continue
            if now_ts > float(it.get("expires_at", 0)):
                out.append(f"⌛ MGMT queue expired: {action} {symbol} (id={it.get('id')}).")
                continue
            if state != "open":
                # Keep waiting until open; do not consume retry budget.
                kept.append(it)
                continue

            in_flight_symbols.add(symbol)
            payload = dict(it.get("payload") or {})
            payload["action"] = action
            payload["stock"] = symbol
            payload["_from_mgmt_queue"] = True
            msg = self.execute_trade(payload)
            if isinstance(msg, str) and (" SUCCESS" in msg or "[DRY RUN]" in msg or "already flat" in msg.lower()):
                out.append(f"✅ MGMT queued action executed: {action} {symbol}.")
                continue

            err = str(msg or "unknown management execution error")
            attempt_count = int(it.get("attempt_count") or 0) + 1
            if attempt_count >= int(Config.MGMT_QUEUE_MAX_RETRIES) or self._is_nonretryable_mgmt_error(err):
                out.append(f"❌ MGMT queue dropped: {action} {symbol} after {attempt_count} attempts. Last: {err}")
                continue
            backoff = min(300, 2 ** min(attempt_count, 8))
            it["attempt_count"] = attempt_count
            it["last_error"] = err
            it["next_attempt_at"] = now_ts + backoff
            kept.append(it)
            out.append(f"⏳ MGMT queue retry scheduled: {action} {symbol} in {backoff}s (attempt {attempt_count}).")

        kept.sort(key=lambda x: (int(x.get("priority", 9)), float(x.get("created_at", 0))))
        self._save_pending_management(kept)
        return out


    def _load_pending(self):
        if not os.path.exists(self.pending_trades_file):
            return []
        try:
            with open(self.pending_trades_file, "r") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _save_pending(self, items):
        with open(self.pending_trades_file, "w") as f:
            json.dump(items, f, indent=4)

    def _append_pending(self, item):
        items = self._load_pending()
        items.append(item)
        self._save_pending(items)

    def _variant_list(self, symbol: str) -> list[str]:
        variants = [symbol]
        m = re.match(r"^(?P<underlying>[A-Z&_\-]+?)(?P<rest>\d.*)$", symbol)
        if m:
            underlying = m.group("underlying")
            rest = m.group("rest")
            underlying_variants = {underlying}
            underlying_variants.add(underlying.replace("&", ""))
            underlying_variants.add(underlying.replace("&", "_"))
            underlying_variants.add(underlying.replace("_", ""))
            underlying_variants.add(underlying.replace("-", ""))
            if underlying.endswith("H") and len(underlying) > 1:
                underlying_variants.add(underlying[:-1])
                underlying_variants.add(underlying.replace("&", "")[:-1])
                underlying_variants.add(underlying.replace("&", "_")[:-1])
                underlying_variants.add(underlying.replace("_", "")[:-1])
            for u in list(underlying_variants):
                if u:
                    variants.append(f"{u}{rest}")
        sc_variants = []
        for v in variants:
            if "&" in v:
                sc_variants.append(v.replace("&", "_"))
                sc_variants.append(v.replace("&", ""))
            if "_" in v:
                sc_variants.append(v.replace("_", "&"))
                sc_variants.append(v.replace("_", ""))
        variants.extend(sc_variants)
        if len(symbol) > 5:
            months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
            for i, mo in enumerate(months):
                if mo in symbol:
                    month_num = str(i + 1)
                    m_variants = []
                    for v in variants:
                        if mo in v:
                            m_variants.append(v.replace(mo, month_num))
                    variants.extend(m_variants)
                    break
        return list(dict.fromkeys(variants))

    def _resolve_option_instrument(self, trade_data: dict):
        """
        Returns (exchange, trading_symbol, segment, instrument_dict) or
        (None, None, None, None).
        """
        symbol = trade_data.get("stock")
        if not symbol:
            return None, None, None, None
        self.warm_instruments_cache()
        variants = self._variant_list(symbol)
        for sym_variant in variants:
            for ex in ["NSE", "BSE"]:
                try:
                    res = self._get_instrument(exchange=ex, trading_symbol=sym_variant)
                    if res and res.get("segment"):
                        return res["exchange"], sym_variant, res["segment"], res
                except Exception:
                    continue
        return None, None, None, None

    @staticmethod
    def _extract_lot_size(instrument: dict | None) -> int | None:
        if not isinstance(instrument, dict):
            return None
        for key in ("lot_size", "lotSize", "quantity_lot_size", "qty_lot_size", "lot"):
            val = instrument.get(key)
            try:
                v = int(float(val))
                if v > 0:
                    return v
            except Exception:
                continue
        return None

    @classmethod
    def _is_index_option_trade(cls, trade_data: dict) -> bool:
        candidates = []
        for k in ("official_symbol", "underlying", "stock", "trading_symbol"):
            v = str(trade_data.get(k) or "").upper().strip()
            if v:
                candidates.append(v)
        for raw in candidates:
            letters = re.match(r"^[A-Z]+", raw)
            head = letters.group(0) if letters else raw
            if head in cls.INDEX_UNDERLYINGS:
                return True
            for idx in cls.INDEX_UNDERLYINGS:
                if raw.startswith(idx):
                    return True
        return False

    def _segment_lots_caps(self, trade_data: dict) -> tuple[int, int]:
        if self._is_index_option_trade(trade_data):
            return int(Config.INDEX_MIN_LOTS), int(Config.INDEX_MAX_LOTS)
        return int(Config.STOCK_MIN_LOTS), int(Config.STOCK_MAX_LOTS)

    @staticmethod
    def _is_buy_entry_trade(trade_data: dict) -> bool:
        return str(trade_data.get("action") or "").upper().strip() == "BUY"

    def _entry_ref_for_risk(self, trade_data: dict) -> float:
        # Conservative sizing for BUY: use worst-case entry in range (max).
        er = trade_data.get("entry_range") or [0, 0]
        if isinstance(er, list) and len(er) >= 2:
            try:
                hi = float(er[1])
                if hi > 0:
                    return hi
            except Exception:
                pass
        return self._num_or(trade_data.get("buy_range_max"), 0.0)

    def _open_portfolio_risk_inr(self) -> float:
        trades = self._load_active_trades()
        total = 0.0
        for _, raw in trades.items():
            rec = self._normalize_active_trade_record(raw)
            side = str(rec.get("entry_side") or "BUY").upper()
            if side != "BUY":
                continue
            entry = self._num_or(rec.get("entry_avg_price"), 0.0)
            sl = self._num_or(rec.get("sl_true"), self._num_or(rec.get("stop_loss"), 0.0))
            qty = int(self._num_or(rec.get("entry_qty"), 0))
            if entry <= 0 or sl <= 0 or qty <= 0:
                continue
            risk_pts = max(entry - sl, 0.0)
            total += risk_pts * qty
        return total

    def _risk_based_lots(self, trade_data: dict, instrument: dict | None) -> tuple[int, dict]:
        diag = {
            "mode": "risk",
            "reason": "",
            "risk_budget_inr": 0.0,
            "risk_per_lot_inr": 0.0,
            "open_portfolio_risk_inr": 0.0,
            "entry_ref": 0.0,
            "sl": 0.0,
            "raw_lots": 0,
            "used_lots": 0,
        }
        lot_size = self._extract_lot_size(instrument) or int(self._num_or(trade_data.get("lot_size"), 0))
        if lot_size <= 0:
            diag["reason"] = "missing_lot_size"
            return 0, diag
        if not self._is_buy_entry_trade(trade_data):
            diag["reason"] = "non_buy_entry"
            return 0, diag
        entry_ref = self._entry_ref_for_risk(trade_data)
        sl = self._num_or(trade_data.get("stop_loss"), 0.0)
        if entry_ref <= 0 or sl <= 0 or entry_ref <= sl:
            diag["reason"] = "invalid_entry_or_sl"
            return 0, diag

        cap = float(Config.ACCOUNT_CAPITAL_INR)
        if cap <= 0:
            diag["reason"] = "missing_account_capital"
            return 0, diag
        per_trade_budget = cap * float(Config.RISK_PCT_PER_TRADE)
        portfolio_budget = cap * float(Config.MAX_TOTAL_OPEN_RISK_PCT)
        open_risk = self._open_portfolio_risk_inr()
        remaining = max(portfolio_budget - open_risk, 0.0)
        effective_budget = min(per_trade_budget, remaining)

        risk_per_lot = (entry_ref - sl) * float(lot_size)
        if risk_per_lot <= 0 or effective_budget <= 0:
            diag["reason"] = "no_budget_or_invalid_risk"
            diag["risk_budget_inr"] = effective_budget
            diag["risk_per_lot_inr"] = risk_per_lot
            diag["open_portfolio_risk_inr"] = open_risk
            diag["entry_ref"] = entry_ref
            diag["sl"] = sl
            return 0, diag

        raw_lots = int(effective_budget // risk_per_lot)
        min_lots, max_lots = self._segment_lots_caps(trade_data)
        used_lots = raw_lots
        if used_lots > 0:
            used_lots = max(min_lots, min(max_lots, used_lots))

        diag.update(
            {
                "risk_budget_inr": effective_budget,
                "risk_per_lot_inr": risk_per_lot,
                "open_portfolio_risk_inr": open_risk,
                "entry_ref": entry_ref,
                "sl": sl,
                "raw_lots": raw_lots,
                "used_lots": used_lots,
            }
        )
        return used_lots, diag

    def _compute_order_quantity(self, trade_data: dict, instrument: dict | None = None) -> int:
        """
        quantity = lot_size * (INDEX_ORDER_LOTS_COUNT or STOCK_ORDER_LOTS_COUNT).
        Falls back to trade_data.quantity, then Config.ORDER_QUANTITY.
        """
        explicit = trade_data.get("quantity")
        try:
            q = int(float(explicit))
            if q > 0:
                return q
        except Exception:
            pass

        lot_size = self._extract_lot_size(instrument)
        if lot_size:
            force_fixed = str(trade_data.get("size_override_mode") or "").upper().strip() == "FIXED"
            sizing_mode = str(Config.POSITION_SIZING_MODE or "fixed").strip().lower()
            if force_fixed:
                sizing_mode = "fixed"
            if sizing_mode == "risk":
                lots, diag = self._risk_based_lots(trade_data, instrument)
                if lots > 0:
                    trade_data["sizing_mode_used"] = "risk"
                    trade_data["sizing_diag"] = diag
                    return int(lot_size) * int(lots)
                # Risk mode could not size; follow configured fallback.
                fallback = str(Config.RISK_MODE_ON_ZERO_LOTS or "fixed").strip().lower()
                if fallback != "fixed":
                    trade_data["sizing_mode_used"] = "risk_skip"
                    trade_data["sizing_diag"] = diag
                    return 0
                trade_data["sizing_mode_used"] = "fixed_fallback"
                trade_data["sizing_diag"] = diag
            if self._is_index_option_trade(trade_data):
                lots = max(1, int(Config.INDEX_ORDER_LOTS_COUNT))
            else:
                lots = max(1, int(Config.STOCK_ORDER_LOTS_COUNT))
            trade_data.setdefault("sizing_mode_used", "fixed")
            return lot_size * lots

        return int(Config.ORDER_QUANTITY)

    def _fetch_ltp(self, exchange: str, trading_symbol: str, segment: str) -> float:
        if self.client is None:
            return 0.0
        try:
            ltp_res = self.client.get_ltp(
                exchange_trading_symbols=f"{exchange}_{trading_symbol}", segment=segment
            )
            v = ltp_res.get(f"{exchange}_{trading_symbol}", ltp_res.get("ltp", 0))
            return float(v) if v else 0.0
        except Exception as e:
            logger.warning(f"get_ltp failed for {exchange}_{trading_symbol} ({segment}): {e}")
            return 0.0

    def _ltp_suffix_for_symbol(self, symbol: str) -> str:
        """Best-effort LTP suffix for order-related user alerts."""
        if not symbol or self.client is None:
            return ""
        try:
            pos, _ = self._get_open_position_for_symbol(symbol)
            exchange = str((pos or {}).get("exchange") or "NSE")
            segment = str((pos or {}).get("segment") or self.SMART_SEGMENT)
            ltp = self._fetch_ltp(exchange, symbol, segment)
            if ltp > 0:
                return f" | LTP={ltp:.2f}"
        except Exception:
            return ""
        return ""

    def _ltp_in_range(self, trade_data: dict, ltp: float) -> bool:
        if not ltp:
            return False
        low, high = trade_data.get("entry_range", [0, 999999])
        return low <= ltp <= high

    @staticmethod
    def _range_high(trade_data: dict) -> float | None:
        er = trade_data.get("entry_range")
        if not er or len(er) < 2:
            return None
        try:
            return float(er[1])
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _range_low(trade_data: dict) -> float | None:
        er = trade_data.get("entry_range")
        if not er or len(er) < 2:
            return None
        try:
            return float(er[0])
        except (TypeError, ValueError):
            return None

    def _buy_ltp_above_range_max(self, trade_data: dict, ltp: float) -> bool:
        """BUY only: premium already above signal max — do not chase (no limit / no LTP queue)."""
        if (trade_data.get("action") or "BUY").upper() != "BUY":
            return False
        high = self._range_high(trade_data)
        if high is None:
            return False
        return ltp > high

    def _sell_ltp_below_range_min(self, trade_data: dict, ltp: float) -> bool:
        """SELL only: premium below signal min — too little credit (no limit / no LTP queue)."""
        if (trade_data.get("action") or "BUY").upper() != "SELL":
            return False
        low = self._range_low(trade_data)
        if low is None:
            return False
        return ltp < low

    @staticmethod
    def _limit_price_for_single_entry(trade_data: dict) -> float | None:
        """BUY: max pay = range low (patient). SELL: min credit = range low."""
        er = trade_data.get("entry_range") or [0, 0]
        if len(er) < 2:
            return None
        low, high = float(er[0]), float(er[1])
        action = (trade_data.get("action") or "BUY").upper()
        if action == "BUY":
            return low
        if action == "SELL":
            return low
        return None

    @staticmethod
    def _order_status_upper_blob(resp: dict) -> str:
        if not resp:
            return ""
        try:
            return json.dumps(resp).upper()
        except Exception:
            return str(resp).upper()

    @staticmethod
    def _walk_order_status(resp) -> str | None:
        """'good' = filled, 'bad' = terminal failure, None = still open / unknown."""
        bad_kw = ("REJECT", "CANCEL", "FAIL", "EXPIRE")
        good_kw = ("FILL", "COMPLETE", "EXECUTE", "DONE")

        def walk(o):
            if isinstance(o, dict):
                for k, v in o.items():
                    lk = str(k).lower()
                    if lk in (
                        "status",
                        "order_status",
                        "order_status_message",
                        "oms_order_status",
                        "order_state",
                    ) and isinstance(v, str):
                        u = v.upper()
                        if any(b in u for b in bad_kw):
                            return "bad"
                        if any(g in u for g in good_kw):
                            return "good"
                    r = walk(v)
                    if r:
                        return r
            elif isinstance(o, list):
                for x in o:
                    r = walk(x)
                    if r:
                        return r
            return None

        return walk(resp)

    def _order_status_filled(self, resp: dict) -> bool:
        return self._walk_order_status(resp) == "good"

    def _order_status_bad_terminal(self, resp: dict) -> bool:
        return self._walk_order_status(resp) == "bad"

    def _attach_oco_after_buy(
        self,
        trade_data: dict,
        exchange: str,
        symbol: str,
        segment: str,
        product_type: str = "NRML",
        *,
        entry_order_id: str | None = None,
        entry_avg_price: float | None = None,
    ) -> str | None:
        if not self._should_attach_oco(trade_data):
            return None
        signal_targets = self._signal_targets(trade_data)
        if not signal_targets:
            return None
        broker_sl = self._round_price_to_tick(float(trade_data["stop_loss"]), mode="nearest")
        if broker_sl <= 0:
            raise RuntimeError(f"Invalid stop loss after tick-rounding: {trade_data.get('stop_loss')}")
        quantity = int(trade_data.get("quantity") or self._compute_order_quantity(trade_data))
        lot_size = int(trade_data.get("lot_size") or 0)
        try:
            plan = self._build_oco_legs_plan(total_qty=quantity, lot_size=lot_size, signal_targets=signal_targets)
        except Exception as e:
            logger.warning(f"Multi-target split config invalid; falling back to single target OCO: {e}")
            plan = [{"qty": quantity, "target_signal": float(signal_targets[0]), "target_index": 1}]
        if not plan:
            return None

        created_legs: list[dict] = []
        for leg in plan:
            leg_qty = int(leg["qty"])
            leg_tgt_signal = float(leg["target_signal"])
            eff_tgt = self._take_profit_order_price(leg_tgt_signal)
            if eff_tgt <= 0:
                raise RuntimeError(f"Invalid buffered target for leg target={leg_tgt_signal}")
            oco_ref = str(uuid.uuid4())[:15]
            logger.info(
                f"OCO leg create {symbol}: qty={leg_qty}, target(signal={leg_tgt_signal}, broker={eff_tgt}), "
                f"sl={broker_sl}"
            )
            oco_resp = self.client.create_smart_order(
                smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                reference_id=oco_ref,
                segment=segment,
                trading_symbol=symbol,
                quantity=leg_qty,
                net_position_quantity=leg_qty,
                transaction_type="SELL",
                product_type=product_type,
                exchange=exchange,
                duration="DAY",
                target={
                    "trigger_price": eff_tgt,
                    "price": eff_tgt,
                    "order_type": "LIMIT",
                },
                stop_loss={
                    "trigger_price": float(broker_sl),
                    "price": float(broker_sl),
                    "order_type": "SL",
                },
            )
            smart_order_id = self._extract_smart_order_id(oco_resp, fallback=oco_ref)
            created_legs.append(
                {
                    "smart_order_id": str(smart_order_id or ""),
                    "qty": leg_qty,
                    "target_signal": leg_tgt_signal,
                    "target_broker": eff_tgt,
                    "target_index": int(leg.get("target_index") or 0),
                    "status": "ACTIVE",
                }
            )

        primary_order_id = str(created_legs[0]["smart_order_id"])
        self._save_active_trade(
            symbol,
            primary_order_id,
            stop_loss=float(broker_sl),
            target=float(signal_targets[0]),
            entry_side=str(trade_data.get("action") or "BUY").upper(),
            entry_qty=int(trade_data.get("quantity") or 0),
            lot_size=int(trade_data.get("lot_size") or 0),
            entry_avg_price=float(entry_avg_price or 0.0),
            opened_at_ist=self._now_ist_iso(),
            entry_order_id=str(entry_order_id or ""),
            spread_group_id=str(trade_data.get("spread_group_id") or ""),
            oco_legs=created_legs,
        )
        leg_lines = [
            f"T{i+1} qty={int(l['qty'])}: signal {float(l['target_signal'])} → broker {float(l['target_broker'])}"
            for i, l in enumerate(created_legs)
        ]
        return "Targets:\n" + "\n".join(leg_lines) + f"\nSL: {trade_data['stop_loss']}"

    def _finalize_option_order(
        self,
        trade_data: dict,
        exchange: str,
        trading_symbol: str,
        segment: str,
        *,
        ltp: float | None = None,
    ) -> str:
        """Place MARKET order (+ optional OCO). Assumes range already OK or caller skipped check."""
        action = (trade_data.get("action") or "BUY").upper()
        ltp = self._fetch_ltp(exchange, trading_symbol, segment) if ltp is None else ltp
        symbol = trading_symbol

        if not self.place_entry_orders:
            if action == "BUY":
                self._save_active_trade(
                    symbol,
                    self.DRY_RUN_PLACEHOLDER_ORDER_ID,
                    stop_loss=float(trade_data.get("stop_loss") or 0),
                    target=float(trade_data.get("target") or 0),
                    entry_side=action,
                    entry_qty=int(trade_data.get("quantity") or 0),
                    lot_size=int(trade_data.get("lot_size") or 0),
                    entry_avg_price=float(ltp or 0.0),
                    opened_at_ist=self._now_ist_iso(),
                    entry_order_id="",
                    spread_group_id=str(trade_data.get("spread_group_id") or ""),
                )
            low, high = trade_data.get("entry_range", [0, 0])
            return f"🧪 [DRY RUN] Validated {action} {symbol} @ {ltp} (range {low}-{high}). No entry order placed."

        product_type = "NRML"
        quantity = self._compute_order_quantity(trade_data)
        if quantity <= 0:
            diag = trade_data.get("sizing_diag") or {}
            return (
                f"⚠️ SKIP: computed quantity is 0 for {symbol}. "
                f"Sizing mode={trade_data.get('sizing_mode_used', 'unknown')} "
                f"reason={diag.get('reason', 'n/a')}."
            )
        trade_data["quantity"] = int(quantity)
        entry_response = self.client.place_order(
            validity="DAY",
            exchange=exchange,
            order_type="MARKET",
            product=product_type,
            quantity=quantity,
            segment=segment,
            trading_symbol=symbol,
            transaction_type=action,
            price=0.0,
        )
        order_id = entry_response.get("groww_order_id")
        if not order_id:
            logger.error(
                f"[STAGE 4] MARKET order rejected for {symbol}: no groww_order_id. "
                f"Broker response: {entry_response}"
            )
            return f"❌ FAILURE: {action} order for {symbol} failed. Broker rejected request."

        fill = self._fetch_order_fill(
            segment=segment,
            groww_order_id=str(order_id or ""),
            fallback_price=float(ltp or 0.0),
            fallback_qty=int(quantity),
        )
        oco_line = self._attach_oco_after_buy(
            trade_data,
            exchange,
            symbol,
            segment,
            product_type,
            entry_order_id=str(order_id or ""),
            entry_avg_price=float(fill.get("avg_price") or 0.0),
        )
        if oco_line:
            sizing_mode = str(trade_data.get("sizing_mode_used") or Config.POSITION_SIZING_MODE).upper()
            override_reason = str(trade_data.get("size_override_reason") or "").strip()
            sizing_line = (
                f"📦 Sizing: {sizing_mode}"
                + (f" (override: {override_reason})" if override_reason else "")
                + f", qty={quantity}"
            )
            rr_line = self._risk_reward_line(trade_data, qty=quantity, entry_ref=float(ltp or 0.0))
            return f"✅ SUCCESS: {action} {symbol} @ {ltp}.\n{sizing_line}\n{oco_line}\n{rr_line}"
        return f"✅ SUCCESS: {action} {symbol} @ {ltp}. (No OCO — spread leg or missing SL/target.)"

    def _after_limit_order_filled(
        self,
        trade_data: dict,
        exchange: str,
        symbol: str,
        segment: str,
        *,
        entry_order_id: str = "",
    ) -> str:
        """Entry already filled via LIMIT; attach OCO for BUY if configured."""
        action = (trade_data.get("action") or "BUY").upper()
        ltp = self._fetch_ltp(exchange, symbol, segment)
        if not self.place_entry_orders:
            return f"🧪 [DRY RUN] Limit would have filled for {symbol} @ ~{ltp}. (No OCO / entry writes.)"

        if action == "BUY":
            fill = self._fetch_order_fill(
                segment=segment,
                groww_order_id=entry_order_id,
                fallback_price=float(ltp or 0.0),
                fallback_qty=int(trade_data.get("quantity") or 0),
            )
            oco_line = self._attach_oco_after_buy(
                trade_data,
                exchange,
                symbol,
                segment,
                entry_order_id=entry_order_id,
                entry_avg_price=float(fill.get("avg_price") or 0.0),
            )
            rr_line = self._risk_reward_line(
                trade_data,
                qty=int(fill.get("filled_qty") or trade_data.get("quantity") or 0),
                entry_ref=float(fill.get("avg_price") or ltp or 0.0),
            )
            if oco_line:
                return f"✅ LIMIT BUY filled {symbol} @ ~{ltp}. {oco_line}\n{rr_line}"
            return f"✅ LIMIT BUY filled {symbol} @ ~{ltp}. (No OCO.)"
        return f"✅ LIMIT {action} filled {symbol} @ ~{ltp}."

    def _place_limit_and_queue(
        self,
        trade_data: dict,
        exchange: str,
        trading_symbol: str,
        segment: str,
        *,
        observed_ltp: float | None = None,
    ) -> str:
        """Resting LIMIT on broker; pending worker polls order status."""
        action = (trade_data.get("action") or "BUY").upper()
        lim = self._limit_price_for_single_entry(trade_data)
        low, high = trade_data.get("entry_range", [0, 0])
        if lim is None:
            if Config.ENTRY_QUEUE_ENABLED:
                return self._queue_single(trade_data, exchange, trading_symbol, segment)
            return f"⏳ SKIP: invalid entry range for limit ({low}-{high})."

        if not self.place_entry_orders:
            return self._queue_single(trade_data, exchange, trading_symbol, segment)

        qty = self._compute_order_quantity(trade_data)
        if qty <= 0:
            diag = trade_data.get("sizing_diag") or {}
            return (
                f"⚠️ SKIP: computed quantity is 0 for {trading_symbol}. "
                f"Sizing mode={trade_data.get('sizing_mode_used', 'unknown')} "
                f"reason={diag.get('reason', 'n/a')}."
            )
        trade_data["quantity"] = int(qty)
        entry_response = None
        try:
            entry_response = self.client.place_order(
                validity="DAY",
                exchange=exchange,
                order_type="LIMIT",
                product="NRML",
                quantity=qty,
                segment=segment,
                trading_symbol=trading_symbol,
                transaction_type=action,
                price=float(lim),
            )
            oid = entry_response.get("groww_order_id")
            if not oid:
                logger.error(
                    f"[STAGE 4] LIMIT order rejected for {trading_symbol}: no groww_order_id. "
                    f"Broker response: {entry_response}"
                )
                raise RuntimeError("no groww_order_id")
        except Exception as e:
            logger.error(
                f"LIMIT order failed for {trading_symbol}: {e}. "
                f"Response snapshot: {entry_response}"
            )
            if Config.ENTRY_QUEUE_ENABLED:
                return (
                    f"⚠️ LIMIT placement failed ({e}). Falling back to LTP queue.\n"
                    + self._queue_single(trade_data, exchange, trading_symbol, segment)
                )
            return f"❌ LIMIT order failed: {e}"

        item = {
            "id": str(uuid.uuid4())[:12],
            "kind": "limit_single",
            "created_at": time.time(),
            "expires_at": time.time() + Config.ENTRY_QUEUE_MAX_WAIT_MIN * 60,
            "trade_data": copy.deepcopy(trade_data),
            "exchange": exchange,
            "trading_symbol": trading_symbol,
            "segment": segment,
            "groww_order_id": oid,
            "limit_price": lim,
        }
        self._append_pending(item)
        ltp_text = f" | LTP={float(observed_ltp):.2f}" if observed_ltp is not None else ""
        return (
            f"📌 LIMIT {action} placed {trading_symbol} @ {lim} (range {low}-{high}{ltp_text}). "
            f"Polling order status every {Config.ENTRY_QUEUE_POLL_SEC}s; max {Config.ENTRY_QUEUE_MAX_WAIT_MIN} min."
        )

    @staticmethod
    def _pending_item_display_symbol(p: dict) -> str | None:
        """Best-effort symbol for admin messages (single, limit_single, spread)."""
        sym = p.get("trading_symbol")
        if sym:
            return str(sym)
        td = p.get("trade_data") or {}
        sym = td.get("trading_symbol") or td.get("stock")
        if sym:
            return str(sym)
        if p.get("kind") == "spread":
            idx = int(p.get("leg_index", 0))
            resolved = p.get("resolved_legs") or []
            if 0 <= idx < len(resolved):
                s = resolved[idx].get("trading_symbol")
                if s:
                    return str(s)
        return None

    def process_pending_queue(self) -> list[str]:
        """
        Poll queued entries; execute when LTP enters range. Returns admin-facing messages.
        """
        out = []
        items = self._load_pending()
        if not items:
            return out
        now = time.time()
        kept = []
        for p in items:
            exp = float(p.get("expires_at", 0))
            if now > exp:
                kid = p.get("id", "?")
                if p.get("kind") == "limit_single" and self.place_entry_orders and self.client:
                    oid = p.get("groww_order_id")
                    seg = p.get("segment")
                    if oid and seg:
                        try:
                            self.client.cancel_order(groww_order_id=oid, segment=seg)
                        except Exception as ce:
                            logger.debug(f"Cancel limit on expiry: {ce}")
                psym = self._pending_item_display_symbol(p)
                if psym:
                    out.append(f"⌛ Pending entry expired — symbol: {psym} (queue id={kid}).")
                else:
                    out.append(f"⌛ Pending entry expired (queue id={kid}).")
                continue
            kind = p.get("kind")
            try:
                if kind == "limit_single":
                    oid = p.get("groww_order_id")
                    seg = p.get("segment")
                    td = p.get("trade_data") or {}
                    ex, sym = p.get("exchange"), p.get("trading_symbol")
                    if not oid or not seg or not ex or not sym:
                        out.append("❌ Limit pending dropped (missing fields).")
                        continue
                    if not self.client:
                        kept.append(p)
                        continue
                    try:
                        st = self.client.get_order_status(segment=seg, groww_order_id=oid)
                    except Exception as e:
                        logger.warning(f"Order status error: {e}")
                        kept.append(p)
                        continue
                    if self._order_status_filled(st):
                        msg = self._after_limit_order_filled(td, ex, sym, seg, entry_order_id=str(oid))
                        out.append(msg)
                    elif self._order_status_bad_terminal(st):
                        blob = self._order_status_upper_blob(st)[:180]
                        out.append(f"❌ Limit order ended ({oid}): {blob}")
                    else:
                        kept.append(p)
                elif kind == "single":
                    td = p.get("trade_data") or {}
                    ex, sym, seg = p.get("exchange"), p.get("trading_symbol"), p.get("segment")
                    if not ex or not sym or not seg:
                        out.append("❌ Pending entry dropped (missing resolution).")
                        continue
                    ltp = self._fetch_ltp(ex, sym, seg)
                    low = td.get("buy_range_min") or (td.get("entry_range") or [0])[0]
                    high = td.get("buy_range_max") or ((td.get("entry_range") or [0, 0])[1])
                    if ltp and self._ltp_in_range(td, ltp):
                        msg = self._finalize_option_order(td, ex, sym, seg, ltp=ltp)
                        out.append(f"✅ Queued entry filled: {msg}")
                    elif not ltp:
                        logger.warning(
                            f"[PENDING] {sym}: LTP fetch returned 0 (no Groww client, bad symbol, or quote error). "
                            f"Skipping this poll. Expires in {max(0, int(exp - time.time()))}s."
                        )
                        kept.append(p)
                    else:
                        logger.info(
                            f"[PENDING] {sym}: LTP={ltp} not in range {low}-{high} — still waiting. "
                            f"Expires in {max(0, int(exp - time.time()))}s."
                        )
                        kept.append(p)
                elif kind == "spread":
                    legs = p.get("legs") or []
                    resolved = p.get("resolved_legs") or []
                    idx = int(p.get("leg_index", 0))
                    if idx >= len(legs) or len(resolved) != len(legs):
                        out.append("❌ Pending spread dropped (invalid state).")
                        continue
                    leg = legs[idx]
                    rex = resolved[idx]
                    ex, sym, seg = rex.get("exchange"), rex.get("trading_symbol"), rex.get("segment")
                    if not ex or not sym or not seg:
                        kept.append(p)
                        continue
                    ltp = self._fetch_ltp(ex, sym, seg)
                    if ltp and self._ltp_in_range(leg, ltp):
                        msg = self._finalize_option_order(leg, ex, sym, seg, ltp=ltp)
                        out.append(f"✅ Spread leg {idx + 1}/{len(legs)} (queued): {msg}")
                        idx += 1
                        if idx < len(legs):
                            p["leg_index"] = idx
                            kept.append(p)
                    else:
                        s_low = (leg.get("entry_range") or [0, 0])[0]
                        s_high = (leg.get("entry_range") or [0, 0])[1]
                        logger.info(
                            f"[PENDING] Spread leg {idx + 1}/{len(legs)} {sym}: "
                            f"LTP={ltp} not in range {s_low}-{s_high} — still waiting. "
                            f"Expires in {max(0, int(exp - time.time()))}s."
                        )
                        kept.append(p)
                else:
                    kept.append(p)
            except Exception as e:
                logger.error(f"Pending queue item error: {e}")
                kept.append(p)
        self._save_pending(kept)
        return out

    def _queue_single(
        self,
        trade_data: dict,
        exchange: str,
        trading_symbol: str,
        segment: str,
        *,
        observed_ltp: float | None = None,
    ) -> str:
        low, high = trade_data.get("entry_range", [0, 0])
        action = (trade_data.get("action") or "BUY").upper()
        item = {
            "id": str(uuid.uuid4())[:12],
            "kind": "single",
            "created_at": time.time(),
            "expires_at": time.time() + Config.ENTRY_QUEUE_MAX_WAIT_MIN * 60,
            "trade_data": copy.deepcopy(trade_data),
            "exchange": exchange,
            "trading_symbol": trading_symbol,
            "segment": segment,
        }
        self._append_pending(item)
        ltp_text = f" (LTP={float(observed_ltp):.2f})" if observed_ltp is not None else ""
        return (
            f"⏳ Queued {action} {trading_symbol}: LTP not in range {low}-{high} yet{ltp_text}. "
            f"Will retry every {Config.ENTRY_QUEUE_POLL_SEC}s for up to {Config.ENTRY_QUEUE_MAX_WAIT_MIN} min."
        )

    def _queue_spread(self, legs: list, start_index: int, resolved_legs: list) -> str:
        item = {
            "id": str(uuid.uuid4())[:12],
            "kind": "spread",
            "created_at": time.time(),
            "expires_at": time.time() + Config.ENTRY_QUEUE_MAX_WAIT_MIN * 60,
            "legs": copy.deepcopy(legs),
            "leg_index": start_index,
            "resolved_legs": copy.deepcopy(resolved_legs),
        }
        self._append_pending(item)
        return (
            f"⏳ Queued spread from leg {start_index + 1}/{len(legs)}: waiting for entry range. "
            f"Poll every {Config.ENTRY_QUEUE_POLL_SEC}s, max {Config.ENTRY_QUEUE_MAX_WAIT_MIN} min."
        )

    def _execute_spread(self, legs: list) -> str:
        if not legs:
            return "❌ No spread legs."
        spread_group_id = str(uuid.uuid4())[:12]
        for leg in legs:
            if isinstance(leg, dict):
                leg["spread_group_id"] = spread_group_id
        resolved = []
        for i, leg in enumerate(legs):
            ex, sym, seg, instrument = self._resolve_option_instrument(leg)
            if not ex:
                return f"❌ FAILURE: Could not resolve leg {i + 1} ({leg.get('stock')})."
            leg["lot_size"] = self._extract_lot_size(instrument) or leg.get("lot_size") or 0
            leg["quantity"] = self._compute_order_quantity(leg, instrument=instrument)
            resolved.append({"exchange": ex, "trading_symbol": sym, "segment": seg})
        parts = []
        for i, leg in enumerate(legs):
            ex, sym, seg = resolved[i]["exchange"], resolved[i]["trading_symbol"], resolved[i]["segment"]
            logger.info(f"Preparing spread leg {i + 1}: {leg.get('action')} {sym}")
            ltp = self._fetch_ltp(ex, sym, seg)
            if not ltp:
                parts.append(f"Leg {i + 1}: ⚠️ Could not read LTP — stopped spread here.")
                return "\n".join(parts)
            if self._buy_ltp_above_range_max(leg, ltp):
                rh = self._range_high(leg)
                parts.append(
                    f"Leg {i + 1}: ⏭️ SKIP (no chase): BUY {sym} @ {ltp} above max {rh}."
                )
                return "\n".join(parts)
            if self._sell_ltp_below_range_min(leg, ltp):
                rl = self._range_low(leg)
                parts.append(
                    f"Leg {i + 1}: ⏭️ SKIP (no chase): SELL {sym} @ {ltp} below min {rl}."
                )
                return "\n".join(parts)
            if not self._ltp_in_range(leg, ltp):
                if Config.ENTRY_QUEUE_ENABLED:
                    parts.append(self._queue_spread(legs, i, resolved))
                    return "\n".join(parts)
                low, high = leg.get("entry_range", [0, 999999])
                parts.append(f"Leg {i + 1}: ⏳ SKIP: {leg.get('action')} {sym} @ {ltp} outside {low}-{high}.")
                return "\n".join(parts)
            parts.append(f"Leg {i + 1}: {self._finalize_option_order(leg, ex, sym, seg, ltp=ltp)}")
        return "\n".join(parts)

    def _load_local_instrument_cache(self):
        """
        Loads the packaged Groww instruments list (CSV) from the installed growwapi package.
        This avoids network dependency on Groww's instruments download endpoint.
        """
        if self._instrument_cache is not None:
            return
        cache = {}
        try:
            import growwapi  # type: ignore

            pkg_dir = os.path.dirname(growwapi.__file__)
            csv_path = os.path.join(pkg_dir, "instruments.csv")
            if not os.path.exists(csv_path):
                self._instrument_cache = {}
                return

            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ex = row.get("exchange")
                    ts = row.get("trading_symbol")
                    if not ex or not ts:
                        continue
                    cache[(ex, ts)] = row

            self._instrument_cache = cache
        except Exception as e:
            logger.debug(f"Local instrument cache load failed: {e}")
            self._instrument_cache = {}

    def _get_instrument(self, exchange: str, trading_symbol: str):
        """
        Try GrowwAPI instrument lookup first; fall back to packaged instruments.csv cache.
        """
        if self.client is not None:
            try:
                return self.client.get_instrument_by_exchange_and_trading_symbol(
                    exchange=exchange, trading_symbol=trading_symbol
                )
            except Exception as e:
                # Fall back to local cache (commonly fails when instruments CSV download is blocked)
                logger.debug(f"Instrument lookup via API failed for {exchange}:{trading_symbol}: {e}")

        self._load_local_instrument_cache()
        return self._instrument_cache.get((exchange, trading_symbol)) if self._instrument_cache else None

    def warm_instruments_cache(self):
        """
        Pre-load instrument metadata so future order placement doesn't block on downloads.
        Safe to call multiple times.
        """
        # 1) Warm GrowwAPI's in-memory dataframe (may download once per process)
        if self.client is not None:
            try:
                self.client.get_all_instruments()
            except Exception as e:
                logger.debug(f"GrowwAPI instruments warm-up failed: {e}")

        # 2) Warm our local fallback cache (reads packaged CSV once per process)
        self._load_local_instrument_cache()

    @staticmethod
    def _take_profit_order_price(raw_signal_target: float) -> float:
        """Broker OCO target = signal target reduced by TARGET_BUFFER_PCT."""
        try:
            raw = float(raw_signal_target)
        except (TypeError, ValueError):
            return 0.0
        if raw <= 0:
            return 0.0
        pct = max(0.0, float(Config.TARGET_BUFFER_PCT))
        reduced = max(raw * (1.0 - pct), 0.0)
        return Trader._round_price_to_tick(reduced, mode="down")

    @staticmethod
    def _round_price_to_tick(price: float, *, mode: str = "nearest") -> float:
        """
        Normalize broker prices to valid tick-size decimals.
        mode:
          - 'down': floor to tick (useful for profit target sell-limit).
          - 'up': ceil to tick.
          - 'nearest': nearest tick.
        """
        try:
            p = float(price)
        except (TypeError, ValueError):
            return 0.0
        if p <= 0:
            return 0.0
        tick = max(0.01, float(Config.TARGET_PRICE_TICK_SIZE))
        steps = p / tick
        if mode == "down":
            n = math.floor(steps + 1e-9)
        elif mode == "up":
            n = math.ceil(steps - 1e-9)
        else:
            n = round(steps)
        out = n * tick
        return max(round(out, 4), 0.0)

    @staticmethod
    def _risk_reward_line(trade_data: dict, *, qty: int, entry_ref: float) -> str:
        try:
            sl = float(trade_data.get("stop_loss") or 0)
            tgt = float(trade_data.get("target") or 0)
            er = trade_data.get("entry_range") or [0, 0]
            range_hi = float(er[1]) if isinstance(er, list) and len(er) > 1 else 0.0
            basis = float(entry_ref or range_hi or 0.0)
            if basis <= 0:
                basis = range_hi
            risk_pts = max(basis - sl, 0.0)
            reward_pts = max(tgt - basis, 0.0)
            risk_inr = risk_pts * float(qty or 0)
            reward_inr = reward_pts * float(qty or 0)
            rr = (reward_pts / risk_pts) if risk_pts > 0 else 0.0
            return (
                f"📐 Risk/Reward: risk ₹{risk_inr:.2f} ({risk_pts:.2f} pts), "
                f"reward ₹{reward_inr:.2f} ({reward_pts:.2f} pts), R:R {rr:.2f}"
            )
        except Exception:
            return "📐 Risk/Reward: n/a"

    def _should_attach_oco(self, trade_data: dict) -> bool:
        if trade_data.get("spread_leg"):
            return False
        if trade_data.get("action") != "BUY":
            return False
        try:
            sl = float(trade_data.get("stop_loss") or 0)
            raw_tgt = float(trade_data.get("target") or 0)
            eff_tgt = Trader._take_profit_order_price(raw_tgt)
            return sl > 0 and raw_tgt > 0 and eff_tgt > 0
        except (TypeError, ValueError):
            return False

    def _execute_option_entry(self, trade_data: dict) -> str:
        """
        Resolve instrument, check LTP vs entry_range, queue or place BUY/SELL.
        """
        action = (trade_data.get("action") or "BUY").upper()
        symbol = trade_data.get("stock")
        if not symbol:
            return "❌ ERROR: missing symbol"

        if action not in ("BUY", "SELL"):
            return f"❌ ERROR: unsupported entry action {action}"

        logger.info(f"[STAGE 4] Resolving instrument for {action} {symbol}...")
        exchange, found_symbol, segment, instrument = self._resolve_option_instrument(trade_data)
        if not exchange:
            variants = self._variant_list(symbol)
            logger.warning(f"[STAGE 4] Instrument NOT found for {symbol}. Variants tried: {variants}")
            return f"❌ FAILURE: Could not find instrument details for {symbol} (Variants tried: {variants}) on Groww."

        lot_size = self._extract_lot_size(instrument)
        trade_data["lot_size"] = lot_size or trade_data.get("lot_size") or 0
        trade_data["quantity"] = self._compute_order_quantity(trade_data, instrument=instrument)

        logger.info(f"[STAGE 4] Instrument resolved: {found_symbol} | exchange={exchange} segment={segment}")
        ltp = self._fetch_ltp(exchange, found_symbol, segment)
        low, high = trade_data.get("entry_range", [0, 0])
        logger.info(f"[STAGE 4] LTP={ltp} | entry_range={low}-{high}")

        if not ltp:
            logger.warning(f"[STAGE 4] LTP fetch failed for {found_symbol}.")
            return "⚠️ Could not read LTP — cannot verify entry range. Not queued."

        if self._buy_ltp_above_range_max(trade_data, ltp):
            high = self._range_high(trade_data)
            logger.info(f"[STAGE 4] LTP {ltp} > max {high} — routing to pending flow (no chase).")
            if (
                Config.ENTRY_SINGLE_LIMIT_ORDER
                and not trade_data.get("spread_leg")
                and self.place_entry_orders
            ):
                return self._place_limit_and_queue(
                    trade_data, exchange, found_symbol, segment, observed_ltp=float(ltp)
                )
            if Config.ENTRY_QUEUE_ENABLED:
                return self._queue_single(
                    trade_data, exchange, found_symbol, segment, observed_ltp=float(ltp)
                )
            return (
                f"⏳ SKIP: BUY {found_symbol} @ {ltp} is above signal max {high}. "
                "Queue/limit disabled."
            )

        if self._sell_ltp_below_range_min(trade_data, ltp):
            low = self._range_low(trade_data)
            logger.info(f"[STAGE 4] LTP {ltp} < min {low} — routing to pending flow (no chase).")
            if (
                Config.ENTRY_SINGLE_LIMIT_ORDER
                and not trade_data.get("spread_leg")
                and self.place_entry_orders
            ):
                return self._place_limit_and_queue(
                    trade_data, exchange, found_symbol, segment, observed_ltp=float(ltp)
                )
            if Config.ENTRY_QUEUE_ENABLED:
                return self._queue_single(
                    trade_data, exchange, found_symbol, segment, observed_ltp=float(ltp)
                )
            return (
                f"⏳ SKIP: SELL {found_symbol} @ {ltp} is below signal min {low}. "
                "Queue/limit disabled."
            )

        if not self._ltp_in_range(trade_data, ltp):
            logger.info(f"[STAGE 4] LTP {ltp} outside range {low}-{high} — checking queue/limit config.")
            if (
                Config.ENTRY_SINGLE_LIMIT_ORDER
                and not trade_data.get("spread_leg")
                and self.place_entry_orders
            ):
                return self._place_limit_and_queue(
                    trade_data, exchange, found_symbol, segment, observed_ltp=float(ltp)
                )
            if Config.ENTRY_QUEUE_ENABLED:
                return self._queue_single(
                    trade_data, exchange, found_symbol, segment, observed_ltp=float(ltp)
                )
            return f"⏳ SKIP: {action} {found_symbol} @ {ltp} is outside range ({low}-{high})."

        logger.info(f"[STAGE 4] LTP {ltp} is IN range {low}-{high} — placing order.")
        return self._finalize_option_order(trade_data, exchange, found_symbol, segment)

    def execute_trade(self, trade_data):
        """
        Main entry point for trade execution.
        Returns a string summary of the action taken.
        """
        if trade_data.get("spread") and trade_data.get("legs"):
            logger.info(f"[STAGE 4] Executing SPREAD ({len(trade_data['legs'])} legs).")
            return self._execute_spread(trade_data["legs"])

        action = trade_data.get("action")
        symbol = trade_data.get("stock")
        
        if not symbol:
            logger.warning("[STAGE 4] execute_trade called with no symbol — skipping.")
            return None

        logger.info(f"[STAGE 4] execute_trade: action={action} symbol={symbol} "
                    f"dry_run={self.dry_run} place_entry_orders={self.place_entry_orders} "
                    f"entry_range={trade_data.get('entry_range')} "
                    f"sl={trade_data.get('stop_loss')} tgt={trade_data.get('target')}")

        try:
            if action == "WATCHLIST":
                self.warm_instruments_cache()
                return f"👀 Added {symbol} to watchlist. Instruments cache warmed for faster execution."

            # EXIT, TRAIL or TARGET UPDATE (symbol already resolved by StateEngine before reaching here)
            if action in ["EXIT_ALL", "TRAIL_SL", "RESTORE_SL", "UPDATE_TGT"]:
                from_queue = bool(trade_data.get("_from_mgmt_queue"))
                market_state = self._market_window_state_ist()
                if not from_queue and market_state != "open":
                    qid = self._enqueue_management_action(
                        symbol=symbol,
                        action=action,
                        payload=trade_data,
                        reason=f"market_{market_state}",
                    )
                    if qid:
                        return (
                            f"⏳ {action} queued for {symbol}: market is {market_state} (IST). "
                            f"Queue id={qid}.{self._ltp_suffix_for_symbol(symbol)}"
                        )
                rec_active = self._get_active_trade_record(symbol) or {}
                order_ids = self._oco_ids_from_record(rec_active)
                order_id = order_ids[0] if order_ids else None
                logger.info(f"[STAGE 4] Order IDs for {symbol}: {order_ids}")
                if not order_id:
                    logger.warning(f"[STAGE 4] No order ID found for {symbol} in active_trades.json.")
                    return f"⚠️ NO ACTIVE TRADE: Could not find an open Order ID for {symbol}.{self._ltp_suffix_for_symbol(symbol)}"

                # Simulated book row from dry-run entry — nothing exists on the broker to exit/modify.
                if order_id == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                    if action == "EXIT_ALL":
                        rec0 = self._get_active_trade_record(symbol) or {}
                        self._record_completed_trade(
                            symbol=symbol,
                            active_rec=rec0,
                            close_reason="EXIT_ALL_DRYRUN",
                            exit_price=self._num_or(rec0.get("entry_avg_price"), 0.0),
                            exit_qty=int(self._num_or(rec0.get("entry_qty"), 0)),
                        )
                        self._remove_active_trade(symbol)
                        logger.info(f"[STAGE 4] DRY RUN book: EXIT_ALL {symbol} — cleared local book only.")
                        return (
                            f"🧪 [DRY RUN] EXIT_ALL for {symbol}: removed from local active book "
                            f"(no broker order was placed).{self._ltp_suffix_for_symbol(symbol)}"
                        )
                    if action == "TRAIL_SL":
                        new_sl = float(trade_data["stop_loss"])
                        rec0 = self._get_active_trade_record(symbol) or {}
                        broker_sl = float(new_sl)
                        if bool(rec0.get("vol_sl_needs_restore")):
                            widened = self._widened_sl_from_true(
                                symbol, float(new_sl), float(rec0.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT)
                            )
                            if widened is None:
                                return (
                                    f"⚠️ TRAIL_SL skipped for {symbol}: could not compute widened broker SL "
                                    f"from sl_true={new_sl}.{self._ltp_suffix_for_symbol(symbol)}"
                                )
                            broker_sl = float(widened)
                        self._update_active_trade_levels(
                            symbol,
                            stop_loss=broker_sl,
                            sl_true=float(new_sl),
                            sl_widened=(broker_sl if bool(rec0.get("vol_sl_needs_restore")) else 0.0),
                            sl_mode=("WIDENED" if bool(rec0.get("vol_sl_needs_restore")) else "NORMAL"),
                            sl_widen_pct=float(rec0.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT),
                        )
                        logger.info(
                            f"[STAGE 4] DRY RUN book: TRAIL_SL {symbol} sl_true={new_sl} broker_sl={broker_sl} "
                            f"(not sent to broker)."
                        )
                        return (
                            f"🧪 [DRY RUN] TRAIL_SL for {symbol}: sl_true={new_sl}, broker_sl={broker_sl} "
                            f"(simulated; no OCO on broker for this symbol).{self._ltp_suffix_for_symbol(symbol)}"
                        )
                    if action == "RESTORE_SL":
                        return self.restore_channel_sl(symbol, source="signal")
                    raw_tgt = float(trade_data.get("target") or 0)
                    eff_tgt = self._take_profit_order_price(raw_tgt)
                    self._update_active_trade_levels(symbol, target=raw_tgt)
                    logger.info(
                        f"[STAGE 4] DRY RUN book: UPDATE_TGT {symbol} signal {raw_tgt} → broker {eff_tgt} "
                        f"(not sent to broker)."
                    )
                    return (
                        f"🧪 [DRY RUN] UPDATE_TGT for {symbol}: signal {raw_tgt} → would place {eff_tgt} "
                        f"(simulated; no OCO on broker for this symbol).{self._ltp_suffix_for_symbol(symbol)}"
                    )

                if not self.client:
                    return f"⚠️ NO GROWW CLIENT: Cannot {action} for {symbol}. Configure GROWW_API_KEY.{self._ltp_suffix_for_symbol(symbol)}"

                if action == "EXIT_ALL":
                    rec_before = self._get_active_trade_record(symbol) or {}
                    # 1) Cancel OCO protection (best effort).
                    for oid in order_ids:
                        try:
                            logger.info(
                                f"[STAGE 4] Calling Groww cancel_smart_order for {symbol} "
                                f"(segment={self.SMART_SEGMENT}, smart_order_type=OCO, id={oid})."
                            )
                            self.client.cancel_smart_order(
                                segment=self.SMART_SEGMENT,
                                smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                                smart_order_id=oid,
                            )
                        except Exception as e:
                            logger.warning(
                                f"[STAGE 4] EXIT_ALL cancel_smart_order warning for {symbol} id={oid}: {e}"
                            )

                    # 2) Flatten any remaining net position with MARKET order.
                    pos, qty = self._get_open_position_for_symbol(symbol)
                    if qty == 0:
                        self._record_completed_trade(
                            symbol=symbol,
                            active_rec=rec_before,
                            close_reason="EXIT_ALL_ALREADY_FLAT",
                            exit_price=self._num_or(rec_before.get("entry_avg_price"), 0.0),
                            exit_qty=int(self._num_or(rec_before.get("entry_qty"), 0)),
                        )
                        self._remove_active_trade(symbol)
                        logger.info(
                            f"[STAGE 4] EXIT SUCCESS: {symbol} has no open position after cancel; removed from active book."
                        )
                        return f"🚨 EXIT SUCCESS: {symbol} already flat; cleared local active trade.{self._ltp_suffix_for_symbol(symbol)}"

                    tx = "SELL" if qty > 0 else "BUY"
                    flat_qty = abs(int(qty))
                    seg = str((pos or {}).get("segment") or self.SMART_SEGMENT)
                    ex = str((pos or {}).get("exchange") or "NSE")
                    product = str(
                        (pos or {}).get("product")
                        or (pos or {}).get("product_type")
                        or (pos or {}).get("productType")
                        or "NRML"
                    ).upper()
                    logger.info(
                        f"[STAGE 4] EXIT_ALL flatten: placing MARKET {tx} {symbol} qty={flat_qty} "
                        f"(exchange={ex}, segment={seg}, product={product})."
                    )
                    exit_resp = self.client.place_order(
                        validity="DAY",
                        exchange=ex,
                        order_type="MARKET",
                        product=product,
                        quantity=flat_qty,
                        segment=seg,
                        trading_symbol=symbol,
                        transaction_type=tx,
                        price=0.0,
                    )

                    # 3) Verify flattened.
                    _, qty_after = self._get_open_position_for_symbol(symbol)
                    if qty_after != 0:
                        logger.warning(
                            f"[STAGE 4] EXIT_ALL partial/failed flatten for {symbol}: remaining qty={qty_after}."
                        )
                        return (
                            f"⚠️ EXIT PARTIAL: Cancelled protection for {symbol}, but position still open "
                            f"(remaining qty {qty_after}).{self._ltp_suffix_for_symbol(symbol)}"
                        )

                    self._remove_active_trade(symbol)
                    exit_order_id = str((exit_resp or {}).get("groww_order_id") or "")
                    fill = self._fetch_order_fill(
                        segment=seg,
                        groww_order_id=exit_order_id,
                        fallback_price=self._num_or((pos or {}).get("ltp"), 0.0),
                        fallback_qty=flat_qty,
                    )
                    self._record_completed_trade(
                        symbol=symbol,
                        active_rec=rec_before,
                        close_reason="EXIT_ALL",
                        exit_price=float(fill.get("avg_price") or 0.0),
                        exit_qty=int(fill.get("filled_qty") or flat_qty),
                        close_order_id=exit_order_id,
                    )
                    logger.info(
                        f"[STAGE 4] EXIT SUCCESS: {symbol} flattened and removed from active book."
                    )
                    return f"🚨 EXIT SUCCESS: Closed {symbol} position and cleared active trade.{self._ltp_suffix_for_symbol(symbol)}"
                
                elif action == "TRAIL_SL":
                    new_sl = float(trade_data["stop_loss"])
                    rec0 = self._get_active_trade_record(symbol) or {}
                    broker_sl = float(new_sl)
                    if bool(rec0.get("vol_sl_needs_restore")):
                        widened = self._widened_sl_from_true(
                            symbol, float(new_sl), float(rec0.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT)
                        )
                        if widened is None:
                            return (
                                f"⚠️ TRAIL_SL skipped for {symbol}: could not compute widened broker SL "
                                f"from sl_true={new_sl}.{self._ltp_suffix_for_symbol(symbol)}"
                            )
                        broker_sl = float(widened)
                    logger.info(
                        f"[STAGE 4] Calling Groww modify_smart_order for {symbol}: "
                        f"sl_true={new_sl} broker_sl={broker_sl}."
                    )
                    for oid in order_ids:
                        broker_sl = self._modify_oco_sl(symbol, oid, broker_sl)
                    self._update_active_trade_levels(
                        symbol,
                        stop_loss=broker_sl,
                        sl_true=float(new_sl),
                        sl_widened=(broker_sl if bool(rec0.get("vol_sl_needs_restore")) else 0.0),
                        sl_mode=("WIDENED" if bool(rec0.get("vol_sl_needs_restore")) else "NORMAL"),
                        sl_widen_pct=float(rec0.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT),
                    )
                    logger.info(f"[STAGE 4] TRAIL SUCCESS: {symbol} sl_true={new_sl} broker_sl={broker_sl}.")
                    return f"📈 TRAIL SUCCESS: Updated {symbol} SL — sl_true={new_sl}, broker_sl={broker_sl}.{self._ltp_suffix_for_symbol(symbol)}"

                elif action == "RESTORE_SL":
                    return self.restore_channel_sl(symbol, source="signal")

                elif action == "UPDATE_TGT":
                    raw_tgt = float(trade_data["target"])
                    eff_tgt = self._take_profit_order_price(raw_tgt)
                    if eff_tgt <= 0:
                        return (
                            f"⚠️ UPDATE_TGT skipped for {symbol}: buffered target must be > 0 "
                            f"(signal {raw_tgt}, TARGET_BUFFER_PCT={Config.TARGET_BUFFER_PCT}).{self._ltp_suffix_for_symbol(symbol)}"
                        )
                    logger.info(
                        f"[STAGE 4] Calling Groww modify_smart_order for {symbol}: "
                        f"target signal {raw_tgt} → broker {eff_tgt}."
                    )
                    rec_live = self._get_active_trade_record(symbol) or {}
                    legs = rec_live.get("oco_legs")
                    if isinstance(legs, list) and legs:
                        for leg in legs:
                            if not isinstance(leg, dict):
                                continue
                            oid = str(leg.get("smart_order_id") or "")
                            if not oid:
                                continue
                            leg_target_signal = float(leg.get("target_signal") or raw_tgt)
                            leg_target_broker = self._take_profit_order_price(leg_target_signal)
                            if leg_target_broker <= 0:
                                leg_target_broker = eff_tgt
                            self._modify_oco_target(oid, leg_target_broker)
                            leg["target_broker"] = float(leg_target_broker)
                        self._save_active_trade(
                            symbol,
                            order_id,
                            target=raw_tgt,
                            oco_legs=legs,
                        )
                    else:
                        for oid in order_ids:
                            self._modify_oco_target(oid, eff_tgt)
                    self._update_active_trade_levels(symbol, target=raw_tgt)
                    logger.info(
                        f"[STAGE 4] UPDATE_TGT SUCCESS: {symbol} broker target {eff_tgt} (signal {raw_tgt})."
                    )
                    return (
                        f"🎯 UPDATE_TGT SUCCESS: {symbol} target {eff_tgt} (signal {raw_tgt}).{self._ltp_suffix_for_symbol(symbol)}"
                    )

            # BUY / SELL option entry (single leg)
            logger.info(f"[STAGE 4] Routing to option entry executor for {action} {symbol}.")
            return self._execute_option_entry(trade_data)

        except Exception as e:
            logger.error(f"[STAGE 4] Execution Error for {symbol}: {str(e)}", exc_info=True)
            mgmt_actions = {"EXIT_ALL", "TRAIL_SL", "RESTORE_SL", "UPDATE_TGT"}
            from_queue = bool(trade_data.get("_from_mgmt_queue"))
            if action in mgmt_actions and not from_queue and Config.MGMT_QUEUE_ENABLED:
                err_text = str(e)
                if self._is_transient_mgmt_error(err_text) and not self._is_nonretryable_mgmt_error(err_text):
                    qid = self._enqueue_management_action(
                        symbol=symbol,
                        action=action,
                        payload=trade_data,
                        reason=err_text,
                    )
                    if qid:
                        return (
                            f"⏳ {action} queued for retry on {symbol} after broker/transient error. "
                            f"Queue id={qid}. Last error: {err_text}{self._ltp_suffix_for_symbol(symbol)}"
                        )
            return f"❌ ERROR for {symbol}: {str(e)}{self._ltp_suffix_for_symbol(symbol)}"
