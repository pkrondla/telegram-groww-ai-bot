import json
import logging
import uuid
import os
import re
import csv
import copy
import time
import tempfile
import threading
import datetime
import math
import pytz
from .config import Config
from .metrics import metrics
from .db import db
from .logger import logger as app_logger

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
    EXIT_LOCK_TTL_SECONDS = 180
    
    def __init__(
        self,
        dry_run=True,
        *,
        account_id: str = "primary",
        account_name: str | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
    ):
        self.account_id = str(account_id or "primary")
        self.account_name = str(account_name or self.account_id)
        self._api_key_override = str(api_key or "")
        self._api_secret_override = str(api_secret or "")
        self.dry_run = dry_run
        self.active_trades_file = "active_trades.json"
        self.pending_trades_file = "pending_trades.json"
        self.pending_management_file = "pending_management.json"
        self.completed_trades_dir = "completed_trades"
        self._instrument_cache = None  # lazy-loaded dict[(exchange,trading_symbol)] -> row dict
        self._instrument_cache_lock = threading.Lock()
        self._file_lock = threading.Lock()
        self.place_entry_orders = not dry_run
        self._access_token = None

        self.client = None
        self._initialize_groww_client(log_fail=True)

    def _initialize_groww_client(self, *, log_fail: bool = True) -> bool:
        api_key = self._api_key_override or str(Config.GROWW_API_KEY or "")
        api_secret = self._api_secret_override or str(Config.GROWW_API_SECRET or "")
        if not api_key:
            if log_fail:
                logger.warning("GROWW_API_KEY missing for account %s. Groww client not initialized.", self.account_id)
            self.client = None
            return False
        try:
            from growwapi import GrowwAPI

            # Prefer exchanging API key + secret for an access token.
            # growwapi expects Authorization: Bearer <token> for API calls.
            token = None
            if api_secret:
                try:
                    token = GrowwAPI.get_access_token(
                        api_key=api_key, secret=api_secret
                    )
                except Exception as e:
                    if log_fail:
                        logger.error(f"Groww access token fetch failed: {e}")

            # Fall back to using GROWW_API_KEY directly (some users store bearer tokens there)
            self._access_token = token or api_key
            self.client = GrowwAPI(self._access_token)
            if log_fail:
                if self.place_entry_orders:
                    logger.info("Trader initialized in LIVE mode (entry orders enabled).")
                else:
                    logger.info(
                        "Trader initialized in DRY RUN mode (entry BUY/SELL placement disabled; "
                        "quotes / LTP / exit-modify APIs still active if key is set)."
                    )
            return True
        except Exception as e:
            if log_fail:
                logger.error(f"Groww client init failed for account {self.account_id}: {e}")
            self.client = None
            return False

    # Limit concurrent Groww broker API calls across all threads.
    _broker_semaphore = threading.Semaphore(3)

    # Groww HTTP status / exception substrings treated as transient and worth retrying.
    _TRANSIENT_BROKER_ERRORS = (
        "429", "rate limit", "ratelimit",
        "503", "502", "500", "server error",
        "timeout", "timed out", "connection",
        "temporarily unavailable",
    )

    def _groww_call(self, fn, *args, **kwargs):
        """
        Execute a single Groww API call with:
        - a shared semaphore (max 3 concurrent broker calls)
        - up to 3 attempts with exponential backoff (2 s, 4 s) on transient errors
        """
        metrics.increment("broker_calls")
        last_err: Exception | None = None
        for attempt in range(1, 4):
            try:
                with self._broker_semaphore:
                    return fn(*args, **kwargs)
            except Exception as e:
                last_err = e
                err_str = str(e).lower()
                is_transient = any(k in err_str for k in self._TRANSIENT_BROKER_ERRORS)
                if is_transient and attempt < 3:
                    wait = 2 ** attempt  # 2 s, 4 s
                    logger.warning(
                        "[BROKER] Attempt %d transient error (%s) — retrying in %ds.",
                        attempt, str(e)[:80], wait,
                    )
                    metrics.increment("broker_retries")
                    time.sleep(wait)
                else:
                    break
        metrics.increment("broker_failed")
        raise last_err

    @property
    def execute_orders(self) -> bool:
        """Alias for place_entry_orders (entry BUY/SELL placement only)."""
        return self.place_entry_orders

    @execute_orders.setter
    def execute_orders(self, value: bool) -> None:
        self.place_entry_orders = bool(value)
        self.dry_run = not bool(value)

    def check_connection(self, *, log_fail: bool = True):
        """
        Verifies login with Groww.
        """
        if self.client is None:
            self._initialize_groww_client(log_fail=log_fail)
        if self.client is None:
            return False
        try:
            # Simple profile check to verify token (growwapi doesn't expose get_balance)
            self.client.get_user_profile()
            logger.info("Groww connection verified.")
            return True
        except Exception as e:
            if log_fail:
                logger.error(f"Groww Connection Error: {str(e)}")
            # Token could have become valid after manual approval; retry full client init once.
            self._initialize_groww_client(log_fail=log_fail)
            if self.client is None:
                return False
            try:
                self.client.get_user_profile()
                logger.info("Groww connection verified.")
                return True
            except Exception as e2:
                if log_fail:
                    logger.error(f"Groww Connection Error: {str(e2)}")
                return False

    def check_trading_access(self, *, log_fail: bool = True) -> bool:
        """
        Verifies that trading-related API endpoints are accessible from current IP.
        This catches static-IP whitelist issues early at startup.
        """
        if self.client is None:
            if log_fail:
                logger.error("Groww trading access check failed: client not initialized.")
            return False
        try:
            # Read-only trading endpoint, but guarded by the same infra controls.
            self.client.get_positions_for_user(segment="FNO")
            logger.info("Groww trading endpoint access verified (FNO positions API).")
            return True
        except Exception as e:
            msg = str(e)
            if log_fail:
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
    def _parse_iso_timestamp_to_ist(raw) -> datetime.datetime | None:
        """Parse broker ISO timestamps (naive → IST; aware → converted to IST)."""
        if raw is None:
            return None
        s = str(raw).strip()
        if not s:
            return None
        ist = pytz.timezone("Asia/Kolkata")
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.datetime.fromisoformat(s)
            if dt.tzinfo is None:
                return ist.localize(dt)
            return dt.astimezone(ist)
        except (ValueError, TypeError, OSError):
            return None

    def _smart_order_payload_is_live_protection(self, payload: dict) -> tuple[bool, str]:
        """
        True if the OCO smart order can still protect a position: not a terminal
        status and not past expire_at (exchange day / session end from API).
        """
        if not isinstance(payload, dict) or not payload:
            return False, "empty_payload"
        st = str(payload.get("status") or "").upper().strip()
        terminal = {
            "CANCELLED",
            "CANCELED",
            "COMPLETED",
            "COMPLETE",
            "FILLED",
            "REJECTED",
            "FAILED",
            "EXPIRED",
            "REJECT",
        }
        if st in terminal:
            return False, f"status={st}"
        for key in ("expire_at", "expireAt"):
            raw = payload.get(key)
            if not raw:
                continue
            exp = self._parse_iso_timestamp_to_ist(raw)
            if exp is not None and self._now_ist() > exp:
                return False, f"past_{key}"
        return True, ""

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
        return db.get_completed_trades_for_date(d, account_id=self.account_id)

    def _save_completed_trades_book(self, book: dict, date_iso: str | None = None) -> None:
        d = str(date_iso or book.get("date") or self._today_ist_iso())
        # Persist any rows not already in the DB by trade_id.
        existing_rows = db.get_completed_trades_for_date(d, account_id=self.account_id)
        existing_ids = {r.get("trade_id") for r in existing_rows.get("trades", [])}
        for row in book.get("trades", []):
            if row.get("trade_id") not in existing_ids:
                db.insert_completed_trade(d, row, account_id=self.account_id)
            else:
                # Update in case exit price / PnL was backfilled.
                trade_id = row.get("trade_id")
                if trade_id:
                    db.update_completed_trade_row(trade_id, row, account_id=self.account_id)
        eod_sent = bool((book.get("eod") or {}).get("summary_sent_at"))
        if eod_sent:
            db.mark_eod_sent(d, account_id=self.account_id)

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

    @classmethod
    def _infer_exit_fill_from_history_payload(cls, *, symbol: str, entry_side: str, payload) -> dict | None:
        """
        Best-effort extraction of executed exit fills from broker order/trade history payloads.
        Works on heterogeneous schemas by walking every dict node.
        """
        want_side = "SELL" if str(entry_side or "BUY").upper() == "BUY" else "BUY"
        symbol_u = str(symbol or "").upper().strip()
        avg_keys = {
            "average_price",
            "avg_price",
            "avgprice",
            "traded_price",
            "trade_price",
            "executed_price",
            "fill_price",
            "price",
        }
        qty_keys = {
            "filled_quantity",
            "filled_qty",
            "traded_quantity",
            "traded_qty",
            "executed_quantity",
            "executed_qty",
            "quantity",
            "qty",
        }
        side_keys = {"transaction_type", "transactiontype", "side", "order_side", "action", "type"}
        sym_keys = {"trading_symbol", "tradingsymbol", "symbol", "exchange_trading_symbol", "instrument"}
        status_keys = {"status", "order_status", "oms_order_status", "state", "order_state"}

        fills: list[dict] = []
        for node in cls._iter_dict_nodes(payload):
            if not isinstance(node, dict):
                continue
            node_sym = ""
            for k in sym_keys:
                v = node.get(k)
                if v:
                    node_sym = str(v).upper().strip()
                    break
            if not node_sym or node_sym != symbol_u:
                continue

            node_side = ""
            for k in side_keys:
                v = node.get(k)
                if v:
                    node_side = str(v).upper().strip()
                    break
            if node_side and want_side not in node_side:
                continue

            # Prefer explicitly executed/filled rows when status exists.
            good_status = False
            for k in status_keys:
                v = node.get(k)
                if not v:
                    continue
                s = str(v).upper()
                if any(x in s for x in ("FILLED", "COMPLETE", "COMPLETED", "EXECUTED", "DONE", "TRADED")):
                    good_status = True
                    break
            if not good_status and status_keys.intersection(set(node.keys())):
                continue

            avg = cls._walk_for_numeric_key(node, avg_keys) or 0.0
            qty = cls._walk_for_numeric_key(node, qty_keys) or 0.0
            if avg <= 0 or qty <= 0:
                continue
            fills.append(
                {
                    "exit_price": float(avg),
                    "exit_qty": int(abs(qty)),
                    "order_id": cls._extract_order_id_from_node(node),
                }
            )

        if not fills:
            return None
        total_qty = sum(int(f.get("exit_qty") or 0) for f in fills)
        if total_qty <= 0:
            return None
        weighted = sum(float(f.get("exit_price") or 0.0) * int(f.get("exit_qty") or 0) for f in fills) / float(total_qty)
        order_ids = [str(f.get("order_id") or "") for f in fills if str(f.get("order_id") or "")]
        return {
            "exit_price": float(weighted),
            "exit_qty": int(total_qty),
            "source": "broker_history",
            "order_id": (order_ids[0] if order_ids else ""),
        }

    def _infer_exit_fill_from_order_history(self, *, symbol: str, active_rec: dict | None) -> dict | None:
        """
        Try broker order/trade history endpoints for already-closed symbols where
        positions and OCO payloads may no longer carry fill details.
        """
        if self.client is None:
            return None
        rec = self._normalize_active_trade_record(active_rec or {})
        side = str(rec.get("entry_side") or "BUY").upper()
        method_candidates = (
            "get_order_history",
            "get_orders",
            "get_all_orders",
            "get_trade_history",
            "get_trades",
            "get_tradebook",
            "get_orders_for_user",
        )
        for name in method_candidates:
            fn = getattr(self.client, name, None)
            if not callable(fn):
                continue
            for kwargs in ({"segment": "FNO"}, {}):
                try:
                    payload = fn(**kwargs) if kwargs else fn()
                except TypeError:
                    continue
                except Exception as e:
                    logger.debug(f"[RECONCILE] {name} failed for {symbol}: {e}")
                    continue
                inferred = self._infer_exit_fill_from_history_payload(symbol=symbol, entry_side=side, payload=payload)
                if inferred:
                    return inferred
        return None

    def _reconcile_exit_fill_from_positions(self, *, symbol: str, active_rec: dict | None) -> dict | None:
        """
        Best-effort exit fill inference for reconcile-close scenarios where no explicit
        EXIT_ALL order id exists.
        Priority:
          1) OCO leg smart-order payloads saved in active_trades (best source)
          2) Broker FNO positions averages/quantities (fallback)
        """
        if self.client is None:
            return None
        rec = self._normalize_active_trade_record(active_rec or {})
        oco_fill = self._infer_exit_fill_from_oco_legs(symbol=symbol, active_rec=rec)
        if oco_fill:
            return oco_fill
        history_fill = self._infer_exit_fill_from_order_history(symbol=symbol, active_rec=rec)
        if history_fill:
            return history_fill
        side = str(rec.get("entry_side") or "BUY").upper()
        try:
            fno_positions = self.client.get_positions_for_user(segment="FNO")
            positions = fno_positions.get("positions", []) if isinstance(fno_positions, dict) else []
        except Exception as e:
            logger.debug(f"[RECONCILE] Could not fetch positions for exit-fill inference {symbol}: {e}")
            return None

        rows = [p for p in positions if str(p.get("trading_symbol") or "") == str(symbol)]
        if not rows:
            return None

        qty_candidates = (
            {"sell_quantity", "sell_qty", "day_sell_qty", "day_sell_quantity", "sold_qty", "sellfilledqty"}
            if side == "BUY"
            else {"buy_quantity", "buy_qty", "day_buy_qty", "day_buy_quantity", "bought_qty", "buyfilledqty"}
        )
        avg_candidates = (
            {
                "sell_average_price",
                "sell_avg_price",
                "sell_avg",
                "average_sell_price",
                "day_sell_avg_price",
                "day_sell_average_price",
                "sell_price_avg",
            }
            if side == "BUY"
            else {
                "buy_average_price",
                "buy_avg_price",
                "buy_avg",
                "average_buy_price",
                "day_buy_avg_price",
                "day_buy_average_price",
                "buy_price_avg",
            }
        )

        best: dict | None = None
        for row in rows:
            ex_qty = self._walk_for_numeric_key(row, qty_candidates) or 0.0
            ex_avg = self._walk_for_numeric_key(row, avg_candidates) or 0.0
            if ex_qty > 0 and ex_avg > 0:
                cand = {
                    "exit_price": float(ex_avg),
                    "exit_qty": int(abs(ex_qty)),
                    "source": "broker_positions",
                    "order_id": "",
                }
                if best is None or int(cand["exit_qty"]) > int(best.get("exit_qty") or 0):
                    best = cand
        return best

    def _record_completed_trade(
        self,
        *,
        symbol: str,
        active_rec: dict | None,
        close_reason: str,
        exit_price: float,
        exit_qty: int,
        close_order_id: str = "",
        exit_pending: bool = False,
        exit_fill_source: str = "",
    ):
        rec = self._normalize_active_trade_record(active_rec or {})
        side = str(rec.get("entry_side") or "BUY").upper()
        entry_price = self._num_or(rec.get("entry_avg_price"), 0.0)
        sl_true = self._num_or(rec.get("sl_true"), self._num_or(rec.get("stop_loss"), 0.0))
        qty = int(self._num_or(rec.get("entry_qty"), 0))
        if qty <= 0:
            qty = int(abs(exit_qty or 0))
        lot_size = int(self._num_or(rec.get("lot_size"), 0))
        if lot_size <= 0:
            lot_size = max(1, qty)
        lots = max(1.0, float(qty) / float(lot_size))
        group_id = str(rec.get("spread_group_id") or "")
        is_spread_leg = bool(group_id)
        signal_targets = rec.get("signal_targets") if isinstance(rec.get("signal_targets"), list) else []
        if not signal_targets:
            t_single = self._num_or(rec.get("target"), 0.0)
            if t_single > 0:
                signal_targets = [float(t_single)]
        norm_targets: list[float] = []
        for t in signal_targets[:3]:
            try:
                tv = float(t)
            except Exception:
                continue
            if tv > 0:
                norm_targets.append(tv)
        signal_targets = norm_targets

        legs_raw = self._sorted_oco_legs(rec)
        oco_legs_snapshot: list[dict] = []
        for leg in legs_raw:
            oco_legs_snapshot.append(
                {
                    "target_index": int(self._num_or(leg.get("target_index"), 0)),
                    "qty": int(self._num_or(leg.get("qty"), 0)),
                    "target_signal": float(self._num_or(leg.get("target_signal"), 0.0)),
                    "target_broker": float(self._num_or(leg.get("target_broker"), 0.0)),
                    "status": str(leg.get("status") or "UNKNOWN").upper(),
                    "smart_order_id": str(leg.get("smart_order_id") or ""),
                    "target_order_id": str(leg.get("target_order_id") or ""),
                    "stop_order_id": str(leg.get("stop_order_id") or ""),
                }
            )

        per_target_rr: list[dict] = []
        risk_pts = max(float(entry_price) - float(sl_true), 0.0) if side == "BUY" else max(float(sl_true) - float(entry_price), 0.0)
        for i, tgt in enumerate(signal_targets):
            tgt_idx = i + 1
            leg_qty = 0
            for leg in oco_legs_snapshot:
                if int(self._num_or(leg.get("target_index"), 0)) == tgt_idx:
                    leg_qty += int(self._num_or(leg.get("qty"), 0))
            if leg_qty <= 0:
                leg_qty = 0
            reward_pts = 0.0
            if side == "BUY":
                reward_pts = max(float(tgt) - float(entry_price), 0.0)
            else:
                reward_pts = max(float(entry_price) - float(tgt), 0.0)
            rr = (reward_pts / risk_pts) if risk_pts > 0 else 0.0
            per_target_rr.append(
                {
                    "target_index": tgt_idx,
                    "target_signal": float(tgt),
                    "qty": int(leg_qty),
                    "risk_points": float(risk_pts),
                    "reward_points": float(reward_pts),
                    "risk_inr": float(risk_pts * leg_qty),
                    "reward_inr": float(reward_pts * leg_qty),
                    "rr": float(rr),
                }
            )

        points_per_lot, pnl_inr = self._trade_pnl(side, entry_price, float(exit_price or 0), qty)
        pending = bool(exit_pending)
        if pending:
            points_per_lot = 0.0
            pnl_inr = 0.0
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
            "exit_pending": pending,
            "pnl_pending": pending,
            "exit_fill_source": str(exit_fill_source or ("pending" if pending else "")),
            "reconcile_attempts": 0,
            "last_reconcile_try_ist": "",
            "smart_order_id": str(rec.get("smart_order_id") or ""),
            "exit_qty": int(abs(exit_qty or 0)),
            "signal_targets": [float(t) for t in signal_targets],
            "oco_legs": oco_legs_snapshot,
            "sl_progress_stage": str(rec.get("sl_progress_stage") or "NONE"),
            "initial_sl": float(self._num_or(rec.get("stop_loss"), 0.0)),
            "sl_true_at_close": float(sl_true),
            "target_at_close": float(self._num_or(rec.get("target"), 0.0)),
            "per_target_rr": per_target_rr,
            "volatility_sl_state": {
                "sl_mode": str(rec.get("sl_mode") or "NORMAL"),
                "sl_widened": float(self._num_or(rec.get("sl_widened"), 0.0)),
                "sl_widen_pct": float(self._num_or(rec.get("sl_widen_pct"), 0.0)),
                "vol_sl_needs_restore": bool(self._bool_or(rec, "vol_sl_needs_restore", False)),
            },
        }

        book = self._load_completed_trades_book()
        trades = book.get("trades") or []
        trades.append(entry)
        book["trades"] = trades
        self._save_completed_trades_book(book)

    @staticmethod
    def _completed_row_active_record(row: dict) -> dict:
        """Rebuild a minimal active-like record so reconcile fill inference can be reused."""
        return {
            "entry_side": str(row.get("side") or "BUY").upper(),
            "entry_avg_price": float(row.get("entry_avg_price") or 0.0),
            "entry_qty": int(row.get("quantity") or row.get("exit_qty") or 0),
            "lot_size": int(row.get("lot_size") or 0),
            "smart_order_id": str(row.get("smart_order_id") or ""),
            "oco_legs": list(row.get("oco_legs") or []),
            "signal_targets": list(row.get("signal_targets") or []),
            "target": float(row.get("target_at_close") or 0.0),
        }

    def _backfill_pending_completed_trades(self, *, date_iso: str | None = None) -> int:
        """
        Auto-backfill pending completed trades with real broker exit fills.
        Called from reconcile loop so no admin command is required.
        """
        if self.client is None:
            return 0
        d = str(date_iso or self._today_ist_iso())
        book = self._load_completed_trades_book(d)
        rows = list(book.get("trades") or [])
        if not rows:
            return 0

        updated_count = 0
        changed = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            pending = bool(row.get("exit_pending") or row.get("pnl_pending"))
            if not pending:
                # Backward compatibility for older journal rows that used entry-price fallback.
                inferred_flat = str(row.get("close_reason") or "").upper().startswith("RECONCILE_")
                zero_or_same_exit = (
                    float(self._num_or(row.get("exit_avg_price"), 0.0)) <= 0
                    or abs(
                        float(self._num_or(row.get("exit_avg_price"), 0.0))
                        - float(self._num_or(row.get("entry_avg_price"), 0.0))
                    )
                    < 1e-9
                )
                no_exit_id = not str(row.get("exit_order_id") or "").strip()
                pending = bool(inferred_flat and zero_or_same_exit and no_exit_id)
            if not pending:
                continue
            symbol = str(row.get("symbol") or "")
            if not symbol:
                continue

            rec = self._completed_row_active_record(row)
            inferred = self._reconcile_exit_fill_from_positions(symbol=symbol, active_rec=rec) or {}
            exit_price = float(self._num_or(inferred.get("exit_price"), 0.0))
            exit_qty = int(abs(self._num_or(inferred.get("exit_qty"), 0)))

            row["reconcile_attempts"] = int(self._num_or(row.get("reconcile_attempts"), 0)) + 1
            row["last_reconcile_try_ist"] = self._now_ist_iso()
            changed = True

            if exit_price <= 0 or exit_qty <= 0:
                logger.info(
                    f"[RECONCILE] Pending exit fill still unavailable for {symbol} "
                    f"(attempt={int(row.get('reconcile_attempts') or 0)})."
                )
                continue

            side = str(row.get("side") or "BUY").upper()
            entry_price = float(self._num_or(row.get("entry_avg_price"), 0.0))
            qty = int(self._num_or(row.get("quantity"), 0)) or exit_qty
            points_per_lot, pnl_inr = self._trade_pnl(side, entry_price, exit_price, qty)

            row["exit_avg_price"] = float(exit_price)
            row["exit_qty"] = int(exit_qty)
            row["exit_order_id"] = str(inferred.get("order_id") or row.get("exit_order_id") or "")
            row["pnl_points_per_lot"] = float(points_per_lot)
            row["pnl_inr"] = float(pnl_inr)
            row["exit_pending"] = False
            row["pnl_pending"] = False
            row["exit_fill_source"] = str(inferred.get("source") or row.get("exit_fill_source") or "backfilled")
            updated_count += 1
            logger.info(
                f"[RECONCILE] Backfilled exit for {symbol}: price={exit_price}, qty={exit_qty}, "
                f"source={row.get('exit_fill_source')}."
            )

        if changed:
            book["trades"] = rows
            self._save_completed_trades_book(book, date_iso=d)
        return updated_count

    def _build_eod_summary_text(self, *, date_iso: str | None = None) -> str:
        d = str(date_iso or self._today_ist_iso())
        book = self._load_completed_trades_book(d)
        rows = list(book.get("trades") or [])

        settled_rows = [r for r in rows if not bool(r.get("pnl_pending"))]
        pending_rows = [r for r in rows if bool(r.get("pnl_pending"))]
        singles = [r for r in settled_rows if not r.get("is_spread_leg")]
        spread_legs = [r for r in settled_rows if r.get("is_spread_leg")]
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

        total_inr = sum(float(r.get("pnl_inr") or 0.0) for r in all_trade_summaries)
        total_pts = sum(float(r.get("pnl_points_per_lot") or 0.0) for r in all_trade_summaries)

        lines = []
        for r in all_trade_summaries:
            lines.append(
                f"• {r['label']}: INR {float(r['pnl_inr']):.2f} | {float(r['pnl_points_per_lot']):.2f} pts/lot"
            )
        per_trade_section = "\n".join(lines) if lines else "• No realized completed trades."
        pending_section = ""
        if pending_rows:
            pending_lines = [f"• {str(r.get('symbol') or '?')}: exit fill pending" for r in pending_rows]
            pending_section = "\n\nPending exit fill\n" + "\n".join(pending_lines)

        metrics_snap = metrics.snapshot()
        metrics_lines = []
        for key in ("signals_received", "trades_entered", "llm_calls", "llm_cache_hits",
                    "llm_retries", "llm_failed", "broker_calls", "broker_retries", "broker_failed"):
            v = metrics_snap.get(key, 0)
            if v:
                metrics_lines.append(f"  {key}: {v}")
        metrics_section = ("\n\nBot metrics (session)\n" + "\n".join(metrics_lines)) if metrics_lines else ""

        return (
            f"📊 EOD Trading Summary ({d} IST)\n\n"
            f"Per-trade P&L\n{per_trade_section}{pending_section}\n\n"
            f"Final day P/L: INR {total_inr:.2f} | {total_pts:.2f} pts/lot"
            f"{metrics_section}"
        )

    def run_eod_summary_tick(self) -> str | None:
        if not Config.EOD_SUMMARY_ENABLED:
            return None
        now = self._now_ist()
        if Config.EOD_SUMMARY_SKIP_WEEKENDS and now.weekday() >= 5:
            return None
        t = self._parse_hhmm_ist(Config.MARKET_CLOSE_TIME_IST)
        if not t:
            return "⚠️ EOD summary skipped: invalid MARKET_CLOSE_TIME_IST (expected HH:MM IST)."
        target_dt = now.replace(hour=t[0], minute=t[1], second=0, microsecond=0)
        # Robust against coarse poll intervals: trigger once/day after configured time.
        if now < target_dt:
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

    def _rearm_oco_for_open_position(
        self,
        symbol: str,
        rec: dict,
        position: dict,
        *,
        force: bool = False,
        payload_cache: dict[str, dict] | None = None,
    ) -> tuple[bool, str | None]:
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
        if not force and str(rec.get("protection_armed_date") or "") == today:
            return False, None

        qty = abs(self._position_quantity(position))
        if qty <= 0:
            return False, None
        rec_norm = self._normalize_active_trade_record(rec)
        rec_norm = self._sync_oco_snapshot_from_broker(symbol, rec_norm, payload_cache=payload_cache)
        rec_norm, _ = self._recompute_leg_progress(rec=rec_norm, open_qty=qty)
        complete_before, reason_before = self._has_complete_oco_protection(
            symbol, rec_norm, qty, payload_cache=payload_cache
        )
        if complete_before:
            # Protection already has both SL and target legs on broker. No re-arm needed.
            trades = self._load_active_trades()
            nrec = self._normalize_active_trade_record(trades.get(symbol))
            nrec["protection_armed_date"] = today
            trades[symbol] = nrec
            self._save_all_trades(trades)
            logger.info(f"[REARM] Skip {symbol}: broker OCO already complete (SL + target present).")
            return False, None

        raw_true_sl = float(self._num_or(rec_norm.get("sl_true"), rec_norm.get("stop_loss")))
        if raw_true_sl <= 0:
            logger.warning(f"[REARM] Skip {symbol}: invalid stored SL (sl_true={raw_true_sl}).")
            return False, f"⚠️ OCO re-arm skipped for {symbol}: invalid stored SL (sl_true={raw_true_sl})."
        broker_sl = float(raw_true_sl)
        if bool(rec_norm.get("vol_sl_needs_restore")):
            widened = self._widened_sl_from_true(
                symbol, raw_true_sl, float(rec_norm.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT)
            )
            if widened is not None and widened > 0:
                broker_sl = float(widened)

        active_legs = [l for l in self._sorted_oco_legs(rec_norm) if str(l.get("status") or "").upper() == "ACTIVE"]
        if not active_legs:
            raw_tgt = float(self._num_or(rec_norm.get("target"), 0.0))
            if raw_tgt <= 0:
                return False, None
            active_legs = [
                {
                    "target_index": 1,
                    "target_signal": raw_tgt,
                    "qty": int(qty),
                    "status": "ACTIVE",
                }
            ]

        base_sum = sum(max(0, int(self._num_or(l.get("qty"), 0))) for l in active_legs)
        if base_sum <= 0:
            base_sum = len(active_legs)
            for l in active_legs:
                l["qty"] = 1
        rebuilt_plan: list[dict] = []
        used = 0
        for i, leg in enumerate(active_legs):
            base_q = max(1, int(self._num_or(leg.get("qty"), 1)))
            if i == len(active_legs) - 1:
                alloc = max(0, int(qty - used))
            else:
                alloc = int(math.floor((qty * base_q) / float(base_sum)))
                if qty >= len(active_legs):
                    alloc = max(1, alloc)
            used += alloc
            if alloc <= 0:
                continue
            rebuilt_plan.append(
                {
                    "qty": int(alloc),
                    "target_index": int(self._num_or(leg.get("target_index"), i + 1)),
                    "target_signal": float(self._num_or(leg.get("target_signal"), rec_norm.get("target"))),
                }
            )
        if not rebuilt_plan:
            return False, None

        existing_ids = self._oco_ids_from_record(rec_norm)
        if existing_ids:
            # Prefer in-place repair to reduce broker cancel/create churn in order history.
            # If an OCO leg has SL but missing target, attempt target modify first.
            leg_by_id = {
                str(leg.get("smart_order_id") or ""): leg
                for leg in self._sorted_oco_legs(rec_norm)
                if str(leg.get("smart_order_id") or "")
            }
            repaired = 0
            for oid in existing_ids:
                sid = str(oid or "")
                if not sid or sid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                    continue
                payload = self._get_oco_order_payload_cached(sid, payload_cache)
                has_t = self._oco_payload_has_target(payload)
                has_s = self._oco_payload_has_stop(payload)
                # If both legs already exist, nothing to repair.
                if has_t and has_s:
                    continue
                leg = leg_by_id.get(sid) or {}
                try:
                    if not has_t:
                        target_broker = float(self._num_or(leg.get("target_broker"), 0.0))
                        if target_broker > 0:
                            eff_tgt = self._round_price_to_tick(target_broker, mode="down")
                        else:
                            target_signal = float(
                                self._num_or(
                                    leg.get("target_signal"),
                                    rec_norm.get("target"),
                                )
                            )
                            eff_tgt = self._take_profit_order_price(target_signal)
                        if eff_tgt > 0:
                            self._modify_oco_target(sid, eff_tgt, symbol=symbol, payload_cache=payload_cache)
                    if not has_s and broker_sl > 0:
                        self._modify_oco_sl(symbol, sid, broker_sl, payload_cache=payload_cache)
                    repaired += 1
                    if payload_cache is not None:
                        payload_cache.pop(sid, None)
                except Exception as me:
                    logger.warning(f"[REARM] In-place leg repair failed for {symbol} id={sid}: {me}")

            if repaired > 0:
                rec_repaired = self._sync_oco_snapshot_from_broker(symbol, rec_norm, payload_cache=payload_cache)
                complete_after_repair, reason_after_repair = self._has_complete_oco_protection(
                    symbol, rec_repaired, qty, payload_cache=payload_cache
                )
                if complete_after_repair:
                    trades = self._load_active_trades()
                    nrec = self._normalize_active_trade_record(trades.get(symbol))
                    nrec["protection_armed_date"] = today
                    trades[symbol] = nrec
                    self._save_all_trades(trades)
                    logger.info(
                        f"[REARM] {symbol}: repaired incomplete OCO in-place via modify "
                        f"(legs_repaired={repaired}); skipped cancel/re-create."
                    )
                    return True, f"✅ OCO repaired in-place for {symbol} (legs={repaired}); no cancel/re-create needed."
                logger.warning(
                    f"[REARM] {symbol}: in-place OCO repair did not fully restore integrity "
                    f"(reason={reason_after_repair}); falling back to cancel/re-create."
                )

            canceled, cancel_errors = self._cancel_oco_ids_for_symbol(symbol, rec_norm)
            if payload_cache is not None:
                for oid in existing_ids:
                    payload_cache.pop(str(oid or ""), None)
            logger.warning(
                f"[REARM] {symbol}: existing OCO incomplete ({reason_before}); "
                f"cancelled {canceled}/{len(existing_ids)} old OCO id(s) before re-arm."
            )
            if cancel_errors:
                logger.warning(
                    f"[REARM] {symbol}: OCO cancel warnings before re-arm: {' | '.join(cancel_errors[:5])}"
                )

        exchange = str(position.get("exchange") or "NSE")
        segment = str(position.get("segment") or self.SMART_SEGMENT)
        product_type = str(
            position.get("product")
            or position.get("product_type")
            or position.get("productType")
            or "NRML"
        ).upper()

        created_legs: list[dict] = []
        rearm_warnings: list[str] = []
        for leg in rebuilt_plan:
            raw_tgt = float(leg.get("target_signal") or 0)
            eff_tgt = self._take_profit_order_price(raw_tgt)
            if eff_tgt <= 0:
                continue
            def _create_rearm_leg(ref: str):
                resp = self.client.create_smart_order(
                    smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                    reference_id=ref,
                    segment=segment,
                    trading_symbol=symbol,
                    quantity=int(leg["qty"]),
                    net_position_quantity=int(leg["qty"]),
                    transaction_type="SELL",
                    product_type=product_type,
                    exchange=exchange,
                    duration="DAY",
                    target={
                        "trigger_price": eff_tgt,
                        "price": None,
                        "order_type": "MARKET",
                    },
                    stop_loss={
                        "trigger_price": float(broker_sl),
                        "price": float(broker_sl),
                        "order_type": "SL",
                    },
                )
                logger.info(
                    f"[REARM] OCO create response {symbol}: ref={ref} target={eff_tgt} sl={broker_sl} "
                    f"resp={(str(resp) or '')[:800]}"
                )
                sid = self._extract_smart_order_id(resp, fallback=ref)
                vp = self._get_oco_order_payload(str(sid or ""))
                logger.info(
                    f"[REARM] OCO verify payload {symbol}: id={sid} payload={(str(vp) or '')[:800]}"
                )
                t_id, s_id = self._extract_oco_leg_child_order_ids(vp)
                has_t = self._oco_payload_has_target(vp)
                has_s = self._oco_payload_has_stop(vp)
                return sid, t_id, s_id, has_t, has_s

            rearm_ref = str(uuid.uuid4())[:15]
            logger.info(
                f"[REARM] Creating OCO for {symbol}: qty={leg['qty']}, sl_true={raw_true_sl}, "
                f"broker_sl={broker_sl}, target(signal={raw_tgt}, broker={eff_tgt})"
            )
            try:
                new_id, target_order_id, stop_order_id, has_target, has_stop = _create_rearm_leg(rearm_ref)
            except Exception as e:
                msg = str(e)
                if "cannot exceed position quantity" in msg.lower():
                    logger.error(
                        f"[REARM] Re-arm create rejected for {symbol} after cleanup "
                        f"(reason='cannot exceed position quantity', raw_error={msg})."
                    )
                    return (
                        False,
                        f"⚠️ OCO re-arm failed for {symbol}: broker still reports quantity locked after cancel "
                        f"(cannot exceed position quantity).",
                    )
                raise

            if not (has_target and has_stop):
                logger.warning(
                    f"[REARM] OCO verify incomplete for {symbol} id={new_id} "
                    f"(target={has_target}, stop={has_stop}). Retrying once."
                )
                try:
                    self.client.cancel_smart_order(
                        segment=self.client.SEGMENT_FNO,
                        smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                        smart_order_id=str(new_id or ""),
                    )
                    logger.info(f"[REARM] Partial OCO canceled for retry: {symbol} id={new_id}")
                except Exception as ce:
                    logger.warning(f"[REARM] Partial OCO cancel failed for {symbol} id={new_id}: {ce}")

                retry_ref = str(uuid.uuid4())[:15]
                try:
                    new_id, target_order_id, stop_order_id, has_target, has_stop = _create_rearm_leg(retry_ref)
                except Exception as re:
                    logger.warning(f"[REARM] OCO retry create failed for {symbol}: {re}")
                    has_target, has_stop = False, False
                    target_order_id, stop_order_id = "", ""

            if not (has_target and has_stop):
                warn = (
                    f"OCO re-arm partial for {symbol} T{int(leg.get('target_index') or 1)}: "
                    f"target={has_target}, stop={has_stop}."
                )
                rearm_warnings.append(warn)
                logger.error(f"[REARM] {warn}")

            created_legs.append(
                {
                    "smart_order_id": str(new_id or ""),
                    "qty": int(leg["qty"]),
                    "target_signal": float(raw_tgt),
                    "target_broker": float(eff_tgt),
                    "target_index": int(leg.get("target_index") or 1),
                    "status": (
                        "ACTIVE"
                        if (has_target and has_stop)
                        else ("TARGET_ONLY" if has_target else "SL_ONLY" if has_stop else "BROKEN")
                    ),
                    "target_order_id": str(target_order_id or ""),
                    "stop_order_id": str(stop_order_id or ""),
                }
            )
        if not created_legs:
            return False, f"⚠️ OCO re-arm skipped for {symbol}: no valid active target legs to re-arm."
        new_id = str(created_legs[0].get("smart_order_id") or "")
        self._save_active_trade(
            symbol,
            new_id,
            stop_loss=float(broker_sl),
            target=float(self._num_or(rec_norm.get("target"), 0.0)),
            sl_progress_stage=str(rec_norm.get("sl_progress_stage") or "NONE"),
            signal_targets=rec_norm.get("signal_targets"),
            oco_legs=created_legs,
        )
        self._update_active_trade_levels(
            symbol,
            stop_loss=float(broker_sl),
            sl_true=float(raw_true_sl),
            sl_widened=(broker_sl if bool(rec_norm.get("vol_sl_needs_restore")) else 0.0),
            sl_mode=("WIDENED" if bool(rec_norm.get("vol_sl_needs_restore")) else "NORMAL"),
            sl_widen_pct=float(rec_norm.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT),
        )
        trades = self._load_active_trades()
        nrec = self._normalize_active_trade_record(trades.get(symbol))
        all_legs_complete = all(
            str(leg.get("status") or "").upper() == "ACTIVE"
            for leg in created_legs
        )
        nrec["protection_armed_date"] = today if all_legs_complete else ""
        trades[symbol] = nrec
        self._save_all_trades(trades)
        logger.info(
            f"[REARM] OCO re-armed for {symbol} with smart_order_id={new_id} "
            f"(all_legs_complete={all_legs_complete})."
        )
        leg_lines = []
        for leg in created_legs:
            leg_lines.append(
                f"T{int(leg.get('target_index') or 1)} qty={int(leg.get('qty') or 0)}: "
                f"signal {float(leg.get('target_signal') or 0)} → broker {float(leg.get('target_broker') or 0)}"
            )
        msg = (
            f"✅ OCO re-armed for {symbol}\n"
            f"Qty: {qty}\n"
            f"SL true: {raw_true_sl} | broker SL: {broker_sl}\n"
            + "\n".join(leg_lines)
        )
        if rearm_warnings or not all_legs_complete:
            msg += "\nWarnings:\n" + "\n".join(
                rearm_warnings or [f"OCO re-arm incomplete for {symbol}: not all legs are ACTIVE."]
            )
            return False, msg
        return True, msg

    def _retry_management_after_stale_oco(
        self, symbol: str, trade_data: dict, rec: dict | None, *, reason: str
    ) -> str | None:
        """
        Self-heal path for management actions:
        stale OCO -> force re-arm -> retry action once.
        """
        if bool(trade_data.get("_stale_rearm_retry")):
            return None
        if self.client is None:
            return None
        pos, qty = self._get_open_position_for_symbol(symbol)
        if qty == 0:
            return (
                f"⚠️ OCO recovery skipped for {symbol}: no open broker position to protect. "
                f"Last error: {reason}{self._ltp_suffix_for_symbol(symbol)}"
            )
        rec_rearm = dict(rec or self._get_active_trade_record(symbol) or {})
        rec_rearm["protection_armed_date"] = ""
        ok, alert = self._rearm_oco_for_open_position(symbol, rec_rearm, pos or {}, force=True)
        if not ok:
            if alert:
                return f"⚠️ OCO recovery failed for {symbol}: {alert}{self._ltp_suffix_for_symbol(symbol)}"
            return (
                f"⚠️ OCO recovery failed for {symbol}: could not re-arm protection after stale OCO error. "
                f"Last error: {reason}{self._ltp_suffix_for_symbol(symbol)}"
            )
        retry_payload = dict(trade_data)
        retry_payload["_stale_rearm_retry"] = True
        # Suppress enqueue recursion; this is already an explicit local retry.
        retry_payload["_from_mgmt_queue"] = True
        retry_result = self.execute_trade(retry_payload)
        return (
            f"🔁 OCO recovered for {symbol} after stale protection error. "
            f"Re-armed and retried action.\n{retry_result}"
        )

    def _has_modifiable_oco(self, symbol: str, rec: dict) -> bool:
        """
        Best-effort broker OCO liveness check for maintenance runs.
        `_modify_oco_sl` fails fast when get_smart_order shows expired/cancelled OCO;
        remaining cases use a same-level SL modify probe; stale broker errors mean
        protection is missing or not modifiable.
        """
        if self.client is None:
            return False
        rec_norm = self._normalize_active_trade_record(rec or {})
        expected_legs = self._expected_oco_leg_count_for_open_qty(
            rec_norm, int(self._num_or(rec_norm.get("entry_qty"), 0))
        )
        probe_ids: list[str] = []
        if expected_legs > 1:
            probe_ids = self._oco_ids_from_record(rec_norm)
        probe_id = str(rec_norm.get("smart_order_id") or "")
        if probe_id and probe_id not in probe_ids:
            probe_ids.insert(0, probe_id)
        if not probe_ids:
            order_ids = self._oco_ids_from_record(rec)
            if order_ids:
                probe_ids = [str(order_ids[0] or "")]
        if not probe_ids:
            return False
        raw_sl = float(self._num_or(rec.get("stop_loss"), 0.0))
        raw_true = float(self._num_or(rec.get("sl_true"), 0.0))
        probe_sl = raw_sl if raw_sl > 0 else raw_true
        if probe_sl <= 0:
            return False
        probe_cache: dict[str, dict] = {}
        for probe_id in probe_ids:
            if not probe_id or probe_id == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                continue
            try:
                # Probe with current level so there is no effective price change.
                self._modify_oco_sl(symbol, probe_id, probe_sl, payload_cache=probe_cache)
            except Exception as e:
                if self._is_stale_oco_error(str(e)):
                    return False
                # Non-stale broker errors should not trigger forced re-arm.
                logger.warning(f"[REARM] OCO probe inconclusive for {symbol} id={probe_id}: {e}")
        return True

    def _has_target_legs_on_broker(
        self, symbol: str, rec: dict, *, payload_cache: dict[str, dict] | None = None
    ) -> bool:
        """
        Verify broker-side OCO payloads still contain target legs.
        This catches SL-only protection states where SL modify succeeds but target is missing.
        """
        if self.client is None:
            return False
        rec_norm = self._normalize_active_trade_record(rec or {})
        expected_legs = self._expected_oco_leg_count_for_open_qty(
            rec_norm, int(self._num_or(rec_norm.get("entry_qty"), 0))
        )
        if expected_legs > 1:
            order_ids = self._oco_ids_from_record(rec_norm)
        else:
            sid = str(rec_norm.get("smart_order_id") or "")
            if not sid:
                order_ids = self._oco_ids_from_record(rec_norm)
                sid = str(order_ids[0] or "") if order_ids else ""
            else:
                order_ids = [sid]
        if not order_ids:
            return False
        for oid in order_ids:
            sid = str(oid or "")
            if not sid or sid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                continue
            payload = self._get_oco_order_payload_cached(sid, payload_cache)
            has_t = self._oco_payload_has_target(payload)
            has_s = self._oco_payload_has_stop(payload)
            live_ok, live_rs = self._smart_order_payload_is_live_protection(payload)
            if not has_t:
                logger.warning(f"[REARM] Missing target leg on broker for {symbol} id={sid}.")
                return False
            if not has_s:
                logger.warning(f"[REARM] Missing stop leg on broker for {symbol} id={sid}.")
                return False
            if not live_ok:
                logger.warning(
                    f"[REARM] OCO not live for {symbol} id={sid} ({live_rs}); needs re-arm."
                )
                return False
        return True

    def _expected_oco_leg_count_for_open_qty(self, rec: dict, open_qty: int) -> int:
        """
        Compute expected number of active OCO legs from current target/split policy.
        """
        qty = int(abs(open_qty or 0))
        if qty <= 0:
            return 0
        rec_norm = self._normalize_active_trade_record(rec or {})
        raw_targets = rec_norm.get("signal_targets")
        signal_targets: list[float] = []
        if isinstance(raw_targets, list):
            for t in raw_targets:
                try:
                    tv = float(t)
                except Exception:
                    continue
                if tv > 0:
                    signal_targets.append(tv)
        if not signal_targets:
            fallback = float(self._num_or(rec_norm.get("target"), 0.0))
            if fallback > 0:
                signal_targets = [fallback]
        if not signal_targets:
            return 0
        lot_size = int(self._num_or(rec_norm.get("lot_size"), 0))
        plan = self._build_oco_legs_plan(
            total_qty=qty,
            lot_size=lot_size,
            signal_targets=signal_targets,
        )
        return len([p for p in plan if int(self._num_or(p.get("qty"), 0)) > 0])

    def _sync_oco_snapshot_from_broker(
        self, symbol: str, rec: dict, *, payload_cache: dict[str, dict] | None = None
    ) -> dict:
        """
        Refresh local oco_legs snapshot from broker smart-order payloads.
        This keeps active_trades.json aligned with broker truth before integrity checks.
        """
        rec_norm = self._normalize_active_trade_record(rec or {})
        if self.client is None:
            return rec_norm
        order_ids = self._oco_ids_from_record(rec_norm)
        if not order_ids:
            return rec_norm

        prev_legs = self._sorted_oco_legs(rec_norm)
        prev_by_id = {
            str(leg.get("smart_order_id") or ""): leg
            for leg in prev_legs
            if str(leg.get("smart_order_id") or "")
        }
        synced_legs: list[dict] = []
        for idx, oid in enumerate(order_ids):
            sid = str(oid or "")
            if not sid or sid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                continue
            payload = self._get_oco_order_payload_cached(sid, payload_cache)
            tgt_id, sl_id = self._extract_oco_leg_child_order_ids(payload)
            # Some broker payloads expose leg IDs but not top-level target/stop keys.
            has_t = bool(self._oco_payload_has_target(payload) or tgt_id)
            has_s = bool(self._oco_payload_has_stop(payload) or sl_id)
            live_ok, _ = self._smart_order_payload_is_live_protection(payload)
            prev = prev_by_id.get(sid) or {}
            if has_t and has_s and not live_ok:
                leg_status = "STALE"
            else:
                leg_status = (
                    "ACTIVE"
                    if (has_t and has_s and live_ok)
                    else ("TARGET_ONLY" if has_t else "SL_ONLY" if has_s else "BROKEN")
                )
            synced_legs.append(
                {
                    "smart_order_id": sid,
                    "qty": int(self._num_or(prev.get("qty"), 0)),
                    "target_signal": float(self._num_or(prev.get("target_signal"), 0.0)),
                    "target_broker": float(self._num_or(prev.get("target_broker"), 0.0)),
                    "target_index": int(self._num_or(prev.get("target_index"), idx + 1)),
                    "status": leg_status,
                    "target_order_id": str(tgt_id or prev.get("target_order_id") or ""),
                    "stop_order_id": str(sl_id or prev.get("stop_order_id") or ""),
                }
            )

        if not synced_legs:
            return rec_norm

        # Preserve qty/index from existing local plan when available.
        total_synced_qty = sum(int(self._num_or(x.get("qty"), 0)) for x in synced_legs)
        if total_synced_qty <= 0:
            base_qty = int(self._num_or(rec_norm.get("entry_qty"), 0))
            if base_qty <= 0:
                base_qty = len(synced_legs)
            if base_qty > 0 and synced_legs:
                per = max(1, base_qty // len(synced_legs))
                used = 0
                for i, leg in enumerate(synced_legs):
                    if i == len(synced_legs) - 1:
                        leg["qty"] = max(1, base_qty - used)
                    else:
                        leg["qty"] = per
                        used += per

        changed = self._sorted_oco_legs({"oco_legs": prev_legs}) != self._sorted_oco_legs({"oco_legs": synced_legs})
        if not changed:
            return rec_norm

        trades = self._load_active_trades()
        if symbol not in trades:
            return rec_norm
        live = self._normalize_active_trade_record(trades.get(symbol))
        live["oco_legs"] = synced_legs
        if synced_legs:
            live["smart_order_id"] = str(synced_legs[0].get("smart_order_id") or live.get("smart_order_id") or "")
        trades[symbol] = live
        self._save_all_trades(trades)
        logger.info(f"[REARM] Synced broker OCO snapshot for {symbol}: legs={len(synced_legs)}.")
        return live

    def _has_complete_oco_protection(
        self, symbol: str, rec: dict, open_qty: int, *, payload_cache: dict[str, dict] | None = None
    ) -> tuple[bool, str]:
        """
        Protection integrity check for active positions:
        - Single-target: evaluate current smart_order_id.
        - Multi-target: evaluate each expected OCO leg id from local plan.
        Target/stop trigger dicts must be present, and the smart order must be
        "live" (not terminal status, not past API expire_at session end).
        """
        rec_norm = self._normalize_active_trade_record(rec or {})
        expected_legs = self._expected_oco_leg_count_for_open_qty(rec_norm, open_qty)
        if expected_legs <= 0:
            return False, "no expected target legs from current target configuration"

        if expected_legs == 1:
            sid = str(rec_norm.get("smart_order_id") or "")
            if not sid:
                order_ids = self._oco_ids_from_record(rec_norm)
                sid = str(order_ids[0] or "") if order_ids else ""
            if not sid:
                return False, "missing smart_order_id"
            if sid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                return False, "dry-run placeholder order id"
            payload = self._get_oco_order_payload_cached(sid, payload_cache)
            has_t = self._oco_payload_has_target(payload)
            has_s = self._oco_payload_has_stop(payload)
            live_ok, live_rs = self._smart_order_payload_is_live_protection(payload)
            if has_t and has_s and live_ok:
                return True, ""
            if has_t and has_s and not live_ok:
                logger.warning(
                    f"[REARM] OCO not live for {symbol} id={sid} ({live_rs}); "
                    f"target={has_t}, stop={has_s}."
                )
                return False, f"oco not live ({live_rs})"
            logger.warning(
                f"[REARM] Incomplete OCO protection for {symbol} id={sid}: target={has_t}, stop={has_s}."
            )
            return False, f"incomplete trigger legs (target={has_t}, stop={has_s})"

        order_ids = self._oco_ids_from_record(rec_norm)
        if len(order_ids) < expected_legs:
            return False, f"insufficient OCO legs (expected={expected_legs}, present={len(order_ids)})"
        complete = 0
        for oid in order_ids:
            sid = str(oid or "")
            if not sid or sid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                continue
            payload = self._get_oco_order_payload_cached(sid, payload_cache)
            has_t = self._oco_payload_has_target(payload)
            has_s = self._oco_payload_has_stop(payload)
            live_ok, live_rs = self._smart_order_payload_is_live_protection(payload)
            if has_t and has_s and live_ok:
                complete += 1
            elif has_t and has_s and not live_ok:
                logger.warning(
                    f"[REARM] OCO not live for {symbol} id={sid} ({live_rs}); "
                    f"target={has_t}, stop={has_s}."
                )
            else:
                logger.warning(
                    f"[REARM] Incomplete OCO protection for {symbol} id={sid}: target={has_t}, stop={has_s}."
                )
        if complete < expected_legs:
            return False, f"incomplete OCO protection (expected={expected_legs}, complete={complete})"
        return True, ""

    def _cancel_oco_ids_for_symbol(self, symbol: str, rec: dict) -> tuple[int, list[str]]:
        """
        Cancel all known OCO smart-order ids for symbol. Best effort.
        Returns (cancelled_count, errors).
        """
        if self.client is None:
            return 0, ["Groww client not initialized"]
        rec_norm = self._normalize_active_trade_record(rec or {})
        order_ids = self._oco_ids_from_record(rec_norm)
        cancelled = 0
        errors: list[str] = []
        for oid in order_ids:
            sid = str(oid or "")
            if not sid or sid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                continue
            try:
                self.client.cancel_smart_order(
                    segment=self.client.SEGMENT_FNO,
                    smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                    smart_order_id=sid,
                )
                cancelled += 1
            except Exception as e:
                errors.append(f"{sid}: {e}")
        return cancelled, errors

    @staticmethod
    def _rearm_alert_line(
        *,
        source: str,
        symbol: str,
        attempted: bool,
        forced: bool,
        retried: bool,
        success: bool,
        detail: str = "",
    ) -> str | None:
        """
        Build a single human-readable rearm alert line.
        Exactly one line should be emitted per symbol rearm attempt.
        """
        if not attempted:
            return None
        mode = "forced" if forced else "normal"
        retry = " +retry" if retried else ""
        detail_up = str(detail or "").upper()
        integrity_ok = "INTEGRITY=OK" in detail_up and "INTEGRITY_FAILED" not in detail_up
        if success:
            status = "✅"
        elif integrity_ok:
            # Re-arm action may fail/skip while protection remains intact.
            status = "⚠️"
        else:
            status = "❌"
        msg = f"{status} REARM[{source}] {symbol}: {mode}{retry}"
        if detail:
            msg += f" ({detail})"
        return msg

    def _execute_rearm_for_symbol(
        self,
        *,
        source: str,
        symbol: str,
        rec: dict,
        pos: dict,
        force_initial: bool,
    ) -> dict:
        """
        Unified per-symbol rearm flow used by both scheduled maintenance and admin REARM.
        Returns:
          {
            "rearmed": int,
            "line": str|None,
            "attempted": bool,
            "success": bool,
            "primary_alert": str,
          }
        """
        payload_cache: dict[str, dict] = {}
        rec_norm = self._normalize_active_trade_record(rec)
        rec_norm = self._sync_oco_snapshot_from_broker(symbol, rec_norm, payload_cache=payload_cache)
        pos = pos or {}
        open_qty = abs(self._position_quantity(pos))

        force_rearm = bool(force_initial)
        force_reasons: list[str] = []
        if force_initial:
            force_reasons.append("manual_force")

        if rec_norm.get("smart_order_id") != self.DRY_RUN_PLACEHOLDER_ORDER_ID and not force_rearm:
            oco_ids = self._oco_ids_from_record(rec_norm)
            if not oco_ids:
                force_rearm = True
                force_reasons.append("oco_missing")
            elif not self._has_modifiable_oco(symbol, rec_norm):
                force_rearm = True
                force_reasons.append("oco_stale")
            elif not self._has_target_legs_on_broker(symbol, rec_norm, payload_cache=payload_cache):
                force_rearm = True
                force_reasons.append("target_leg_missing")
            else:
                complete, reason = self._has_complete_oco_protection(
                    symbol, rec_norm, open_qty, payload_cache=payload_cache
                )
                if not complete:
                    force_rearm = True
                    force_reasons.append(f"integrity:{reason}")

        ok, alert = self._rearm_oco_for_open_position(
            symbol, rec_norm, pos, force=force_rearm, payload_cache=payload_cache
        )
        attempted = bool(force_rearm or ok or alert)
        retried = False
        success = bool(ok)
        rearmed_inc = 1 if ok else 0
        detail_parts: list[str] = []
        if force_reasons:
            detail_parts.append("forced_by=" + ",".join(force_reasons))
        if ok:
            detail_parts.append("re-armed")
        elif alert:
            detail_parts.append(str(alert).replace("\n", " | "))

        rec_after = self._get_active_trade_record(symbol) or rec_norm
        complete_after, _ = self._has_complete_oco_protection(
            symbol, rec_after, open_qty, payload_cache=payload_cache
        )
        if not complete_after:
            rec_force = dict(rec_after)
            rec_force["protection_armed_date"] = ""
            ok2, alert2 = self._rearm_oco_for_open_position(
                symbol, rec_force, pos, force=True, payload_cache=payload_cache
            )
            retried = True
            if ok2:
                rearmed_inc += 1
                success = True
                detail_parts.append("integrity_retry=ok")
            elif alert2:
                detail_parts.append("integrity_retry=" + str(alert2).replace("\n", " | "))
            rec_final = self._get_active_trade_record(symbol) or rec_after
            final_ok, final_reason = self._has_complete_oco_protection(
                symbol, rec_final, open_qty, payload_cache=payload_cache
            )
            if not final_ok:
                success = False
                detail_parts.append(f"integrity_failed:{final_reason}")
                logger.error(
                    f"[REARM] Final integrity failure for {symbol} after {source} re-arm "
                    f"(forced={force_rearm}, retried={retried}): {final_reason}"
                )
            else:
                success = True
                detail_parts.append("integrity=ok")
        elif attempted:
            detail_parts.append("integrity=ok")

        line = self._rearm_alert_line(
            source=source,
            symbol=symbol,
            attempted=attempted,
            forced=force_rearm,
            retried=retried,
            success=success,
            detail="; ".join(detail_parts),
        )
        return {
            "rearmed": int(rearmed_inc),
            "line": line,
            "attempted": bool(attempted),
            "success": bool(success),
            "primary_alert": str(alert or ""),
        }

    def _run_scheduled_rearm_for_symbols(
        self,
        *,
        source: str,
        updated: dict,
        positions_map: dict[str, dict],
        stats: dict,
    ) -> None:
        for symbol, rec in list(updated.items()):
            try:
                pos = positions_map.get(symbol)
                if not pos:
                    continue
                result = self._execute_rearm_for_symbol(
                    source=source,
                    symbol=symbol,
                    rec=rec,
                    pos=pos,
                    force_initial=False,
                )
                stats["rearmed"] += int(result.get("rearmed") or 0)
                line = result.get("line")
                if line:
                    stats["alerts"].append(str(line))
            except Exception as e:
                logger.warning(f"[REARM] Failed for {symbol}: {e}")
                line = self._rearm_alert_line(
                    source=source,
                    symbol=symbol,
                    attempted=True,
                    forced=True,
                    retried=False,
                    success=False,
                    detail=f"exception:{e}",
                )
                if line:
                    stats["alerts"].append(line)

    def maintain_active_trades(self, *, include_rearm: bool = True) -> dict:
        """
        Position/book maintenance (split concerns):

        1) Reconcile: align local active book with broker open positions — journal
           closed rows and remove stale actives. Does **not** modify broker SL/targets;
           use event-driven `sync_target_progression_for_symbol`, VOL_SL scheduler,
           management commands, or `AUTO_REARM_PROTECTION` for that.
        2) Optionally re-arm OCO when `include_rearm` and `AUTO_REARM_PROTECTION`.
        """
        stats = {"active": 0, "removed": 0, "rearmed": 0, "alerts": []}
        if self.client is None:
            return stats

        local = self._load_active_trades()
        if not local:
            return stats

        # Keep completed-trade journal accurate by auto-backfilling any pending exits.
        try:
            backfilled = int(self._backfill_pending_completed_trades())
            if backfilled > 0:
                logger.info(f"[RECONCILE] Backfilled {backfilled} pending completed trade(s) with broker exit fills.")
        except Exception as e:
            logger.warning(f"[RECONCILE] Pending completed-trade backfill failed: {e}")

        positions_map = self._active_positions_map()
        stats["active"] = len(positions_map)
        active_symbols = set(positions_map.keys())

        # Reconcile: remove locals that are not open on broker; keep opens unchanged here.
        updated = {}
        for symbol, rec in local.items():
            if symbol in active_symbols:
                updated[symbol] = self._normalize_active_trade_record(rec)
            else:
                stats["removed"] += 1
                try:
                    nrec = self._normalize_active_trade_record(rec)
                    inferred = self._reconcile_exit_fill_from_positions(symbol=symbol, active_rec=nrec) or {}
                    has_fill = (
                        float(self._num_or(inferred.get("exit_price"), 0.0)) > 0
                        and int(abs(self._num_or(inferred.get("exit_qty"), 0))) > 0
                    )
                    fallback_exit = (
                        float(inferred.get("exit_price"))
                        if has_fill
                        else 0.0
                    )
                    fallback_qty = (
                        int(abs(self._num_or(inferred.get("exit_qty"), 0)))
                        if has_fill
                        else int(self._num_or(nrec.get("entry_qty"), 0))
                    )
                    close_reason = (
                        "RECONCILE_OCO_FILL"
                        if inferred.get("source") == "oco_legs"
                        else "RECONCILE_BROKER_FILL"
                        if inferred.get("source") == "broker_positions"
                        else "RECONCILE_FLAT"
                    )
                    self._record_completed_trade(
                        symbol=symbol,
                        active_rec=nrec,
                        close_reason=close_reason,
                        exit_price=fallback_exit,
                        exit_qty=fallback_qty,
                        close_order_id=str(inferred.get("order_id") or ""),
                        exit_pending=not has_fill,
                        exit_fill_source=str(inferred.get("source") or ("pending" if not has_fill else "")),
                    )
                except Exception as e:
                    logger.warning(f"[RECONCILE] Could not journal closed trade for {symbol}: {e}")
                logger.info(f"[RECONCILE] Removing stale local trade {symbol} (not active on broker).")
        self._save_all_trades(updated)

        if not (include_rearm and Config.AUTO_REARM_PROTECTION):
            return stats

        # Re-arm protection for carry-forward open positions.
        self._run_scheduled_rearm_for_symbols(
            source="schedule",
            updated=updated,
            positions_map=positions_map,
            stats=stats,
        )
        return stats

    def run_auto_rearm_protection_tick(self) -> dict:
        """
        Re-arm-only maintenance tick.
        Intended for a dedicated scheduler separate from reconcile polling.
        """
        stats = {"active": 0, "removed": 0, "rearmed": 0, "alerts": []}
        if self.client is None or not Config.AUTO_REARM_PROTECTION:
            return stats
        updated = self._load_active_trades()
        if not updated:
            return stats
        positions_map = self._active_positions_map()
        stats["active"] = len(positions_map)
        self._run_scheduled_rearm_for_symbols(
            source="schedule",
            updated=updated,
            positions_map=positions_map,
            stats=stats,
        )
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
                result = self._execute_rearm_for_symbol(
                    source="admin",
                    symbol=symbol,
                    rec=rec,
                    pos=pos,
                    force_initial=True,
                )
                rearmed += int(result.get("rearmed") or 0)
                line = result.get("line")
                if line:
                    details.append(f"• {line}")
                primary_alert = str(result.get("primary_alert") or "")
                attempted = bool(result.get("attempted"))
                success = bool(result.get("success"))
                if primary_alert and "already attached" in primary_alert.lower():
                    already += 1
                elif primary_alert and not success:
                    failed += 1
                elif not attempted:
                    skipped += 1
            except Exception as e:
                failed += 1
                line = self._rearm_alert_line(
                    source="admin",
                    symbol=symbol,
                    attempted=True,
                    forced=True,
                    retried=False,
                    success=False,
                    detail=f"exception:{e}",
                )
                details.append(f"• {line or f'{symbol}: failed ({e})'}")

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
            inferred = self._reconcile_exit_fill_from_positions(symbol=symbol, active_rec=rec) or {}
            has_fill = (
                float(self._num_or(inferred.get("exit_price"), 0.0)) > 0
                and int(abs(self._num_or(inferred.get("exit_qty"), 0))) > 0
            )
            fallback_exit = (
                float(inferred.get("exit_price"))
                if has_fill
                else 0.0
            )
            fallback_qty = (
                int(abs(self._num_or(inferred.get("exit_qty"), 0)))
                if has_fill
                else int(self._num_or(rec.get("entry_qty"), 0))
            )
            close_reason = (
                "RECONCILE_OCO_FILL"
                if inferred.get("source") == "oco_legs"
                else "RECONCILE_BROKER_FILL"
                if inferred.get("source") == "broker_positions"
                else "RECONCILE_FLAT"
            )
            self._record_completed_trade(
                symbol=symbol,
                active_rec=rec,
                close_reason=close_reason,
                exit_price=fallback_exit,
                exit_qty=fallback_qty,
                close_order_id=str(inferred.get("order_id") or ""),
                exit_pending=not has_fill,
                exit_fill_source=str(inferred.get("source") or ("pending" if not has_fill else "")),
            )
            self._remove_active_trade(symbol)
            return False
        except Exception as e:
            logger.warning(f"Symbol reconcile failed for {symbol}: {e}")
            # Fail-open: don't delete local trade on transient broker/API error.
            return True

    def resolve_active_symbol(self, symbol: str) -> str | None:
        """
        Resolve a management symbol to an active-book symbol.
        Single source of truth for symbol reconciliation across components.
        """
        s = str(symbol or "").upper().strip()
        if not s:
            return None
        active = self._load_active_trades()
        if s in active:
            return s
        if not active:
            return None

        # Fast-path variant matching (month-abbr/month-num and underlying aliases).
        variant_hits = [v for v in self._variant_list(s) if v in active]
        if len(variant_hits) == 1:
            return variant_hits[0]
        if len(variant_hits) > 1:
            return variant_hits[0]

        req = self._parse_option_symbol_parts(s)
        if not req:
            return None

        candidates: list[str] = []
        for k in active.keys():
            got = self._parse_option_symbol_parts(str(k))
            if not got:
                continue
            if (
                str(req.get("strike") or "") == str(got.get("strike") or "")
                and str(req.get("option_type") or "") == str(got.get("option_type") or "")
                and self._underlying_match(str(req.get("underlying") or ""), str(got.get("underlying") or ""))
            ):
                candidates.append(str(k))

        if len(candidates) == 1:
            return candidates[0]
        if not candidates:
            return None

        # If multiple candidates exist, prefer exact parsed expiry match when available.
        req_exp = req.get("expiry")
        if isinstance(req_exp, datetime.date):
            exact = []
            for c in candidates:
                got = self._parse_option_symbol_parts(c)
                if got and got.get("expiry") == req_exp:
                    exact.append(c)
            if len(exact) == 1:
                return exact[0]

        return candidates[0]

    def sync_target_progression_for_symbol(self, symbol: str) -> str | None:
        """
        Immediate broker-validated progression check for one symbol.
        Used by target-hit message flow to avoid waiting for periodic reconcile.
        """
        resolved = self.resolve_active_symbol(symbol)
        if not resolved:
            return None
        rec = self._get_active_trade_record(resolved)
        if not rec:
            return None
        pos, qty = self._get_open_position_for_symbol(resolved)
        if qty <= 0:
            return None
        return self._sync_multi_target_progress_for_symbol(resolved, rec, pos or {})

    def admin_sync_target_progression(self, symbol_filter: str | None = None) -> str:
        """
        Admin sweep: run broker-validated multi-target SL progression for active trade(s).
        symbol_filter: None = all actives; else one trading_symbol (resolved via active book).
        """
        if self.client is None:
            return "⚠️ PROGRESS skipped: Groww client not initialized."

        active = self._load_active_trades()
        if not active:
            return "ℹ️ PROGRESS: no active trades."

        if symbol_filter:
            sym = str(symbol_filter).upper().strip()
            resolved = self.resolve_active_symbol(sym)
            if not resolved:
                return f"⚠️ PROGRESS: no active trade matched for {sym}."
            symbols = [resolved]
        else:
            symbols = [str(s) for s in active.keys() if str(s)]

        applied: list[str] = []
        errors: list[str] = []
        unchanged = 0
        for sym in symbols:
            try:
                msg = self.sync_target_progression_for_symbol(sym)
                if msg:
                    applied.append(f"• {sym}\n  {msg}")
                else:
                    unchanged += 1
            except Exception as e:
                errors.append(f"• {sym}: {e}")
                logger.warning(f"[PROGRESS] Admin sweep failed for {sym}: {e}")

        lines = [
            "🪜 Target progression (admin)",
            f"Processed: {len(symbols)} | broker SL update / message: {len(applied)} | no change: {unchanged}",
        ]
        if applied:
            lines.append("\n".join(applied))
        if errors:
            lines.append("Errors:\n" + "\n".join(errors))
        if not applied and not errors:
            lines.append(
                "No progression applied. Typical reasons: fewer than 2 OCO legs, "
                "no open broker qty, or SL stage already matches inferred fills."
            )
        return "\n".join(lines)

    def _atomic_json_write(self, path: str, data) -> None:
        """
        Write *data* to *path* atomically: serialise to a sibling .tmp file,
        then os.replace() so the target is never partially written.
        The file-level lock ensures concurrent to_thread() calls serialise.
        """
        tmp = path + ".tmp"
        with self._file_lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            os.replace(tmp, path)

    def _load_active_trades(self) -> dict:
        return db.get_active_trades(account_id=self.account_id)

    def _save_all_trades(self, trades_dict: dict) -> None:
        db.replace_all_active_trades(trades_dict, account_id=self.account_id)

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
                            "target_order_id": str(item.get("target_order_id") or ""),
                            "stop_order_id": str(item.get("stop_order_id") or ""),
                        }
                    )
            if norm_legs:
                out["oco_legs"] = norm_legs
                if not out.get("smart_order_id"):
                    out["smart_order_id"] = str(norm_legs[0].get("smart_order_id") or "")
                inferred = []
                for leg in sorted(norm_legs, key=lambda x: int(x.get("target_index") or 0)):
                    try:
                        t = float(leg.get("target_signal") or 0)
                    except Exception:
                        t = 0.0
                    if t > 0 and t not in inferred:
                        inferred.append(t)
                if inferred:
                    out["signal_targets"] = inferred[:3]
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
            raw_targets = out.get("signal_targets")
            norm_targets = []
            if isinstance(raw_targets, list):
                for t in raw_targets:
                    try:
                        tv = float(t)
                    except Exception:
                        continue
                    if tv > 0:
                        norm_targets.append(tv)
            if norm_targets:
                out["signal_targets"] = norm_targets[:3]
            elif float(out.get("target") or 0) > 0:
                out["signal_targets"] = [float(out.get("target") or 0)]
            stage = str(out.get("sl_progress_stage") or "").upper().strip()
            out["sl_progress_stage"] = stage if stage in ("NONE", "T1_HIT", "T2_HIT") else "NONE"
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
            if "exit_in_progress" in out:
                v = out.get("exit_in_progress")
                if isinstance(v, bool):
                    out["exit_in_progress"] = v
                else:
                    sv = str(v or "").strip().lower()
                    out["exit_in_progress"] = sv in ("1", "true", "yes", "y", "on")
            if "exit_started_at" in out:
                try:
                    out["exit_started_at"] = float(out.get("exit_started_at") or 0)
                except Exception:
                    out["exit_started_at"] = 0.0
            if "exit_lock_id" in out and out.get("exit_lock_id") is not None:
                out["exit_lock_id"] = str(out.get("exit_lock_id") or "")
            if "exit_reason" in out and out.get("exit_reason") is not None:
                out["exit_reason"] = str(out.get("exit_reason") or "")
            # Policy bookkeeping: if sl_true missing, treat current stop_loss as authoritative channel SL.
            st = float(out.get("sl_true") or 0)
            if st <= 0 and float(out.get("stop_loss") or 0) > 0:
                out["sl_true"] = float(out.get("stop_loss") or 0)
            return out
        return {"smart_order_id": str(record or "")}

    def _exit_lock_ttl_seconds(self) -> int:
        cfg_ttl = getattr(Config, "EXIT_LOCK_TTL_SECONDS", self.EXIT_LOCK_TTL_SECONDS)
        try:
            ttl = int(float(cfg_ttl))
        except Exception:
            ttl = int(self.EXIT_LOCK_TTL_SECONDS)
        return max(30, ttl)

    def _get_exit_lock(self, symbol: str) -> dict:
        rec = self._get_active_trade_record(symbol) or {}
        active = bool(rec.get("exit_in_progress"))
        started_at = float(self._num_or(rec.get("exit_started_at"), 0.0))
        lock_id = str(rec.get("exit_lock_id") or "")
        reason = str(rec.get("exit_reason") or "")
        age = max(0.0, time.time() - started_at) if started_at > 0 else 0.0
        ttl = float(self._exit_lock_ttl_seconds())
        stale = bool(active and started_at > 0 and age > ttl)
        return {
            "active": active,
            "stale": stale,
            "lock_id": lock_id,
            "started_at": started_at,
            "age_seconds": age,
            "reason": reason,
        }

    def _set_exit_lock(self, symbol: str, *, lock_id: str, reason: str = "") -> None:
        trades = self._load_active_trades()
        if symbol not in trades:
            return
        rec = self._normalize_active_trade_record(trades.get(symbol))
        rec["exit_in_progress"] = True
        rec["exit_lock_id"] = str(lock_id or "")
        rec["exit_started_at"] = float(time.time())
        rec["exit_reason"] = str(reason or "")
        trades[symbol] = rec
        self._save_all_trades(trades)

    def _clear_exit_lock(self, symbol: str) -> None:
        trades = self._load_active_trades()
        if symbol not in trades:
            return
        rec = self._normalize_active_trade_record(trades.get(symbol))
        rec["exit_in_progress"] = False
        rec.pop("exit_lock_id", None)
        rec.pop("exit_started_at", None)
        rec.pop("exit_reason", None)
        trades[symbol] = rec
        self._save_all_trades(trades)

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
        signal_targets: list[float] | None = None,
        sl_progress_stage: str | None = None,
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
        if signal_targets is not None:
            norm_targets = []
            for t in (signal_targets or []):
                try:
                    tv = float(t)
                except Exception:
                    continue
                if tv > 0:
                    norm_targets.append(tv)
            current["signal_targets"] = norm_targets[:3]
        if sl_progress_stage is not None:
            stage = str(sl_progress_stage or "").upper().strip()
            current["sl_progress_stage"] = stage if stage in ("NONE", "T1_HIT", "T2_HIT") else "NONE"
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
                        "target_order_id": str(leg.get("target_order_id") or ""),
                        "stop_order_id": str(leg.get("stop_order_id") or ""),
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

    @staticmethod
    def _sorted_oco_legs(rec: dict | None) -> list[dict]:
        legs = (rec or {}).get("oco_legs")
        out: list[dict] = []
        if not isinstance(legs, list):
            return out
        for i, leg in enumerate(legs):
            if not isinstance(leg, dict):
                continue
            row = dict(leg)
            try:
                row["target_index"] = int(float(row.get("target_index") or (i + 1)))
            except Exception:
                row["target_index"] = i + 1
            try:
                row["qty"] = max(0, int(float(row.get("qty") or 0)))
            except Exception:
                row["qty"] = 0
            row["status"] = str(row.get("status") or "ACTIVE").upper().strip() or "ACTIVE"
            out.append(row)
        out.sort(key=lambda x: int(x.get("target_index") or 0))
        return out

    @classmethod
    def _derive_progression_sl_true(cls, rec: dict, hit_stage: str) -> float:
        cur_true = float(cls._num_or(rec.get("sl_true"), 0.0))
        base_sl = float(cls._num_or(rec.get("stop_loss"), 0.0))
        entry = float(cls._num_or(rec.get("entry_avg_price"), 0.0))
        signal_targets = rec.get("signal_targets") if isinstance(rec.get("signal_targets"), list) else []
        t1_signal = float(cls._num_or(signal_targets[0] if signal_targets else rec.get("target"), 0.0))

        desired = cur_true if cur_true > 0 else base_sl
        if hit_stage == "T1_HIT" and entry > 0:
            desired = max(desired, entry)
        elif hit_stage == "T2_HIT" and t1_signal > 0:
            desired = max(desired, t1_signal)
        return float(desired if desired > 0 else 0.0)

    def _recompute_leg_progress(
        self,
        *,
        rec: dict,
        open_qty: int,
    ) -> tuple[dict, str]:
        out = self._normalize_active_trade_record(rec)
        legs = self._sorted_oco_legs(out)
        if not legs:
            return out, "NONE"
        entry_qty = int(self._num_or(out.get("entry_qty"), 0))
        if entry_qty <= 0:
            entry_qty = sum(int(self._num_or(l.get("qty"), 0)) for l in legs)
        if entry_qty <= 0:
            return out, "NONE"
        open_q = max(0, int(open_qty or 0))
        closed_qty = max(0, entry_qty - open_q)

        cum = 0
        for leg in legs:
            lq = max(0, int(self._num_or(leg.get("qty"), 0)))
            prev = str(leg.get("status") or "ACTIVE").upper()
            if prev in ("CANCELLED", "STALE"):
                continue
            cum += lq
            leg["status"] = "FILLED" if lq > 0 and closed_qty >= cum else "ACTIVE"

        highest_hit = 0
        for leg in legs:
            if str(leg.get("status") or "").upper() == "FILLED":
                highest_hit = max(highest_hit, int(self._num_or(leg.get("target_index"), 0)))
        stage = "NONE"
        if highest_hit >= 2:
            stage = "T2_HIT"
        elif highest_hit >= 1:
            stage = "T1_HIT"

        out["oco_legs"] = legs
        out["sl_progress_stage"] = stage
        out["sl_true"] = self._derive_progression_sl_true(out, stage)
        return out, stage

    def _active_leg_order_ids(self, rec: dict) -> list[str]:
        ids: list[str] = []
        for leg in self._sorted_oco_legs(rec):
            if str(leg.get("status") or "").upper() != "ACTIVE":
                continue
            oid = str(leg.get("smart_order_id") or "")
            if oid and oid not in ids:
                ids.append(oid)
        if ids:
            return ids
        return self._oco_ids_from_record(rec)

    def _sync_multi_target_progress_for_symbol(self, symbol: str, rec: dict, position: dict) -> str | None:
        rec_norm = self._normalize_active_trade_record(rec)
        legs = self._sorted_oco_legs(rec_norm)
        if len(legs) < 2:
            return None
        open_qty = abs(self._position_quantity(position or {}))
        next_rec, new_stage = self._recompute_leg_progress(rec=rec_norm, open_qty=open_qty)
        prev_stage = str(rec_norm.get("sl_progress_stage") or "NONE").upper().strip() or "NONE"
        prev_true = float(self._num_or(rec_norm.get("sl_true"), rec_norm.get("stop_loss")))
        next_true = float(self._num_or(next_rec.get("sl_true"), 0.0))
        if next_true <= 0:
            return None

        stage_advanced = new_stage != prev_stage
        sl_changed = abs(next_true - prev_true) > 1e-6
        if not stage_advanced and not sl_changed:
            return None

        logger.info(
            f"[PROGRESS] {symbol}: stage {prev_stage} -> {new_stage}, "
            f"sl_true {prev_true:.2f} -> {next_true:.2f} "
            f"(open_qty={open_qty}, legs={len(legs)})"
        )

        broker_sl = float(next_true)
        if bool(next_rec.get("vol_sl_needs_restore")):
            widened = self._widened_sl_from_true(
                symbol, float(next_true), float(next_rec.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT)
            )
            if widened is not None:
                broker_sl = float(widened)

        if self.client is not None:
            ids = self._active_leg_order_ids(next_rec)
            prog_sl_cache: dict[str, dict] = {}
            for oid in ids:
                try:
                    broker_sl = self._modify_oco_sl(symbol, oid, broker_sl, payload_cache=prog_sl_cache)
                except Exception as e:
                    if self._is_stale_oco_error(str(e)):
                        logger.info(f"[PROGRESS] {symbol}: stale OCO leg skipped during SL progression (id={oid}).")
                        continue
                    logger.warning(f"[PROGRESS] SL sync failed for {symbol} id={oid}: {e}")

        self._save_active_trade(
            symbol,
            str(next_rec.get("smart_order_id") or ""),
            stop_loss=float(broker_sl),
            sl_progress_stage=str(new_stage),
            oco_legs=next_rec.get("oco_legs"),
            signal_targets=next_rec.get("signal_targets"),
        )
        self._update_active_trade_levels(
            symbol,
            stop_loss=float(broker_sl),
            sl_true=float(next_true),
            sl_widened=(broker_sl if bool(next_rec.get("vol_sl_needs_restore")) else 0.0),
            sl_mode=("WIDENED" if bool(next_rec.get("vol_sl_needs_restore")) else "NORMAL"),
            sl_widen_pct=float(next_rec.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT),
        )
        logger.info(
            f"[PROGRESS] {symbol}: progression applied. broker_sl={broker_sl:.2f}, "
            f"sl_true={next_true:.2f}, stage={new_stage}"
        )
        if new_stage == "T1_HIT":
            return f"🔐 SL progression for {symbol}: T1 filled, SL moved to entry ({next_true})."
        if new_stage == "T2_HIT":
            return f"🔐 SL progression for {symbol}: T2 filled, SL moved to T1 signal ({next_true})."
        return None

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

    def _broker_sl_for_target_modify(self, symbol: str, payload: dict) -> float:
        """Broker SL level to preserve when applying a target-only OCO modify."""
        sl_px = float(self._oco_payload_stop_trigger_price(payload))
        if sl_px > 0:
            return sl_px
        sym = str(symbol or "").strip()
        if not sym:
            return 0.0
        nrec = self._normalize_active_trade_record(self._get_active_trade_record(sym) or {})
        broker_sl = float(self._num_or(nrec.get("stop_loss"), 0.0))
        if broker_sl > 0:
            return broker_sl
        st = float(self._num_or(nrec.get("sl_true"), 0.0))
        if st <= 0:
            return 0.0
        if bool(nrec.get("vol_sl_needs_restore")):
            widened = self._widened_sl_from_true(
                sym, st, float(nrec.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT)
            )
            if widened is not None and float(widened) > 0:
                return float(widened)
        return float(st)

    def _require_live_oco_payload_for_modify(self, order_id: str, payload: dict) -> None:
        """
        Block modify_smart_order when the fetched smart-order payload shows expired
        session / terminal status. Empty payload skips (transient API failure — caller
        may still attempt modify). Message includes \"cannot be modified\" for
        _is_stale_oco_error routing on management paths.
        """
        oid = str(order_id or "").strip()
        if not oid or oid == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
            return
        if not payload:
            return
        live_ok, live_rs = self._smart_order_payload_is_live_protection(payload)
        if live_ok:
            return
        raise RuntimeError(f"OCO id={oid} not live ({live_rs}); cannot be modified.")

    def _modify_oco_sl(
        self,
        symbol: str,
        order_id: str,
        new_sl: float,
        *,
        payload_cache: dict[str, dict] | None = None,
    ) -> float:
        """
        Modify OCO stop-loss on broker. Raises if smart order is expired/cancelled
        (same stale handling as broker \"cannot be modified\" errors).
        """
        if not self.client:
            raise RuntimeError("Groww client not initialized")
        sl = self._round_price_to_tick(new_sl, mode="nearest")
        if sl <= 0:
            raise RuntimeError(f"Invalid SL after tick-rounding: {new_sl}")

        payload: dict = {}
        oid = str(order_id or "")
        if oid and oid != self.DRY_RUN_PLACEHOLDER_ORDER_ID:
            try:
                payload = self._get_oco_order_payload_cached(oid, payload_cache)
            except Exception as e:
                logger.debug(f"[OCO] target preserve: get_smart_order failed for {oid}: {e}")

        self._require_live_oco_payload_for_modify(oid, payload)

        tgt_px = float(self._oco_payload_target_trigger_price(payload))
        if tgt_px <= 0 and symbol:
            rec_fb = self._get_active_trade_record(str(symbol).strip()) or {}
            for leg in self._sorted_oco_legs(rec_fb):
                if str(leg.get("smart_order_id") or "") != oid:
                    continue
                tb = float(self._num_or(leg.get("target_broker"), 0.0))
                if tb > 0:
                    tgt_px = tb
                    break
                ts = float(self._num_or(leg.get("target_signal"), 0.0))
                if ts > 0:
                    tgt_px = float(self._take_profit_order_price(ts))
                    break

        modify_kw: dict = {
            "smart_order_id": order_id,
            "smart_order_type": self.client.SMART_ORDER_TYPE_OCO,
            "segment": self.SMART_SEGMENT,
            "stop_loss": {
                "trigger_price": float(sl),
                "price": float(sl),
                "order_type": "SL",
            },
        }
        if tgt_px > 0:
            tgt_r = self._round_price_to_tick(tgt_px, mode="down")
            if tgt_r > 0:
                modify_kw["target"] = {
                    "trigger_price": float(tgt_r),
                    "price": None,
                    "order_type": "MARKET",
                }
                logger.debug(
                    f"[OCO] modify_smart_order {symbol} id={order_id}: SL={sl} + preserve target={tgt_r}"
                )

        try:
            self._groww_call(self.client.modify_smart_order, **modify_kw)
        finally:
            if payload_cache is not None and oid:
                payload_cache.pop(oid, None)
        return float(sl)

    def _modify_oco_target(
        self,
        order_id: str,
        new_target: float,
        *,
        symbol: str | None = None,
        payload_cache: dict[str, dict] | None = None,
    ) -> float:
        """
        Modify OCO target on broker. Raises if smart order is expired/cancelled.
        """
        if not self.client:
            raise RuntimeError("Groww client not initialized")
        tgt = self._round_price_to_tick(new_target, mode="down")
        if tgt <= 0:
            raise RuntimeError(f"Invalid target after tick-rounding: {new_target}")

        oid = str(order_id or "")
        payload: dict = {}
        if oid and oid != self.DRY_RUN_PLACEHOLDER_ORDER_ID:
            try:
                payload = self._get_oco_order_payload_cached(oid, payload_cache)
            except Exception as e:
                logger.debug(f"[OCO] SL preserve: get_smart_order failed for {oid}: {e}")

        self._require_live_oco_payload_for_modify(oid, payload)

        sl_px = float(self._broker_sl_for_target_modify(str(symbol or "").strip(), payload))
        modify_kw: dict = {
            "smart_order_id": order_id,
            "smart_order_type": self.client.SMART_ORDER_TYPE_OCO,
            "segment": self.SMART_SEGMENT,
            "target": {
                "trigger_price": float(tgt),
                "price": None,
                "order_type": "MARKET",
            },
        }
        if sl_px > 0:
            sl_r = self._round_price_to_tick(sl_px, mode="nearest")
            if sl_r > 0:
                modify_kw["stop_loss"] = {
                    "trigger_price": float(sl_r),
                    "price": float(sl_r),
                    "order_type": "SL",
                }
                logger.debug(
                    f"[OCO] modify_smart_order {symbol or '?'} id={order_id}: target={tgt} + preserve SL={sl_r}"
                )

        try:
            self._groww_call(self.client.modify_smart_order, **modify_kw)
        finally:
            if payload_cache is not None and oid:
                payload_cache.pop(oid, None)
        return float(tgt)

    @staticmethod
    def _is_transient_update_tgt_error(err: str) -> bool:
        low = str(err or "").lower()
        return (
            "unexpected error occurred while processing your request" in low
            or "internal server error" in low
            or "service unavailable" in low
            or "gateway timeout" in low
            or "temporarily unavailable" in low
            or "try again" in low
        )

    @staticmethod
    def _is_stale_oco_error(err: str) -> bool:
        low = str(err or "").lower()
        return (
            "cannot be modified" in low
            or "does not exist" in low
            or "not exist" in low
            or "not found" in low
        )

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
        restore_sl_cache: dict[str, dict] = {}
        attempted = 0
        success = 0
        skipped_stale = 0
        failed: list[str] = []
        last_sl_true = float(sl_true)
        for oid in order_ids:
            attempted += 1
            try:
                last_sl_true = self._modify_oco_sl(symbol, oid, last_sl_true, payload_cache=restore_sl_cache)
                success += 1
            except Exception as e:
                err = str(e or "")
                if self._is_stale_oco_error(err):
                    skipped_stale += 1
                    logger.warning(
                        f"[STAGE 4] RESTORE_SL skipped stale OCO leg for {symbol} id={oid}: {err}"
                    )
                    continue
                failed.append(f"{oid}: {err}")
                logger.warning(
                    f"[STAGE 4] RESTORE_SL leg modify failed for {symbol} id={oid}: {err}"
                )

        # Broker-side OCO can be cancelled/expired while position remains open.
        # If every attempted leg is stale/unmodifiable, create fresh OCO protection.
        if attempted > 0 and success == 0 and skipped_stale > 0 and not failed:
            pos, qty = self._get_open_position_for_symbol(symbol)
            if qty == 0:
                return (
                    f"⚠️ RESTORE_SL failed for {symbol}: broker order is stale and no open position exists."
                    f"{self._ltp_suffix_for_symbol(symbol)}"
                )
            rec_rearm = dict(rec)
            rec_rearm["stop_loss"] = float(last_sl_true)
            # Force immediate re-arm regardless of today's armed date.
            rec_rearm["protection_armed_date"] = ""
            ok, alert = self._rearm_oco_for_open_position(symbol, rec_rearm, pos or {})
            if ok:
                today = self._today_ist_iso()
                self._update_active_trade_levels(
                    symbol,
                    stop_loss=last_sl_true,
                    sl_true=last_sl_true,
                    sl_mode="NORMAL",
                    sl_widened=0.0,
                    vol_sl_needs_restore=False,
                    vol_sl_restore_done_for=today,
                )
                return (
                    f"🔁 RESTORE_SL RE-ARMED: {symbol} had no modifiable OCO; "
                    f"created fresh protection at sl_true={last_sl_true} ({source}).{self._ltp_suffix_for_symbol(symbol)}"
                )
            if alert:
                return f"⚠️ RESTORE_SL re-arm attempt for {symbol}: {alert}{self._ltp_suffix_for_symbol(symbol)}"
            return (
                f"⚠️ RESTORE_SL failed for {symbol}: stale OCO and re-arm did not complete."
                f"{self._ltp_suffix_for_symbol(symbol)}"
            )

        if attempted > 0 and success == 0 and failed:
            return (
                f"⚠️ RESTORE_SL partial: broker SL restore failed for {symbol} "
                f"(attempted={attempted}, stale={skipped_stale}, failed={len(failed)}). "
                f"Last error: {failed[-1]}{self._ltp_suffix_for_symbol(symbol)}"
            )

        if attempted > 0 and success == 0:
            return (
                f"⚠️ RESTORE_SL skipped for {symbol}: no modifiable OCO legs found "
                f"(attempted={attempted}, stale={skipped_stale}, failed={len(failed)})."
                f"{self._ltp_suffix_for_symbol(symbol)}"
            )

        sl_true = float(last_sl_true)
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
            f"🔁 RESTORE_SL SUCCESS: {symbol} broker SL restored to sl_true={sl_true} "
            f"({source}; updated={success}/{attempted}, stale={skipped_stale}, failed={len(failed)})."
            f"{self._ltp_suffix_for_symbol(symbol)}"
        )

    def run_volatility_sl_schedule_tick(
        self,
        *,
        admin_widen: bool = False,
        admin_restore: bool = False,
    ) -> list[str]:
        """
        IST widen/restore tick driven by Config.VOL_SL_*.
        Optional admin_widen / admin_restore force the corresponding branch immediately
        (bypass clock and, for widen, today's widen idempotency flag).
        Returns human-readable lines suitable for admin notifications/logging.
        """
        lines: list[str] = []
        now = self._now_ist()
        admin_any = bool(admin_widen or admin_restore)

        if not admin_any and not Config.VOL_SL_SCHEDULE_ENABLED:
            return lines

        if Config.VOL_SL_SKIP_WEEKENDS and now.weekday() >= 5 and not admin_any:
            return lines

        widen_t = self._parse_hhmm_ist(Config.VOL_SL_WIDEN_TIME_IST)
        restore_t = self._parse_hhmm_ist(Config.VOL_SL_RESTORE_TIME_IST)

        if admin_any:
            want_widen = bool(admin_widen)
            want_restore = bool(admin_restore)
        else:
            if not widen_t or not restore_t:
                lines.append(
                    "⚠️ VOL_SL schedule: invalid VOL_SL_WIDEN_TIME_IST / VOL_SL_RESTORE_TIME_IST (expected HH:MM IST)."
                )
                return lines
            hm = now.strftime("%H:%M")
            want_widen = hm == f"{widen_t[0]:02d}:{widen_t[1]:02d}"
            want_restore = hm == f"{restore_t[0]:02d}:{restore_t[1]:02d}"

        if not want_widen and not want_restore:
            return lines

        restore_source = "admin" if admin_restore else "schedule"

        today = now.date().isoformat()
        pct = float(Config.VOL_SL_WIDEN_PCT or 0.0)
        trades = self._load_active_trades()
        for symbol, raw in trades.items():
            rec = self._normalize_active_trade_record(raw)
            order_ids = self._oco_ids_from_record(rec)
            if not order_ids:
                continue
            if any(oid == self.DRY_RUN_PLACEHOLDER_ORDER_ID for oid in order_ids):
                continue

            pos, qty = self._get_open_position_for_symbol(symbol)
            if qty == 0:
                continue

            sl_true = float(rec.get("sl_true") or rec.get("stop_loss") or 0)
            if sl_true <= 0:
                continue

            if want_widen:
                if not admin_widen and str(rec.get("vol_sl_widen_done_for") or "") == today:
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
                        f"[VOL_SL] Widen ({'admin' if admin_widen else 'schedule'}): {symbol} sl_true={sl_true} "
                        f"pct={pct} → broker_sl={widened} (smart_order_ids={order_ids})."
                    )
                    vol_sl_cache: dict[str, dict] = {}
                    for oid in order_ids:
                        widened = self._modify_oco_sl(symbol, oid, widened, payload_cache=vol_sl_cache)
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
                        f"VOL_SL WIDEN: {symbol} broker SL -> {widened} "
                        f"(sl_true {sl_true}, pct {pct})."
                    )
                except Exception as e:
                    if self._is_stale_oco_error(str(e)) and self.client:
                        try:
                            rec_rearm = dict(rec)
                            rec_rearm["stop_loss"] = float(sl_true)
                            rec_rearm["protection_armed_date"] = ""
                            ok, alert = self._rearm_oco_for_open_position(symbol, rec_rearm, pos or {})
                            if ok:
                                rec_after = self._get_active_trade_record(symbol) or {}
                                retry_ids = self._oco_ids_from_record(rec_after)
                                retry_widened = widened
                                vol_sl_retry_cache: dict[str, dict] = {}
                                for rid in retry_ids:
                                    retry_widened = self._modify_oco_sl(
                                        symbol, rid, retry_widened, payload_cache=vol_sl_retry_cache
                                    )
                                self._update_active_trade_levels(
                                    symbol,
                                    stop_loss=retry_widened,
                                    sl_true=sl_true,
                                    sl_widened=retry_widened,
                                    sl_widen_pct=pct,
                                    sl_mode="WIDENED",
                                    vol_sl_needs_restore=True,
                                    vol_sl_widen_done_for=today,
                                )
                                lines.append(
                                    f"VOL_SL WIDEN RE-ARMED: {symbol} broker SL -> {retry_widened} "
                                    f"(sl_true {sl_true}, pct {pct})."
                                )
                            else:
                                msg = alert or str(e)
                                lines.append(f"VOL_SL widen failed for {symbol}: {msg}")
                        except Exception as rearm_err:
                            logger.error(f"[VOL_SL] Widen re-arm failed for {symbol}: {rearm_err}", exc_info=True)
                            lines.append(f"VOL_SL widen failed for {symbol}: {rearm_err}")
                    else:
                        logger.error(f"[VOL_SL] Widen failed for {symbol}: {e}", exc_info=True)
                        lines.append(f"VOL_SL widen failed for {symbol}: {e}")
                rec = self._get_active_trade_record(symbol) or rec

            if want_restore:
                if not admin_restore and str(rec.get("vol_sl_restore_done_for") or "") == today:
                    continue
                if not bool(rec.get("vol_sl_needs_restore")):
                    if admin_restore:
                        lines.append(
                            f"ℹ️ VOLSL RESTORE skipped {symbol}: not awaiting restore "
                            f"(run VOLSL WIDEN first, or use RESTORE SL <SYMBOL>)."
                        )
                    continue
                if not self.client:
                    lines.append("⚠️ VOL_SL restore skipped: Groww client not initialized.")
                    return lines
                try:
                    msg = self.restore_channel_sl(symbol, source=restore_source)
                    lines.append(msg)
                except Exception as e:
                    logger.error(f"[VOL_SL] Restore failed for {symbol}: {e}", exc_info=True)
                    lines.append(f"VOL_SL restore failed for {symbol}: {e}")

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

    def _get_oco_order_payload(self, smart_order_id: str) -> dict:
        if not self.client or not smart_order_id:
            return {}
        sid = str(smart_order_id)
        try:
            res = self.client.get_smart_order(
                segment=self.client.SEGMENT_FNO,
                smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                smart_order_id=sid,
            )
            out: dict = {}
            if isinstance(res, dict):
                inner = res.get("payload")
                if isinstance(inner, dict):
                    out = dict(inner)
                    for k in (
                        "expire_at",
                        "expireAt",
                        "status",
                        "smart_order_id",
                        "trading_symbol",
                        "quantity",
                        "updated_at",
                        "updatedAt",
                        "created_at",
                        "createdAt",
                    ):
                        if k in res and k not in out:
                            out[k] = res[k]
                else:
                    out = res
            else:
                app_logger.debug(
                    "[OCO_PAYLOAD] get_smart_order id=%s unexpected_response_type=%s",
                    sid,
                    type(res).__name__,
                )
                return {}

            live_ok, live_rs = self._smart_order_payload_is_live_protection(out)
            has_t = self._oco_payload_has_target(out)
            has_s = self._oco_payload_has_stop(out)
            tgt_trig = self._oco_payload_target_trigger_price(out)
            sl_trig = self._oco_payload_stop_trigger_price(out)
            tgt_node = out.get("target")
            sl_node = out.get("stop_loss") or out.get("stopLoss")
            tgt_blk = "dict" if isinstance(tgt_node, dict) else (
                type(tgt_node).__name__ if tgt_node is not None else "missing"
            )
            sl_blk = "dict" if isinstance(sl_node, dict) else (
                type(sl_node).__name__ if sl_node is not None else "missing"
            )
            app_logger.debug(
                "[OCO_PAYLOAD] get_smart_order id=%s status=%s expire_at=%s protection_live=%s "
                "live_reason=%s has_target=%s has_stop=%s target_trigger=%s stop_trigger=%s "
                "target_block=%s stop_block=%s",
                sid,
                out.get("status"),
                out.get("expire_at") or out.get("expireAt"),
                live_ok,
                live_rs,
                has_t,
                has_s,
                tgt_trig if tgt_trig > 0 else None,
                sl_trig if sl_trig > 0 else None,
                tgt_blk,
                sl_blk,
            )
            return out
        except Exception as e:
            app_logger.warning(f"OCO verify get_smart_order failed for id={sid}: {e}")
        return {}

    def _get_oco_order_payload_cached(self, smart_order_id: str, payload_cache: dict[str, dict] | None) -> dict:
        """
        Fetch OCO payload once and reuse within a maintenance/rearm cycle.
        """
        sid = str(smart_order_id or "")
        if not sid:
            return {}
        if payload_cache is None:
            return self._get_oco_order_payload(sid)
        if sid not in payload_cache:
            payload_cache[sid] = self._get_oco_order_payload(sid)
        return payload_cache.get(sid) or {}

    @staticmethod
    def _iter_dict_nodes(payload):
        if isinstance(payload, dict):
            yield payload
            for v in payload.values():
                yield from Trader._iter_dict_nodes(v)
        elif isinstance(payload, list):
            for item in payload:
                yield from Trader._iter_dict_nodes(item)

    @staticmethod
    def _extract_order_id_from_node(node: dict) -> str:
        if not isinstance(node, dict):
            return ""
        for key in (
            "groww_order_id",
            "growwOrderId",
            "order_id",
            "orderId",
            "id",
            "oms_order_id",
            "omsOrderId",
        ):
            val = node.get(key)
            if val:
                return str(val)
        return ""

    @classmethod
    def _extract_oco_leg_child_order_ids(cls, payload: dict) -> tuple[str, str]:
        """
        Best-effort extraction of target-leg and stop-leg broker order ids from
        smart-order payload. Field names can vary by API version.
        """
        target_id = ""
        stop_id = ""
        if not isinstance(payload, dict):
            return target_id, stop_id
        for node in cls._iter_dict_nodes(payload):
            text = cls._order_status_upper_blob(node)
            oid = cls._extract_order_id_from_node(node)
            if not oid:
                continue
            if not target_id and any(k in text for k in ("TARGET", "TAKE_PROFIT", "PROFIT", "LIMIT_EXIT")):
                target_id = oid
            if not stop_id and any(k in text for k in ("STOP", "SL", "STOPLOSS", "STOP_LOSS")):
                stop_id = oid
        return target_id, stop_id

    @classmethod
    def _extract_oco_fill_from_payload(cls, payload: dict) -> dict | None:
        """
        Extract executed fill info from OCO smart-order payload.
        Prefers filled/completed nodes and falls back to best-effort payload-level values.
        """
        if not isinstance(payload, dict):
            return None
        avg_keys = {
            "average_price",
            "avg_price",
            "avgprice",
            "traded_price",
            "trade_price",
            "executed_price",
            "fill_price",
            "price",
        }
        qty_keys = {
            "filled_quantity",
            "filled_qty",
            "traded_quantity",
            "traded_qty",
            "executed_quantity",
            "executed_qty",
            "quantity",
            "qty",
        }
        best: dict | None = None
        for node in cls._iter_dict_nodes(payload):
            text = cls._order_status_upper_blob(node)
            status_good = any(k in text for k in ("FILLED", "COMPLETE", "COMPLETED", "EXECUTED", "DONE"))
            if not status_good:
                continue
            avg = cls._walk_for_numeric_key(node, avg_keys) or 0.0
            qty = cls._walk_for_numeric_key(node, qty_keys) or 0.0
            if avg <= 0 or qty <= 0:
                continue
            cand = {
                "exit_price": float(avg),
                "exit_qty": int(abs(qty)),
                "source": "oco_payload",
                "order_id": cls._extract_order_id_from_node(node),
            }
            if best is None or int(cand["exit_qty"]) > int(best.get("exit_qty") or 0):
                best = cand
        if best:
            return best
        avg = cls._walk_for_numeric_key(payload, avg_keys) or 0.0
        qty = cls._walk_for_numeric_key(payload, qty_keys) or 0.0
        if avg > 0 and qty > 0:
            return {
                "exit_price": float(avg),
                "exit_qty": int(abs(qty)),
                "source": "oco_payload_fallback",
                "order_id": "",
            }
        return None

    def _infer_exit_fill_from_oco_legs(self, *, symbol: str, active_rec: dict | None) -> dict | None:
        """
        Query each saved OCO smart order id and aggregate any filled exit leg(s).
        """
        rec = self._normalize_active_trade_record(active_rec or {})
        oids = self._oco_ids_from_record(rec)
        if not oids or self.client is None:
            return None
        fills: list[dict] = []
        for oid in oids:
            payload = self._get_oco_order_payload(str(oid))
            fill = self._extract_oco_fill_from_payload(payload)
            if fill and float(fill.get("exit_price") or 0) > 0 and int(fill.get("exit_qty") or 0) > 0:
                fill["smart_order_id"] = str(oid)
                fills.append(fill)
        if not fills:
            return None
        total_qty = sum(int(f.get("exit_qty") or 0) for f in fills)
        if total_qty <= 0:
            return None
        weighted = sum(float(f.get("exit_price") or 0.0) * int(f.get("exit_qty") or 0) for f in fills) / float(total_qty)
        order_ids = [str(f.get("order_id") or "") for f in fills if str(f.get("order_id") or "")]
        return {
            "exit_price": float(weighted),
            "exit_qty": int(total_qty),
            "source": "oco_legs",
            "order_id": (order_ids[0] if order_ids else ""),
        }

    @staticmethod
    def _oco_payload_has_target(payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False
        tgt = payload.get("target")
        if not isinstance(tgt, dict):
            return False
        trig = tgt.get("triggerPrice", tgt.get("trigger_price"))
        return trig not in (None, "", 0, "0", "0.0")

    @staticmethod
    def _oco_payload_has_stop(payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False
        sl = payload.get("stop_loss", payload.get("stopLoss"))
        if not isinstance(sl, dict):
            return False
        trig = sl.get("triggerPrice", sl.get("trigger_price"))
        return trig not in (None, "", 0, "0", "0.0")

    @staticmethod
    def _oco_payload_target_trigger_price(payload: dict) -> float:
        """Broker-reported target trigger for an OCO payload (0 when unknown)."""
        if not isinstance(payload, dict):
            return 0.0
        tgt = payload.get("target")
        if not isinstance(tgt, dict):
            return 0.0
        for key in ("trigger_price", "triggerPrice"):
            v = tgt.get(key)
            if v in (None, "", 0, "0", "0.0"):
                continue
            try:
                f = float(v)
                if f > 0:
                    return f
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _oco_payload_stop_trigger_price(payload: dict) -> float:
        """Broker-reported stop trigger for an OCO payload (0 when unknown)."""
        if not isinstance(payload, dict):
            return 0.0
        sl = payload.get("stop_loss") or payload.get("stopLoss")
        if not isinstance(sl, dict):
            return 0.0
        for key in ("trigger_price", "triggerPrice", "price"):
            v = sl.get(key)
            if v in (None, "", 0, "0", "0.0"):
                continue
            try:
                f = float(v)
                if f > 0:
                    return f
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _parse_split_config(raw: str, expected_parts: int) -> list[int]:
        parts = [p.strip() for p in str(raw or "").split(":") if p.strip()]
        if len(parts) != expected_parts:
            raise ValueError(f"Expected {expected_parts} ratio parts, got {len(parts)}")
        vals = [int(p) for p in parts]
        if any(v < 0 for v in vals):
            raise ValueError("Ratio values must be non-negative")
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
        # Policy: for single-lot entries, prioritize runner capture by targeting T2.
        # If T2 is unavailable, gracefully fall back to T1.
        if total_lots == 1 and len(targets) >= 2:
            logger.info(
                f"[STAGE 4] Single-lot policy applied: total_lots=1, routing full qty={qty} to T2 ({float(targets[1]):.2f})."
            )
            return [{"qty": qty, "target_signal": float(targets[1]), "target_index": 2}]
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
        return db.get_pending_management(account_id=self.account_id)

    def _save_pending_management(self, items: list[dict]) -> None:
        db.replace_pending_management(items, account_id=self.account_id)

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


    def _load_pending(self) -> list:
        return db.get_pending_entries(account_id=self.account_id)

    def _save_pending(self, items: list) -> None:
        db.replace_pending_entries(items, account_id=self.account_id)

    def _append_pending(self, item):
        items = self._load_pending()
        items.append(item)
        self._save_pending(items)

    def clear_pending_entries(self) -> int:
        """Clear pending entry queue and return number of removed items."""
        items = self._load_pending()
        removed = len(items)
        self._save_pending([])
        return removed

    def _index_weekly_symbol_encoding_variants(self, symbol: str) -> list[str]:
        """
        Groww/NSE index weeklies use YY + M + DD + strike (month 1-9 unpadded).
        Older builds emitted YY + MM + DD; add the compact token so resolution still hits.
        """
        p = self._parse_option_symbol_parts(symbol)
        if not p or not isinstance(p.get("expiry"), datetime.date):
            return []
        u = str(p.get("underlying") or "").upper()
        yy = str(p.get("yy") or "")
        mm = int(p.get("month_num") or 0)
        day = int(p.get("day") or 0)
        strike = str(p.get("strike") or "")
        ot = str(p.get("option_type") or "").upper()
        if not u or len(yy) != 2 or mm < 1 or mm > 12 or day < 1 or day > 31 or not strike or ot not in ("CE", "PE"):
            return []
        dd = str(day).zfill(2)
        compact = f"{u}{yy}{mm}{dd}{strike}{ot}"
        padded_m = f"{u}{yy}{str(mm).zfill(2)}{dd}{strike}{ot}"
        out = [compact, padded_m]
        return [s for s in out if s and s.upper() != str(symbol).upper()]

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
        variants.extend(self._index_weekly_symbol_encoding_variants(symbol))
        return list(dict.fromkeys(variants))

    @staticmethod
    def _parse_index_weekly_numeric(s: str) -> dict | None:
        """
        Index weekly after YY (NSE-style): Oct–Dec use a 2-digit month 10|11|12; Jan–Sep
        use a single digit 1–9 then DD (2 digits) then strike. Padded months 01–09 use
        a leading 0 on the tail (YY + 0M + DD + strike).
        """
        m = re.match(
            r"^(?P<underlying>[A-Z&_\-]+)(?P<yy>\d{2})(?P<tail>\d+)(?P<option_type>CE|PE)$",
            s,
        )
        if not m:
            return None
        yy_s = m.group("yy")
        tail = m.group("tail")
        if len(tail) < 3:
            return None

        mm: int
        dd: int
        strike: str
        if tail.startswith(("10", "11", "12")):
            if len(tail) < 4:
                return None
            mm = int(tail[:2])
            dd = int(tail[2:4])
            strike = tail[4:]
        elif tail.startswith("0") and len(tail) >= 4:
            mm = int(tail[0:2])
            dd = int(tail[2:4])
            strike = tail[4:]
        else:
            mm = int(tail[0])
            dd = int(tail[1:3])
            strike = tail[3:]

        if mm < 1 or mm > 12 or dd < 1 or dd > 31 or not strike.isdigit():
            return None
        try:
            exp = datetime.date(2000 + int(yy_s), mm, dd)
        except ValueError:
            return None
        return {
            "underlying": m.group("underlying"),
            "yy": yy_s,
            "month_num": mm,
            "day": dd,
            "month_abbr": "",
            "strike": strike,
            "option_type": m.group("option_type"),
            "expiry": exp,
        }

    @staticmethod
    def _parse_option_symbol_parts(symbol: str) -> dict | None:
        """
        Parse Groww option trading symbols into comparable components.
        Supports:
          - Index weekly: NIFTY2650524200CE (yy + m + dd + strike + opt, padded or compact month)
          - Stock monthly: WIPRO26APR200PE   (yy + MON + strike + opt)
        """
        s = str(symbol or "").upper().strip()
        if not s:
            return None
        m = re.match(
            r"^(?P<underlying>[A-Z&_\-]+)"
            r"(?P<yy>\d{2})"
            r"(?P<month_abbr>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
            r"(?P<strike>\d+)"
            r"(?P<option_type>CE|PE)$",
            s,
        )
        if m:
            month_map = {
                "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
            }
            month_num = month_map.get(m.group("month_abbr"), 0)
            return {
                "underlying": m.group("underlying"),
                "yy": m.group("yy"),
                "month_num": month_num,
                "day": None,
                "month_abbr": m.group("month_abbr"),
                "strike": m.group("strike"),
                "option_type": m.group("option_type"),
                "expiry": None,
            }

        return Trader._parse_index_weekly_numeric(s)

    def _fallback_option_candidates(self, symbol: str) -> list[str]:
        """
        Build nearest-match fallback symbols when direct lookup variants miss.
        Matches by underlying + strike + option_type, then prefers closest expiry.
        """
        target = self._parse_option_symbol_parts(symbol)
        if not target:
            return []

        self._load_local_instrument_cache()
        if not self._instrument_cache:
            return []

        candidates: list[tuple[int, str]] = []
        today = datetime.date.today()
        is_index = str(target.get("underlying") or "").upper() in self.INDEX_UNDERLYINGS
        for (_, trading_symbol), row in self._instrument_cache.items():
            if str(row.get("segment") or "").upper() != self.SMART_SEGMENT:
                continue
            parsed = self._parse_option_symbol_parts(trading_symbol)
            if not parsed:
                continue
            if (
                parsed["underlying"] != target["underlying"]
                or parsed["strike"] != target["strike"]
                or parsed["option_type"] != target["option_type"]
            ):
                continue

            score = 999999
            tgt_exp = target.get("expiry")
            got_exp = parsed.get("expiry")
            if is_index:
                # Index policy: only non-stale weeklies; prefer exact target expiry,
                # else nearest future active expiry available in broker instruments.
                if not isinstance(got_exp, datetime.date) or got_exp < today:
                    continue
                if isinstance(tgt_exp, datetime.date) and got_exp == tgt_exp:
                    score = 0
                else:
                    score = (got_exp - today).days
            else:
                # Stock policy: only current-month active contracts.
                if parsed.get("yy") != str(today.year)[-2:] or int(parsed.get("month_num") or 0) != int(today.month):
                    continue
                if isinstance(got_exp, datetime.date) and got_exp < today:
                    continue
                score = 1 if parsed.get("yy") == target.get("yy") else 2
            candidates.append((score, trading_symbol))

        candidates.sort(key=lambda x: (x[0], x[1]))
        return [sym for _, sym in candidates[:20]]

    @staticmethod
    def _underlying_match(a: str, b: str) -> bool:
        a0 = re.sub(r"[^A-Z0-9]", "", str(a or "").upper())
        b0 = re.sub(r"[^A-Z0-9]", "", str(b or "").upper())
        if not a0 or not b0:
            return False
        return a0 == b0 or a0.startswith(b0) or b0.startswith(a0)

    def _active_contract_candidates(self, trade_data: dict, requested_symbol: str) -> list[str]:
        """
        Resolve active contracts from instrument chain using contract keys:
        underlying + strike + option_type.
        """
        strike = re.sub(r"[^\d]", "", str(trade_data.get("strike") or ""))
        option_type = str(trade_data.get("option_type") or "").upper().strip()
        underlying_hint = str(
            trade_data.get("official_symbol")
            or trade_data.get("underlying")
            or trade_data.get("stock")
            or ""
        ).upper()

        req_parts = self._parse_option_symbol_parts(requested_symbol)
        if not strike and req_parts:
            strike = str(req_parts.get("strike") or "")
        if option_type not in ("CE", "PE") and req_parts:
            option_type = str(req_parts.get("option_type") or "").upper()
        if not underlying_hint and req_parts:
            underlying_hint = str(req_parts.get("underlying") or "")

        if not strike or option_type not in ("CE", "PE") or not underlying_hint:
            return []

        self._load_local_instrument_cache()
        if not self._instrument_cache:
            return []

        today = datetime.date.today()
        is_index = self._is_index_option_trade(
            {
                "official_symbol": underlying_hint,
                "stock": requested_symbol,
                "trading_symbol": requested_symbol,
            }
        )
        req_exp = req_parts.get("expiry") if req_parts else None

        ranked: list[tuple[int, str]] = []
        for (_, trading_symbol), row in self._instrument_cache.items():
            if str(row.get("segment") or "").upper() != self.SMART_SEGMENT:
                continue
            parsed = self._parse_option_symbol_parts(trading_symbol)
            if not parsed:
                continue
            if str(parsed.get("strike") or "") != strike:
                continue
            if str(parsed.get("option_type") or "") != option_type:
                continue
            if not self._underlying_match(parsed.get("underlying"), underlying_hint):
                continue

            exp = parsed.get("expiry")
            if is_index:
                # Index contracts can appear in two forms:
                # - Weekly numeric: NIFTY2650523800PE (has explicit date)
                # - Monthly alpha:   NIFTY26APR23800PE (month-only symbol)
                #
                # When the *requested* symbol is weekly (parsed calendar expiry), do not
                # substitute month-only or another week's contract from CSV — wrong LTP
                # and wrong fills (e.g. NIFTY26MAY24200CE vs NIFTY26050524200CE).
                if isinstance(req_exp, datetime.date):
                    if not isinstance(exp, datetime.date) or exp != req_exp:
                        continue
                if isinstance(exp, datetime.date):
                    if exp < today:
                        continue
                    if isinstance(req_exp, datetime.date) and exp == req_exp:
                        score = 0
                    else:
                        score = 10 + max((exp - today).days, 0)
                else:
                    # Month-only index contracts: accept current/next active months.
                    yy = str(parsed.get("yy") or "")
                    mm = int(parsed.get("month_num") or 0)
                    if not yy or mm <= 0:
                        continue
                    cur_yy = int(str(today.year)[-2:])
                    cur_mm = int(today.month)
                    cand_yy = int(yy)
                    month_delta = (cand_yy - cur_yy) * 12 + (mm - cur_mm)
                    if month_delta < 0 or month_delta > 2:
                        continue
                    # Prefer current month monthly over farther monthlies.
                    score = 40 + (month_delta * 31)
            else:
                # Stock monthly:
                # 1) If signal carries explicit month/year, honor it.
                # 2) If missing/invalid, fall back to current active month.
                req_yy = str(req_parts.get("yy") or "") if req_parts else ""
                req_mm = int(req_parts.get("month_num") or 0) if req_parts else 0
                cand_yy = str(parsed.get("yy") or "")
                cand_mm = int(parsed.get("month_num") or 0)

                if req_yy and req_mm > 0:
                    if cand_yy != req_yy or cand_mm != req_mm:
                        continue
                else:
                    if cand_yy != str(today.year)[-2:] or cand_mm != int(today.month):
                        continue
                if isinstance(exp, datetime.date) and exp < today:
                    continue
                score = 100 + (0 if str(trading_symbol).upper() == str(requested_symbol).upper() else 1)
            ranked.append((score, trading_symbol))

        ranked.sort(key=lambda x: (x[0], x[1]))
        return [sym for _, sym in ranked[:30]]

    def _resolution_reason(self, requested_symbol: str, resolved_symbol: str) -> str:
        req = self._parse_option_symbol_parts(requested_symbol)
        got = self._parse_option_symbol_parts(resolved_symbol)
        if not req or not got:
            return "fallback_unparsed"
        if str(requested_symbol).upper() == str(resolved_symbol).upper():
            return "exact_match"
        req_exp = req.get("expiry")
        got_exp = got.get("expiry")
        if isinstance(req_exp, datetime.date) and isinstance(got_exp, datetime.date):
            if got_exp == req_exp:
                return "same_expiry_variant"
            return "nearest_future_weekly"
        if isinstance(got_exp, datetime.date):
            return "weekly_selected"
        return "monthly_fallback_selected"

    def _is_policy_compatible_contract(self, *, requested: str, candidate: str, is_index: bool) -> bool:
        """
        Enforce active-contract policy for fallback substitutions:
          - Index options: candidate must be same expiry as requested and non-stale.
          - Stock options: candidate must be in current month/year and non-stale.
        """
        req = self._parse_option_symbol_parts(requested)
        got = self._parse_option_symbol_parts(candidate)
        if not req or not got:
            return False

        today = datetime.date.today()
        if is_index:
            req_exp = req.get("expiry")
            got_exp = got.get("expiry")
            if not isinstance(req_exp, datetime.date) or not isinstance(got_exp, datetime.date):
                return False
            if got_exp < today:
                return False
            # Same calendar expiry only (no other week / no monthly via this path).
            return got_exp == req_exp

        # Stock options are monthly; enforce current active month.
        year_now_2d = str(today.year)[-2:]
        month_now = int(today.month)
        got_month = int(got.get("month_num") or 0)
        if got.get("yy") != year_now_2d or got_month != month_now:
            return False
        # If a day exists (non-monthly format), do not allow stale.
        got_exp = got.get("expiry")
        if isinstance(got_exp, datetime.date) and got_exp < today:
            return False
        return True

    def _resolve_option_instrument(self, trade_data: dict):
        """
        Returns (exchange, trading_symbol, segment, instrument_dict) or
        (None, None, None, None).
        """
        symbol = trade_data.get("stock")
        if not symbol:
            return None, None, None, None
        self.warm_instruments_cache()
        is_index = self._is_index_option_trade(trade_data)

        tried: set[tuple[str, str]] = set()

        def exchanges_for_lookup() -> list[str]:
            # Options are listed in derivative exchanges; keep cash exchanges only as fallback.
            if is_index:
                return ["NFO", "BFO", "NSE", "BSE"]
            return ["NFO", "NSE", "BFO", "BSE"]

        def try_symbols(candidates: list[str]):
            exchanges = exchanges_for_lookup()
            for sym_variant in candidates:
                for ex in exchanges:
                    key = (ex, sym_variant)
                    if key in tried:
                        continue
                    tried.add(key)
                    try:
                        res = self._get_instrument(exchange=ex, trading_symbol=sym_variant)
                        if res and res.get("segment"):
                            return res["exchange"], sym_variant, res["segment"], res
                    except Exception:
                        continue
            return None

        variants = self._variant_list(symbol)
        active_candidates = self._active_contract_candidates(trade_data, symbol)
        # Resolve the exact constructed symbol (Groww API) before CSV chain matches.
        # Packaged instruments.csv can lag or omit a weekly while still listing monthlies;
        # trying active_candidates first used to pin LTP/orders to the wrong contract.
        hit = try_symbols(list(dict.fromkeys(variants)))
        if hit:
            reason = self._resolution_reason(symbol, hit[1])
            logger.info(
                f"[STAGE 4] Contract resolution: requested={symbol} resolved={hit[1]} "
                f"exchange={hit[0]} segment={hit[2]} reason={reason}"
            )
            return hit

        hit = try_symbols(active_candidates)
        if hit:
            reason = self._resolution_reason(symbol, hit[1])
            logger.info(
                f"[STAGE 4] Contract resolution: requested={symbol} resolved={hit[1]} "
                f"exchange={hit[0]} segment={hit[2]} reason={reason}"
            )
            return hit

        # Try broader, nearest-expiry candidates from local instruments fallback.
        fallback_variants = [
            s
            for s in self._fallback_option_candidates(symbol)
            if self._is_policy_compatible_contract(requested=symbol, candidate=s, is_index=is_index)
        ]
        hit = try_symbols(fallback_variants)
        if hit:
            logger.info(
                f"[STAGE 4] Instrument resolved via fallback candidate: requested={symbol} resolved={hit[1]}"
            )
            return hit

        # Last attempt: refresh Groww instrument dataframe and retry once.
        if self.client is not None:
            try:
                self.client.get_all_instruments()
            except Exception as e:
                logger.debug(f"Groww instrument refresh on miss failed: {e}")
            hit = try_symbols(variants + fallback_variants)
            if hit:
                logger.info(
                    f"[STAGE 4] Instrument resolved after refresh: requested={symbol} resolved={hit[1]}"
                )
                return hit
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
        # Sizing reference for BUY: use range minimum (best-case entry).
        er = trade_data.get("entry_range") or [0, 0]
        if isinstance(er, list) and len(er) >= 2:
            try:
                lo = float(er[0])
                if lo > 0:
                    return lo
            except Exception:
                pass
        return self._num_or(trade_data.get("buy_range_min"), 0.0)

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
            is_index = self._is_index_option_trade(trade_data)
            force_fixed = str(trade_data.get("size_override_mode") or "").upper().strip() == "FIXED"
            sizing_mode = (
                str(Config.INDEX_POSITION_SIZING_MODE if is_index else Config.STOCK_POSITION_SIZING_MODE)
                .strip()
                .lower()
            )
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
            if is_index:
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
        override = trade_data.get("limit_price_override")
        if override is not None:
            try:
                ov = float(override)
                if ov > 0:
                    return ov
            except Exception:
                pass
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
        oco_warnings: list[str] = []
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
                    "price": None,
                    "order_type": "MARKET",
                },
                stop_loss={
                    "trigger_price": float(broker_sl),
                    "price": float(broker_sl),
                    "order_type": "SL",
                },
            )
            logger.info(
                f"OCO leg response {symbol}: ref={oco_ref} target={eff_tgt} sl={broker_sl} "
                f"resp={(str(oco_resp) or '')[:500]}"
            )
            blob = self._order_status_upper_blob(oco_resp)
            if "TARGET" not in blob or "STOP" not in blob:
                logger.warning(
                    f"OCO leg response may be incomplete for {symbol}: ref={oco_ref} "
                    f"(target/sl fields not clearly present in response)"
                )
            smart_order_id = self._extract_smart_order_id(oco_resp, fallback=oco_ref)
            verify_payload = self._get_oco_order_payload(str(smart_order_id or ""))
            target_order_id, stop_order_id = self._extract_oco_leg_child_order_ids(verify_payload)
            has_target = self._oco_payload_has_target(verify_payload)
            if not has_target:
                logger.warning(
                    f"OCO leg verify failed (target missing) for {symbol} id={smart_order_id}. "
                    "Retrying with MARKET target leg."
                )
                # Cancel potentially partial SL-only protection before re-create.
                try:
                    self.client.cancel_smart_order(
                        segment=self.client.SEGMENT_FNO,
                        smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                        smart_order_id=str(smart_order_id or ""),
                    )
                except Exception as ce:
                    logger.warning(f"OCO partial cancel failed for {symbol} id={smart_order_id}: {ce}")

                retry_ref = str(uuid.uuid4())[:15]
                retry_resp = self.client.create_smart_order(
                    smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                    reference_id=retry_ref,
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
                        "price": None,
                        "order_type": "MARKET",
                    },
                    stop_loss={
                        "trigger_price": float(broker_sl),
                        "price": float(broker_sl),
                        "order_type": "SL",
                    },
                )
                logger.info(
                    f"OCO leg retry response {symbol}: ref={retry_ref} target={eff_tgt} sl={broker_sl} "
                    f"resp={(str(retry_resp) or '')[:500]}"
                )
                smart_order_id = self._extract_smart_order_id(retry_resp, fallback=retry_ref)
                verify_payload = self._get_oco_order_payload(str(smart_order_id or ""))
                target_order_id, stop_order_id = self._extract_oco_leg_child_order_ids(verify_payload)
                has_target = self._oco_payload_has_target(verify_payload)
                if not has_target:
                    warn = (
                        f"Target leg missing on broker for {symbol} leg T{int(leg.get('target_index') or 1)} "
                        f"after create+retry."
                    )
                    logger.error(f"[STAGE 4] {warn}")
                    oco_warnings.append(warn)
            created_legs.append(
                {
                    "smart_order_id": str(smart_order_id or ""),
                    "qty": leg_qty,
                    "target_signal": leg_tgt_signal,
                    "target_broker": eff_tgt,
                    "target_index": int(leg.get("target_index") or 0),
                    "status": ("ACTIVE" if has_target else "SL_ONLY"),
                    "target_order_id": str(target_order_id or ""),
                    "stop_order_id": str(stop_order_id or ""),
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
            signal_targets=list(signal_targets or []),
            sl_progress_stage="NONE",
        )
        try:
            entry_basis = float(entry_avg_price or 0.0)
        except (TypeError, ValueError):
            entry_basis = 0.0
        try:
            sl_true = float(trade_data.get("stop_loss") or 0.0)
        except (TypeError, ValueError):
            sl_true = 0.0
        risk_pts = max(entry_basis - sl_true, 0.0)
        lot_size = int(trade_data.get("lot_size") or 0)

        leg_lines = []
        for i, l in enumerate(created_legs):
            tgt_signal = float(l["target_signal"])
            tgt_broker = float(l["target_broker"])
            leg_qty = int(l.get("qty") or 0)
            leg_lots = (float(leg_qty) / float(lot_size)) if lot_size > 0 else 0.0
            lots_text = f"{leg_lots:.2f}".rstrip("0").rstrip(".") if leg_lots > 0 else "n/a"
            if entry_basis > 0 and risk_pts > 0:
                reward_pts = max(tgt_signal - entry_basis, 0.0)
                rr = reward_pts / risk_pts
                risk_inr = risk_pts * float(leg_qty)
                reward_inr = reward_pts * float(leg_qty)
                rr_text = (
                    f" | risk ₹{risk_inr:.2f} ({risk_pts:.2f} pts)"
                    f", reward ₹{reward_inr:.2f} ({reward_pts:.2f} pts)"
                    f", R:R {rr:.2f}"
                )
            else:
                rr_text = " | R:R n/a"
            leg_lines.append(
                f"T{i+1} qty={leg_qty} (lots={lots_text}): signal {tgt_signal} → broker {tgt_broker}{rr_text}"
            )
        out = "Targets:\n" + "\n".join(leg_lines) + f"\nSL: {trade_data['stop_loss']}"
        if oco_warnings:
            out += "\n" + "\n".join([f"Warning: {w}" for w in oco_warnings])
        return out

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
        # Persist the entry immediately even if OCO is not attached (e.g., SL=0).
        # This keeps `active_trades.json` consistent with broker state and enables later
        # management actions (EXIT/TRAIL/UPDATE_TGT) and reconciliation.
        try:
            self._save_active_trade(
                symbol,
                "",
                stop_loss=float(trade_data.get("stop_loss") or 0),
                target=float(trade_data.get("target") or 0),
                entry_side=action,
                entry_qty=int(quantity),
                lot_size=int(trade_data.get("lot_size") or 0),
                entry_avg_price=float(fill.get("avg_price") or ltp or 0.0),
                opened_at_ist=self._now_ist_iso(),
                entry_order_id=str(order_id or ""),
                spread_group_id=str(trade_data.get("spread_group_id") or ""),
                oco_legs=[],
                signal_targets=list(self._signal_targets(trade_data) or []),
                sl_progress_stage="NONE",
            )
        except Exception as e:
            logger.warning(f"[STAGE 4] Failed to persist entry-only active trade for {symbol}: {e}")
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
            sizing_mode = str(trade_data.get("sizing_mode_used") or "").upper() or "FIXED"
            override_reason = str(trade_data.get("size_override_reason") or "").strip()
            sizing_line = (
                f"📦 Sizing: {sizing_mode}"
                + (f" (override: {override_reason})" if override_reason else "")
                + f", qty={quantity}"
            )
            rr_line = self._risk_reward_line(trade_data, qty=quantity, entry_ref=float(ltp or 0.0))
            return f"✅ SUCCESS: {action} {symbol} @ {ltp}.\n{sizing_line}\n{oco_line}\n{rr_line}"
        # More explicit operator message for why OCO wasn't attached.
        reason = "OCO not attached"
        if trade_data.get("spread_leg"):
            reason = "spread leg"
        elif str(trade_data.get("action") or "").upper() != "BUY":
            reason = "non-BUY entry"
        else:
            try:
                sl = float(trade_data.get("stop_loss") or 0.0)
            except Exception:
                sl = 0.0
            try:
                tgt = float(trade_data.get("target") or 0.0)
            except Exception:
                tgt = 0.0
            if sl <= 0 and tgt <= 0:
                reason = "missing/zero SL and target"
            elif sl <= 0:
                reason = "missing/zero SL"
            elif tgt <= 0:
                reason = "missing/zero target"
        return f"✅ SUCCESS: {action} {symbol} @ {ltp}. (No OCO — {reason}.)"

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
            # Persist the filled LIMIT entry immediately even if OCO is not attached.
            try:
                self._save_active_trade(
                    symbol,
                    "",
                    stop_loss=float(trade_data.get("stop_loss") or 0),
                    target=float(trade_data.get("target") or 0),
                    entry_side=action,
                    entry_qty=int(fill.get("filled_qty") or trade_data.get("quantity") or 0),
                    lot_size=int(trade_data.get("lot_size") or 0),
                    entry_avg_price=float(fill.get("avg_price") or ltp or 0.0),
                    opened_at_ist=self._now_ist_iso(),
                    entry_order_id=str(entry_order_id or ""),
                    spread_group_id=str(trade_data.get("spread_group_id") or ""),
                    oco_legs=[],
                    signal_targets=list(self._signal_targets(trade_data) or []),
                    sl_progress_stage="NONE",
                )
            except Exception as e:
                logger.warning(f"[STAGE 4] Failed to persist limit entry-only active trade for {symbol}: {e}")
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
        sizing_line = self._sizing_line(trade_data, qty=int(qty))
        logger.info(f"[STAGE 4] {trading_symbol} limit sizing -> {sizing_line}")
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
                    + self._queue_single(
                        trade_data,
                        exchange,
                        trading_symbol,
                        segment,
                        observed_ltp=observed_ltp,
                    )
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
            f"Polling order status every {Config.ENTRY_QUEUE_POLL_SEC}s; max {Config.ENTRY_QUEUE_MAX_WAIT_MIN} min.\n"
            f"{sizing_line}"
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
        qty = int(trade_data.get("quantity") or 0)
        if qty <= 0:
            qty = int(self._compute_order_quantity(trade_data))
            if qty > 0:
                trade_data["quantity"] = qty
        sizing_line = self._sizing_line(trade_data, qty=qty) if qty > 0 else "📦 Sizing: n/a"
        logger.info(f"[STAGE 4] {trading_symbol} queued sizing -> {sizing_line}")
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
            f"Will retry every {Config.ENTRY_QUEUE_POLL_SEC}s for up to {Config.ENTRY_QUEUE_MAX_WAIT_MIN} min.\n"
            f"{sizing_line}"
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
        with self._instrument_cache_lock:
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
            lot_size = int(trade_data.get("lot_size") or 0)
            lots = (float(qty) / float(lot_size)) if lot_size > 0 else 0.0
            lots_text = f"{lots:.2f}".rstrip("0").rstrip(".") if lots > 0 else "n/a"
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
                f"📐 Risk/Reward: qty={int(qty or 0)} (lots={lots_text}), "
                f"risk ₹{risk_inr:.2f} ({risk_pts:.2f} pts), "
                f"reward ₹{reward_inr:.2f} ({reward_pts:.2f} pts), R:R {rr:.2f}"
            )
        except Exception:
            return "📐 Risk/Reward: n/a"

    @staticmethod
    def _sizing_line(trade_data: dict, *, qty: int) -> str:
        """Human-readable sizing summary for logs and operator alerts."""
        mode = str(trade_data.get("sizing_mode_used") or "").upper() or "FIXED"
        override_reason = str(trade_data.get("size_override_reason") or "").strip()
        lot_size = int(trade_data.get("lot_size") or 0)
        lots_text = "n/a"
        if lot_size > 0 and qty > 0:
            lots = float(qty) / float(lot_size)
            lots_text = f"{lots:.2f}".rstrip("0").rstrip(".")
        return (
            f"📦 Sizing: {mode}"
            + (f" (override: {override_reason})" if override_reason else "")
            + f", qty={int(qty or 0)}, lot_size={lot_size if lot_size > 0 else 'n/a'}, lots={lots_text}"
        )

    @classmethod
    def _chase_rr_target(cls, trade_data: dict) -> tuple[float, int]:
        """Return (target_price, target_index_used) for chase R:R math."""
        try:
            idx = int(getattr(Config, "CHASE_RR_TARGET_INDEX", 1) or 1)
        except Exception:
            idx = 1
        idx = max(1, min(3, idx))

        targets = cls._signal_targets(trade_data)
        if targets and len(targets) >= idx:
            chosen = float(targets[idx - 1] or 0.0)
            if chosen > 0:
                return chosen, idx

        # Fallback to first available target.
        if targets:
            first = float(targets[0] or 0.0)
            if first > 0:
                return first, 1

        try:
            t = float(trade_data.get("target") or 0.0)
        except Exception:
            t = 0.0
        return (t if t > 0 else 0.0), 1

    @classmethod
    def _rr_at_entry(cls, trade_data: dict, entry_price: float) -> float:
        try:
            sl = float(trade_data.get("stop_loss") or 0.0)
            tgt, _ = cls._chase_rr_target(trade_data)
            entry = float(entry_price or 0.0)
            if entry <= 0 or sl <= 0 or tgt <= 0 or entry <= sl:
                return 0.0
            risk = entry - sl
            reward = tgt - entry
            if risk <= 0:
                return 0.0
            return max(0.0, reward / risk)
        except Exception:
            return 0.0

    def _entry_price_for_rr(self, trade_data: dict, min_rr: float) -> float | None:
        """Solve entry price where reward:risk equals min_rr."""
        try:
            rr = max(0.0, float(min_rr))
            sl = float(trade_data.get("stop_loss") or 0.0)
            tgt, _ = self._chase_rr_target(trade_data)
            if sl <= 0 or tgt <= 0:
                return None
            # (tgt - e) / (e - sl) = rr  =>  e = (tgt + rr*sl) / (1+rr)
            e = (tgt + (rr * sl)) / (1.0 + rr)
            if e <= 0:
                return None
            return float(self._round_price_to_tick(e, mode="down"))
        except Exception:
            return None

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
            rr_tgt, rr_tgt_idx = self._chase_rr_target(trade_data)
            rr_now = self._rr_at_entry(trade_data, float(ltp))
            min_rr = float(getattr(Config, "CHASE_MIN_RR", 1.0) or 0.0)
            logger.info(
                f"[STAGE 4] LTP {ltp} > max {high} for BUY. "
                f"R:R(now)={rr_now:.2f} min_rr={min_rr:.2f} "
                f"chase_target=T{rr_tgt_idx} ({rr_tgt:.2f})"
            )
            if min_rr > 0 and rr_now >= min_rr:
                logger.info(
                    f"[STAGE 4] BUY chase allowed for {found_symbol}: "
                    f"LTP {ltp} keeps R:R {rr_now:.2f} >= {min_rr:.2f} "
                    f"using T{rr_tgt_idx} ({rr_tgt:.2f})."
                )
                return self._finalize_option_order(trade_data, exchange, found_symbol, segment, ltp=float(ltp))

            chase_limit = float(self._round_price_to_tick(float(high), mode="down")) if high else 0.0
            if chase_limit > 0:
                trade_data["limit_price_override"] = chase_limit
                logger.info(
                    f"[STAGE 4] BUY chase blocked for {found_symbol}: R:R {rr_now:.2f} < {min_rr:.2f}. "
                    f"Using T{rr_tgt_idx} ({rr_tgt:.2f}) and range-max limit {chase_limit}."
                )
            else:
                trade_data.pop("limit_price_override", None)
                logger.info(
                    f"[STAGE 4] BUY chase blocked for {found_symbol}: R:R {rr_now:.2f} < {min_rr:.2f}. "
                    "Range-max limit unavailable; using default limit logic."
                )
            if (
                Config.ENTRY_SINGLE_LIMIT_ORDER
                and not trade_data.get("spread_leg")
                and self.place_entry_orders
            ):
                base_msg = self._place_limit_and_queue(
                    trade_data, exchange, found_symbol, segment, observed_ltp=float(ltp)
                )
                if chase_limit > 0:
                    return (
                        f"{base_msg}\n"
                        f"📐 Chase guard: current R:R={rr_now:.2f} (< {min_rr:.2f}); "
                        f"target=T{rr_tgt_idx} ({rr_tgt:.2f}); placed range-max limit at {chase_limit}."
                    )
                return base_msg
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
                    lock = self._get_exit_lock(symbol)
                    if lock.get("active") and lock.get("stale"):
                        logger.warning(
                            f"[STAGE 4] EXIT_ALL stale lock cleared for {symbol}: "
                            f"lock_id={lock.get('lock_id')} age={lock.get('age_seconds'):.1f}s"
                        )
                        self._clear_exit_lock(symbol)
                        lock = self._get_exit_lock(symbol)

                    if lock.get("active") and not from_queue:
                        return (
                            f"⏳ EXIT_ALL already in progress for {symbol} "
                            f"(lock={lock.get('lock_id')}, age={lock.get('age_seconds'):.0f}s). "
                            f"Skipping duplicate signal.{self._ltp_suffix_for_symbol(symbol)}"
                        )

                    if not lock.get("active"):
                        src = "queue" if from_queue else "signal"
                        new_lock_id = str(uuid.uuid4())[:12]
                        self._set_exit_lock(symbol, lock_id=new_lock_id, reason=src)
                        logger.info(
                            f"[STAGE 4] EXIT_ALL lock armed for {symbol}: id={new_lock_id} source={src}"
                        )

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
                    exit_fallback_price = self._num_or((pos or {}).get("ltp"), 0.0)
                    if exit_fallback_price <= 0:
                        exit_fallback_price = self._fetch_ltp(ex, symbol, seg)
                    fill = self._fetch_order_fill(
                        segment=seg,
                        groww_order_id=exit_order_id,
                        fallback_price=float(exit_fallback_price or 0.0),
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
                    trail_sl_cache: dict[str, dict] = {}
                    attempted = 0
                    success = 0
                    skipped_stale = 0
                    failed: list[str] = []
                    last_broker_sl = float(broker_sl)
                    for oid in order_ids:
                        attempted += 1
                        try:
                            last_broker_sl = self._modify_oco_sl(
                                symbol, oid, last_broker_sl, payload_cache=trail_sl_cache
                            )
                            success += 1
                        except Exception as e:
                            err = str(e)
                            if self._is_stale_oco_error(err):
                                skipped_stale += 1
                                logger.warning(
                                    f"[STAGE 4] TRAIL_SL skipped stale OCO leg for {symbol} id={oid}: {err}"
                                )
                                continue
                            failed.append(f"{oid}: {err}")
                            logger.warning(
                                f"[STAGE 4] TRAIL_SL leg modify failed for {symbol} id={oid}: {err}"
                            )

                    if attempted > 0 and success == 0 and skipped_stale > 0 and not failed:
                        recovered = self._retry_management_after_stale_oco(
                            symbol,
                            trade_data,
                            rec0,
                            reason="stale OCO detected during TRAIL_SL",
                        )
                        if recovered:
                            return recovered

                    if attempted > 0 and success == 0 and failed:
                        return (
                            f"⚠️ TRAIL_SL partial: broker SL update failed for {symbol} "
                            f"(attempted={attempted}, stale={skipped_stale}, failed={len(failed)}). "
                            f"Last error: {failed[-1]}{self._ltp_suffix_for_symbol(symbol)}"
                        )

                    if attempted > 0 and success == 0:
                        return (
                            f"⚠️ TRAIL_SL skipped for {symbol}: no modifiable OCO legs found "
                            f"(attempted={attempted}, stale={skipped_stale}, failed={len(failed)})."
                            f"{self._ltp_suffix_for_symbol(symbol)}"
                        )

                    broker_sl = float(last_broker_sl)
                    self._update_active_trade_levels(
                        symbol,
                        stop_loss=broker_sl,
                        sl_true=float(new_sl),
                        sl_widened=(broker_sl if bool(rec0.get("vol_sl_needs_restore")) else 0.0),
                        sl_mode=("WIDENED" if bool(rec0.get("vol_sl_needs_restore")) else "NORMAL"),
                        sl_widen_pct=float(rec0.get("sl_widen_pct") or Config.VOL_SL_WIDEN_PCT),
                    )
                    logger.info(
                        f"[STAGE 4] TRAIL SUCCESS: {symbol} sl_true={new_sl} broker_sl={broker_sl} "
                        f"(attempted={attempted}, success={success}, stale={skipped_stale}, failed={len(failed)})."
                    )
                    return (
                        f"📈 TRAIL SUCCESS: Updated {symbol} SL — sl_true={new_sl}, broker_sl={broker_sl} "
                        f"(updated={success}/{attempted}, stale={skipped_stale}, failed={len(failed)})."
                        f"{self._ltp_suffix_for_symbol(symbol)}"
                    )

                elif action == "RESTORE_SL":
                    return self.restore_channel_sl(symbol, source="signal")

                elif action == "UPDATE_TGT":
                    raw_tgt = float(trade_data["target"])
                    eff_tgt = self._take_profit_order_price(raw_tgt)
                    target_leg_index = int(self._num_or(trade_data.get("target_leg_index"), 0))
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
                    oco_tgt_modify_cache: dict[str, dict] = {}
                    legs = rec_live.get("oco_legs")
                    attempted = 0
                    success = 0
                    skipped_stale = 0
                    failed: list[str] = []
                    if isinstance(legs, list) and legs:
                        for i, leg in enumerate(legs):
                            if not isinstance(leg, dict):
                                continue
                            oid = str(leg.get("smart_order_id") or "")
                            if not oid:
                                continue
                            leg_target_index = int(self._num_or(leg.get("target_index"), i + 1))
                            if target_leg_index > 0 and leg_target_index != target_leg_index:
                                continue
                            attempted += 1
                            if target_leg_index > 0:
                                leg_target_signal = float(raw_tgt)
                            else:
                                # Global target update: unify all active legs to the new channel target.
                                leg_target_signal = float(raw_tgt)
                            leg_target_broker = self._take_profit_order_price(leg_target_signal)
                            if leg_target_broker <= 0:
                                leg_target_broker = eff_tgt
                            try:
                                try:
                                    self._modify_oco_target(
                                        oid,
                                        leg_target_broker,
                                        symbol=symbol,
                                        payload_cache=oco_tgt_modify_cache,
                                    )
                                except Exception as e1:
                                    em1 = str(e1)
                                    if self._is_transient_update_tgt_error(em1):
                                        logger.warning(
                                            f"[STAGE 4] UPDATE_TGT transient error for {symbol} id={oid}; retrying once: {em1}"
                                        )
                                        time.sleep(0.8)
                                        self._modify_oco_target(
                                            oid,
                                            leg_target_broker,
                                            symbol=symbol,
                                            payload_cache=oco_tgt_modify_cache,
                                        )
                                    else:
                                        raise
                                leg["target_broker"] = float(leg_target_broker)
                                leg["target_signal"] = float(leg_target_signal)
                                success += 1
                            except Exception as e:
                                em = str(e)
                                if self._is_stale_oco_error(em):
                                    skipped_stale += 1
                                    logger.warning(
                                        f"[STAGE 4] UPDATE_TGT skipped stale OCO leg for {symbol} id={oid}: {em}"
                                    )
                                    continue
                                failed.append(f"{oid}: {em}")
                                logger.warning(
                                    f"[STAGE 4] UPDATE_TGT leg modify failed for {symbol} id={oid}: {em}"
                                )
                        if success > 0:
                            self._save_active_trade(
                                symbol,
                                order_id,
                                target=raw_tgt,
                                oco_legs=legs,
                            )
                    else:
                        for oid in order_ids:
                            attempted += 1
                            try:
                                try:
                                    self._modify_oco_target(
                                        oid, eff_tgt, symbol=symbol, payload_cache=oco_tgt_modify_cache
                                    )
                                except Exception as e1:
                                    em1 = str(e1)
                                    if self._is_transient_update_tgt_error(em1):
                                        logger.warning(
                                            f"[STAGE 4] UPDATE_TGT transient error for {symbol} id={oid}; retrying once: {em1}"
                                        )
                                        time.sleep(0.8)
                                        self._modify_oco_target(
                                            oid, eff_tgt, symbol=symbol, payload_cache=oco_tgt_modify_cache
                                        )
                                    else:
                                        raise
                                success += 1
                            except Exception as e:
                                em = str(e)
                                if self._is_stale_oco_error(em):
                                    skipped_stale += 1
                                    logger.warning(
                                        f"[STAGE 4] UPDATE_TGT skipped stale OCO order for {symbol} id={oid}: {em}"
                                    )
                                    continue
                                failed.append(f"{oid}: {em}")
                                logger.warning(
                                    f"[STAGE 4] UPDATE_TGT order modify failed for {symbol} id={oid}: {em}"
                                )
                    if attempted > 0 and success == 0 and skipped_stale > 0 and not failed:
                        recovered = self._retry_management_after_stale_oco(
                            symbol,
                            trade_data,
                            rec_live,
                            reason="stale OCO detected during UPDATE_TGT",
                        )
                        if recovered:
                            return recovered
                    if attempted > 0 and success == 0 and failed:
                        return (
                            f"⚠️ UPDATE_TGT partial: broker target update failed for {symbol} "
                            f"(attempted={attempted}, stale={skipped_stale}, failed={len(failed)}). "
                            f"Last error: {failed[-1]}{self._ltp_suffix_for_symbol(symbol)}"
                        )
                    if isinstance(legs, list) and legs and success > 0:
                        self._save_active_trade(
                            symbol,
                            order_id,
                            target=raw_tgt,
                            oco_legs=legs,
                        )
                    if target_leg_index <= 0 and success > 0:
                        self._update_active_trade_levels(symbol, target=raw_tgt)
                    logger.info(
                        f"[STAGE 4] UPDATE_TGT SUCCESS: {symbol} broker target {eff_tgt} "
                        f"(signal {raw_tgt}, target_leg_index={target_leg_index or 'ALL'}, "
                        f"attempted={attempted}, success={success}, stale={skipped_stale}, failed={len(failed)})."
                    )
                    return (
                        f"🎯 UPDATE_TGT SUCCESS: {symbol} target {eff_tgt} (signal {raw_tgt}; "
                        f"target_leg={'ALL' if target_leg_index <= 0 else target_leg_index}; "
                        f"updated={success}/{attempted}, stale={skipped_stale}, failed={len(failed)})."
                        f"{self._ltp_suffix_for_symbol(symbol)}"
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
