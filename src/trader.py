import json
import logging
import uuid
import os
import re
import csv
import copy
import time
import datetime
from .config import Config

logger = logging.getLogger(__name__)

class Trader:
    """
    Handles order execution, instrument lookup, and local state management for Groww.

    When dry_run is True, only *entry* placement is skipped (MARKET/LIMIT BUY or SELL that
    opens a position, plus OCO attach after entry, and broker LIMIT rows for that path).
    LTP/quotes, instrument resolution, and pending-queue LTP polls still call Groww when
    the key is set.

    EXIT_ALL / TRAIL_SL / UPDATE_TGT: if active_trades.json holds a real broker smart-order id,
    those APIs run against Groww. If the id is DRY_RUN_PLACEHOLDER_ORDER_ID (entry was
    simulated), management is local-only — no broker call, since there is no position/OCO.
    """
    
    # Stored in active_trades.json when a BUY was validated in dry run but not sent to Groww.
    DRY_RUN_PLACEHOLDER_ORDER_ID = "DRY_RUN_NO_ORDER"
    SMART_SEGMENT = "FNO"
    
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.active_trades_file = "active_trades.json"
        self.pending_trades_file = "pending_trades.json"
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
        self._save_active_trade(symbol, new_id, stop_loss=raw_sl, target=raw_tgt)
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
            return out
        return {"smart_order_id": str(record or "")}

    def _get_active_trade_record(self, symbol: str) -> dict | None:
        rec = self._load_active_trades().get(symbol)
        if rec is None:
            return None
        return self._normalize_active_trade_record(rec)

    def _save_active_trade(self, symbol, order_id, *, stop_loss: float | None = None, target: float | None = None):
        trades = self._load_active_trades()
        current = self._normalize_active_trade_record(trades.get(symbol))
        oid = str(order_id or "")
        current["smart_order_id"] = oid
        if stop_loss is not None:
            current["stop_loss"] = float(stop_loss)
        if target is not None:
            current["target"] = float(target)
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

    def _update_active_trade_levels(self, symbol: str, *, stop_loss: float | None = None, target: float | None = None):
        trades = self._load_active_trades()
        if symbol not in trades:
            return
        rec = self._normalize_active_trade_record(trades[symbol])
        if stop_loss is not None:
            rec["stop_loss"] = float(stop_loss)
        if target is not None:
            rec["target"] = float(target)
        trades[symbol] = rec
        self._save_all_trades(trades)

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

    def _compute_order_quantity(self, trade_data: dict, instrument: dict | None = None) -> int:
        """
        quantity = lot_size * ORDER_LOTS_COUNT.
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
            lots = max(1, int(Config.ORDER_LOTS_COUNT))
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
        self, trade_data: dict, exchange: str, symbol: str, segment: str, product_type: str = "NRML"
    ) -> str | None:
        if not self._should_attach_oco(trade_data):
            return None
        raw_tgt = float(trade_data["target"])
        eff_tgt = self._take_profit_order_price(raw_tgt)
        logger.info(
            f"OCO target: signal {raw_tgt} → broker {eff_tgt} (TARGET_BUFFER_POINTS={Config.TARGET_BUFFER_POINTS})"
        )
        oco_ref = str(uuid.uuid4())[:15]
        quantity = self._compute_order_quantity(trade_data)
        oco_resp = self.client.create_smart_order(
            smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
            reference_id=oco_ref,
            segment=segment,
            trading_symbol=symbol,
            quantity=quantity,
            net_position_quantity=quantity,
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
                "trigger_price": float(trade_data["stop_loss"]),
                "price": float(trade_data["stop_loss"]),
                "order_type": "SL",
            },
        )
        smart_order_id = self._extract_smart_order_id(oco_resp, fallback=oco_ref)
        self._save_active_trade(
            symbol,
            smart_order_id,
            stop_loss=float(trade_data["stop_loss"]),
            target=float(raw_tgt),
        )
        return (
            f"Targets: signal {raw_tgt} → broker {eff_tgt} | SL: {trade_data['stop_loss']}"
        )

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
                )
            low, high = trade_data.get("entry_range", [0, 0])
            return f"🧪 [DRY RUN] Validated {action} {symbol} @ {ltp} (range {low}-{high}). No entry order placed."

        product_type = "NRML"
        quantity = self._compute_order_quantity(trade_data)
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

        oco_line = self._attach_oco_after_buy(trade_data, exchange, symbol, segment, product_type)
        if oco_line:
            return f"✅ SUCCESS: {action} {symbol} @ {ltp}.\n{oco_line}"
        return f"✅ SUCCESS: {action} {symbol} @ {ltp}. (No OCO — spread leg or missing SL/target.)"

    def _after_limit_order_filled(self, trade_data: dict, exchange: str, symbol: str, segment: str) -> str:
        """Entry already filled via LIMIT; attach OCO for BUY if configured."""
        action = (trade_data.get("action") or "BUY").upper()
        ltp = self._fetch_ltp(exchange, symbol, segment)
        if not self.place_entry_orders:
            return f"🧪 [DRY RUN] Limit would have filled for {symbol} @ ~{ltp}. (No OCO / entry writes.)"

        if action == "BUY":
            oco_line = self._attach_oco_after_buy(trade_data, exchange, symbol, segment)
            if oco_line:
                return f"✅ LIMIT BUY filled {symbol} @ ~{ltp}. {oco_line}"
            return f"✅ LIMIT BUY filled {symbol} @ ~{ltp}. (No OCO.)"
        return f"✅ LIMIT {action} filled {symbol} @ ~{ltp}."

    def _place_limit_and_queue(self, trade_data: dict, exchange: str, trading_symbol: str, segment: str) -> str:
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

        entry_response = None
        try:
            entry_response = self.client.place_order(
                validity="DAY",
                exchange=exchange,
                order_type="LIMIT",
                product="NRML",
                quantity=self._compute_order_quantity(trade_data),
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
        return (
            f"📌 LIMIT {action} placed {trading_symbol} @ {lim} (range {low}-{high}). "
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
                        msg = self._after_limit_order_filled(td, ex, sym, seg)
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

    def _queue_single(self, trade_data: dict, exchange: str, trading_symbol: str, segment: str) -> str:
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
        return (
            f"⏳ Queued {action} {trading_symbol}: LTP not in range {low}-{high} yet. "
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
        """Broker OCO target = signal target minus TARGET_BUFFER_POINTS (e.g. 100 − 0.5 → 99.5)."""
        try:
            raw = float(raw_signal_target)
        except (TypeError, ValueError):
            return 0.0
        if raw <= 0:
            return 0.0
        buf = max(0.0, float(Config.TARGET_BUFFER_POINTS))
        return max(raw - buf, 0.0)

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
                return self._place_limit_and_queue(trade_data, exchange, found_symbol, segment)
            if Config.ENTRY_QUEUE_ENABLED:
                return self._queue_single(trade_data, exchange, found_symbol, segment)
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
                return self._place_limit_and_queue(trade_data, exchange, found_symbol, segment)
            if Config.ENTRY_QUEUE_ENABLED:
                return self._queue_single(trade_data, exchange, found_symbol, segment)
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
                return self._place_limit_and_queue(trade_data, exchange, found_symbol, segment)
            if Config.ENTRY_QUEUE_ENABLED:
                return self._queue_single(trade_data, exchange, found_symbol, segment)
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
            if action in ["EXIT_ALL", "TRAIL_SL", "UPDATE_TGT"]:
                order_id = self._get_order_id(symbol)
                logger.info(f"[STAGE 4] Order ID for {symbol}: {order_id}")
                if not order_id:
                    logger.warning(f"[STAGE 4] No order ID found for {symbol} in active_trades.json.")
                    return f"⚠️ NO ACTIVE TRADE: Could not find an open Order ID for {symbol}."

                # Simulated book row from dry-run entry — nothing exists on the broker to exit/modify.
                if order_id == self.DRY_RUN_PLACEHOLDER_ORDER_ID:
                    if action == "EXIT_ALL":
                        self._remove_active_trade(symbol)
                        logger.info(f"[STAGE 4] DRY RUN book: EXIT_ALL {symbol} — cleared local book only.")
                        return (
                            f"🧪 [DRY RUN] EXIT_ALL for {symbol}: removed from local active book "
                            f"(no broker order was placed)."
                        )
                    if action == "TRAIL_SL":
                        new_sl = float(trade_data["stop_loss"])
                        self._update_active_trade_levels(symbol, stop_loss=new_sl)
                        logger.info(f"[STAGE 4] DRY RUN book: TRAIL_SL {symbol} → {new_sl} (not sent to broker).")
                        return (
                            f"🧪 [DRY RUN] TRAIL_SL for {symbol} → {new_sl} "
                            f"(simulated; no OCO on broker for this symbol)."
                        )
                    raw_tgt = float(trade_data.get("target") or 0)
                    eff_tgt = self._take_profit_order_price(raw_tgt)
                    self._update_active_trade_levels(symbol, target=raw_tgt)
                    logger.info(
                        f"[STAGE 4] DRY RUN book: UPDATE_TGT {symbol} signal {raw_tgt} → broker {eff_tgt} "
                        f"(not sent to broker)."
                    )
                    return (
                        f"🧪 [DRY RUN] UPDATE_TGT for {symbol}: signal {raw_tgt} → would place {eff_tgt} "
                        f"(simulated; no OCO on broker for this symbol)."
                    )

                if not self.client:
                    return f"⚠️ NO GROWW CLIENT: Cannot {action} for {symbol}. Configure GROWW_API_KEY."

                if action == "EXIT_ALL":
                    # 1) Cancel OCO protection (best effort).
                    try:
                        logger.info(
                            f"[STAGE 4] Calling Groww cancel_smart_order for {symbol} "
                            f"(segment={self.SMART_SEGMENT}, smart_order_type=OCO, id={order_id})."
                        )
                        self.client.cancel_smart_order(
                            segment=self.SMART_SEGMENT,
                            smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                            smart_order_id=order_id,
                        )
                    except Exception as e:
                        logger.warning(f"[STAGE 4] EXIT_ALL cancel_smart_order warning for {symbol}: {e}")

                    # 2) Flatten any remaining net position with MARKET order.
                    pos, qty = self._get_open_position_for_symbol(symbol)
                    if qty == 0:
                        self._remove_active_trade(symbol)
                        logger.info(
                            f"[STAGE 4] EXIT SUCCESS: {symbol} has no open position after cancel; removed from active book."
                        )
                        return f"🚨 EXIT SUCCESS: {symbol} already flat; cleared local active trade."

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
                    self.client.place_order(
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
                            f"(remaining qty {qty_after})."
                        )

                    self._remove_active_trade(symbol)
                    logger.info(
                        f"[STAGE 4] EXIT SUCCESS: {symbol} flattened and removed from active book."
                    )
                    return f"🚨 EXIT SUCCESS: Closed {symbol} position and cleared active trade."
                
                elif action == "TRAIL_SL":
                    new_sl = float(trade_data["stop_loss"])
                    logger.info(f"[STAGE 4] Calling Groww modify_smart_order for {symbol}: new SL={new_sl}.")
                    self.client.modify_smart_order(
                        smart_order_id=order_id,
                        smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                        segment=self.SMART_SEGMENT,
                        stop_loss={
                            "trigger_price": new_sl,
                            "price": new_sl,
                            "order_type": "SL",
                        },
                    )
                    self._update_active_trade_levels(symbol, stop_loss=new_sl)
                    logger.info(f"[STAGE 4] TRAIL SUCCESS: {symbol} SL updated to {new_sl}.")
                    return f"📈 TRAIL SUCCESS: Updated {symbol} SL to {new_sl}."

                elif action == "UPDATE_TGT":
                    raw_tgt = float(trade_data["target"])
                    eff_tgt = self._take_profit_order_price(raw_tgt)
                    if eff_tgt <= 0:
                        return (
                            f"⚠️ UPDATE_TGT skipped for {symbol}: buffered target must be > 0 "
                            f"(signal {raw_tgt}, TARGET_BUFFER_POINTS={Config.TARGET_BUFFER_POINTS})."
                        )
                    logger.info(
                        f"[STAGE 4] Calling Groww modify_smart_order for {symbol}: "
                        f"target signal {raw_tgt} → broker {eff_tgt}."
                    )
                    self.client.modify_smart_order(
                        smart_order_id=order_id,
                        smart_order_type=self.client.SMART_ORDER_TYPE_OCO,
                        segment=self.SMART_SEGMENT,
                        target={
                            "trigger_price": eff_tgt,
                            "price": eff_tgt,
                            "order_type": "LIMIT",
                        },
                    )
                    self._update_active_trade_levels(symbol, target=raw_tgt)
                    logger.info(
                        f"[STAGE 4] UPDATE_TGT SUCCESS: {symbol} broker target {eff_tgt} (signal {raw_tgt})."
                    )
                    return (
                        f"🎯 UPDATE_TGT SUCCESS: {symbol} target {eff_tgt} (signal {raw_tgt})."
                    )

            # BUY / SELL option entry (single leg)
            logger.info(f"[STAGE 4] Routing to option entry executor for {action} {symbol}.")
            return self._execute_option_entry(trade_data)

        except Exception as e:
            logger.error(f"[STAGE 4] Execution Error for {symbol}: {str(e)}", exc_info=True)
            return f"❌ ERROR for {symbol}: {str(e)}"
