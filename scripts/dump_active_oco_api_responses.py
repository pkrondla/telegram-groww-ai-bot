import argparse
import json
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.db import Database
from src.trader import Trader

DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "trading_bot.db")


def load_active_trades(db_path: str) -> dict:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"SQLite database not found: {db_path}")
    store = Database(db_path)
    active = store.get_active_trades()
    if not isinstance(active, dict):
        raise ValueError("active_trades table must deserialize to a dict")
    return active


def extract_order_id(record: dict) -> str:
    if not isinstance(record, dict):
        return ""
    oid = str(record.get("smart_order_id") or "").strip()
    if oid:
        return oid
    legs = record.get("oco_legs")
    if isinstance(legs, list):
        for leg in legs:
            if not isinstance(leg, dict):
                continue
            sid = str(leg.get("smart_order_id") or "").strip()
            if sid:
                return sid
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Dump raw Groww get_smart_order(OCO) responses for each active trade "
        "(symbols + smart_order_ids from trading_bot.db)."
    )
    ap.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite DB (default: {DEFAULT_DB_PATH})",
    )
    args = ap.parse_args()
    db_path = os.path.abspath(args.db)

    try:
        active = load_active_trades(db_path)
    except Exception as e:
        print(f"ERROR: {e}")
        return 1

    trader = Trader(dry_run=True)
    if trader.client is None:
        print("ERROR: Groww client not initialized. Check GROWW_API_KEY / GROWW_API_SECRET.")
        return 2

    print("Ready to Groww!")
    print(f"Loaded {len(active)} active trade(s) from {db_path}")

    for symbol, rec in active.items():
        oid = extract_order_id(rec)
        print("\n" + "=" * 100)
        print(f"SYMBOL: {symbol}")
        print(f"SMART_ORDER_ID: {oid or '<missing>'}")
        if not oid:
            print("RAW_API_RESPONSE: <skipped, no smart_order_id>")
            continue
        try:
            response = trader.client.get_smart_order(
                segment=trader.client.SEGMENT_FNO,
                smart_order_type=trader.client.SMART_ORDER_TYPE_OCO,
                smart_order_id=oid,
            )
            print("RAW_API_RESPONSE:")
            print(json.dumps(response, indent=2, ensure_ascii=False))
        except Exception as e:
            print("RAW_API_RESPONSE_ERROR:")
            print(str(e))

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
