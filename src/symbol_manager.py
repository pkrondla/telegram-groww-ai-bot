from __future__ import annotations

import csv
import json
import os
import re
import datetime
from .logger import logger


_CACHE_FILE = "symbol_map.json"
_OVERRIDES_FILE = "symbol_overrides.json"
_DEFAULT_MAX_AGE_DAYS = 7


class SymbolManager:
    """
    Manages the mapping between channel-style symbol names and exact Groww
    FNO underlying_symbol values.

    Lifecycle:
      1. refresh() called on bot startup.
      2. Tries Groww API / packaged instruments.csv to get all FNO underlyings.
      3. Auto-generates aliases from symbol names mathematically.
      4. Merges with symbol_overrides.json (manual non-obvious aliases).
      5. Saves result to symbol_map.json (local cache).
      6. resolve() and hint_for_message() used at parse time.
    """

    def __init__(self, max_age_days: int = _DEFAULT_MAX_AGE_DAYS):
        self._max_age_days = max_age_days
        self._symbols: list[str] = []          # exact Groww underlying_symbol values
        self._aliases: dict[str, str] = {}     # UPPER_KEY → groww_symbol
        self._symbol_keys: set[str] = set()    # normalised keys of _symbols for O(1) lookup
        self._loaded = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def refresh(self, trader=None) -> None:
        """
        Called once on bot startup.
        Downloads fresh data if cache is stale; otherwise loads from cache.
        """
        if self._cache_is_fresh():
            logger.info("SymbolManager: cache is fresh, loading from symbol_map.json")
            self._load_cache()
            return

        logger.info("SymbolManager: refreshing symbol list from Groww instruments...")
        symbols = self._download_symbols(trader)

        if not symbols:
            logger.warning("SymbolManager: download failed, falling back to existing cache.")
            if not self._load_cache():
                logger.warning("SymbolManager: no cache available, using overrides only.")
                self._load_overrides_only()
            return

        self._symbols = sorted(symbols)
        self._aliases = self._build_aliases(self._symbols)
        self._symbol_keys = {self._normalise_key(s) for s in self._symbols}
        self._save_cache()
        logger.info(
            f"SymbolManager: loaded {len(self._symbols)} FNO underlyings, "
            f"{len(self._aliases)} aliases."
        )
        self._loaded = True

    def resolve(self, raw: str) -> str:
        """
        Translate a channel-style symbol name to the exact Groww underlying_symbol.
        Returns the original string unchanged if no mapping found.
        """
        key = self._normalise_key(raw)
        resolved = self._aliases.get(key, raw)
        if resolved != raw:
            logger.debug(f"SymbolManager: {raw} → {resolved}")
        return resolved

    def hint_for_message(self, text: str) -> str:
        """
        Scan message text for any token that matches the alias map or symbol list.
        Returns a compact LLM hint for only the matched symbols.

        Catches all forms: #WAAREE, WAAREE, Waaree, waaree, TATASTL, Zomato, etc.
        """
        matches: dict[str, str] = {}  # original_token → groww_symbol

        # Tokenise: split on whitespace and common separators, strip punctuation
        raw_tokens = re.split(r"[\s,|/\-\(\)\[\]]+", text)
        for tok in raw_tokens:
            # Strip leading # and surrounding punctuation
            clean = re.sub(r"[^A-Za-z0-9&_\-]", "", tok)
            if len(clean) < 2:
                continue
            key = self._normalise_key(clean)
            if not key:
                continue
            # Check alias map first, then direct symbol list
            if key in self._aliases:
                groww_sym = self._aliases[key]
                display = clean.lstrip("#")
                if display.upper() != groww_sym:
                    matches[display] = groww_sym
                else:
                    matches[display] = groww_sym  # exact, still hint
            elif key in self._symbol_keys:
                matches[clean.lstrip("#")] = key

        if not matches:
            return ""

        lines = []
        seen_groww: set[str] = set()
        for display, groww_sym in matches.items():
            if groww_sym in seen_groww:
                continue
            seen_groww.add(groww_sym)
            if display.upper() == groww_sym:
                lines.append(f'  "{display}" → exact Groww symbol: {groww_sym}')
            else:
                lines.append(f'  "{display}" in this message → Groww symbol: {groww_sym}')

        if not lines:
            return ""

        return (
            "\nSYMBOL RESOLUTION HINT (use these exact values for official_symbol):\n"
            + "\n".join(lines)
            + "\n"
        )

    @property
    def symbols(self) -> list[str]:
        return list(self._symbols)

    @property
    def aliases(self) -> dict[str, str]:
        return dict(self._aliases)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _download_symbols(self, trader=None) -> list[str]:
        """
        Try Groww API first (via trader client), then fall back to packaged CSV.
        Returns list of unique underlying_symbol strings for FNO segment.
        """
        # Try via trader's Groww client
        if trader is not None and getattr(trader, "client", None) is not None:
            try:
                instruments = trader.client.get_all_instruments()
                if instruments is not None:
                    return self._extract_from_dataframe(instruments)
            except Exception as e:
                logger.debug(f"SymbolManager: Groww API instruments failed: {e}")

        # Fall back to packaged instruments.csv
        return self._load_from_csv()

    def _extract_from_dataframe(self, df) -> list[str]:
        """Extract FNO underlying symbols from a pandas DataFrame."""
        try:
            import pandas as pd
            if not isinstance(df, pd.DataFrame):
                return []
            fno = df[df["segment"].str.upper() == "FNO"] if "segment" in df.columns else df
            col = next(
                (c for c in ("underlying_symbol", "underlyingSymbol", "underlying") if c in fno.columns),
                None,
            )
            if not col:
                return []
            syms = fno[col].dropna().unique().tolist()
            return [
                str(s).strip()
                for s in syms
                if str(s).strip() and not str(s).endswith("NSETEST")
            ]
        except Exception as e:
            logger.debug(f"SymbolManager: DataFrame extraction failed: {e}")
            return []

    def _load_from_csv(self) -> list[str]:
        """Load FNO underlyings from Groww's packaged instruments.csv."""
        try:
            import growwapi
            csv_path = os.path.join(os.path.dirname(growwapi.__file__), "instruments.csv")
            if not os.path.exists(csv_path):
                logger.warning("SymbolManager: instruments.csv not found in growwapi package.")
                return []
            seen: set[str] = set()
            with open(csv_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    sym = row.get("underlying_symbol", "").strip()
                    seg = row.get("segment", "").upper()
                    if sym and seg == "FNO" and not sym.endswith("NSETEST"):
                        seen.add(sym)
            logger.info(f"SymbolManager: loaded {len(seen)} symbols from instruments.csv")
            return sorted(seen)
        except Exception as e:
            logger.warning(f"SymbolManager: CSV load failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Alias building
    # ------------------------------------------------------------------

    def _build_aliases(self, symbols: list[str]) -> dict[str, str]:
        """
        Build a complete alias dict from:
        1. Auto-generated variants of each Groww symbol.
        2. symbol_overrides.json (manual non-obvious mappings).
        Overrides take precedence over auto-generated aliases.
        """
        aliases: dict[str, str] = {}

        # 1. Auto-generate from symbol list
        for sym in symbols:
            for variant in self._auto_variants(sym):
                key = self._normalise_key(variant)
                if key and key not in aliases:
                    aliases[key] = sym

        # 2. Merge manual overrides (higher priority)
        overrides = self._load_overrides()
        for raw_key, groww_sym in overrides.items():
            key = self._normalise_key(raw_key)
            if key:
                aliases[key] = groww_sym

        return aliases

    @staticmethod
    def _auto_variants(symbol: str) -> list[str]:
        """
        Derive common channel-name variants from a Groww underlying_symbol.
        e.g. BAJAJ-AUTO → [BAJAJ-AUTO, BAJAJAUTO, BAJAJ_AUTO]
             M&M        → [M&M, MM, M_M]
             HINDUNILVR → [HINDUNILVR, HINDUSTAN UNILEVER, HUL]  (only exact transforms)
        """
        variants = [symbol]
        s = symbol.upper()

        # Separator variants: -, &, _
        for sep in ("-", "&", "_"):
            if sep in s:
                variants.append(s.replace(sep, ""))
                variants.append(s.replace(sep, "_" if sep != "_" else "-"))

        # Common suffix stripping: LTD, LIMITED, IND, INDIA, BANK, FINANCE
        for suffix in ("LTD", "LIMITED", "IND", "INDIA", "BANK", "FINANCE", "CORP", "CO"):
            if s.endswith(suffix) and len(s) > len(suffix) + 2:
                variants.append(s[: -len(suffix)])

        # Deduplicate preserving order
        seen: set[str] = set()
        out = []
        for v in variants:
            v = v.strip("-_& ")
            if v and v not in seen:
                seen.add(v)
                out.append(v)
        return out

    # ------------------------------------------------------------------
    # Overrides file
    # ------------------------------------------------------------------

    @staticmethod
    def _load_overrides() -> dict[str, str]:
        """Load symbol_overrides.json. Returns empty dict if file missing/invalid."""
        try:
            if not os.path.exists(_OVERRIDES_FILE):
                logger.debug("SymbolManager: symbol_overrides.json not found, skipping.")
                return {}
            with open(_OVERRIDES_FILE, encoding="utf-8") as f:
                data = json.load(f)
            overrides = data.get("overrides", {})
            if not isinstance(overrides, dict):
                return {}
            return {str(k): str(v) for k, v in overrides.items()}
        except Exception as e:
            logger.warning(f"SymbolManager: could not load symbol_overrides.json: {e}")
            return {}

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _cache_is_fresh(self) -> bool:
        if not os.path.exists(_CACHE_FILE):
            return False
        try:
            with open(_CACHE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            updated_at = data.get("updated_at", "")
            if not updated_at:
                return False
            ts = datetime.datetime.fromisoformat(updated_at)
            age = datetime.datetime.now() - ts
            return age.days < self._max_age_days
        except Exception:
            return False

    def _load_cache(self) -> bool:
        try:
            with open(_CACHE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            self._symbols = data.get("symbols", [])
            self._aliases = {
                self._normalise_key(k): v
                for k, v in data.get("aliases", {}).items()
                if k and v
            }
            # Always re-apply manual overrides at load time so local edits in
            # symbol_overrides.json take effect immediately even with fresh cache.
            overrides = self._load_overrides()
            for raw_key, groww_sym in overrides.items():
                key = self._normalise_key(raw_key)
                if key:
                    self._aliases[key] = groww_sym
            self._symbol_keys = {self._normalise_key(s) for s in self._symbols}
            self._loaded = True
            logger.info(
                f"SymbolManager: cache loaded — {len(self._symbols)} symbols, "
                f"{len(self._aliases)} aliases."
            )
            return True
        except Exception as e:
            logger.warning(f"SymbolManager: cache load failed: {e}")
            return False

    def _save_cache(self) -> None:
        try:
            data = {
                "updated_at": datetime.datetime.now().isoformat(timespec="seconds"),
                "symbols": self._symbols,
                "aliases": {k: v for k, v in sorted(self._aliases.items())},
            }
            with open(_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"SymbolManager: saved symbol_map.json ({len(self._symbols)} symbols).")
        except Exception as e:
            logger.warning(f"SymbolManager: could not save symbol_map.json: {e}")

    def _load_overrides_only(self) -> None:
        """Last-resort: build alias map from overrides file only, no symbol list."""
        overrides = self._load_overrides()
        self._symbols = []
        self._aliases = {self._normalise_key(k): v for k, v in overrides.items()}
        self._symbol_keys: set[str] = set()
        self._loaded = True
        logger.info(f"SymbolManager: overrides-only mode ({len(self._aliases)} aliases).")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_key(s: str) -> str:
        """Normalise to upper-case, strip #, spaces, special chars for dict lookup."""
        return re.sub(r"[^A-Z0-9&_\-]", "", s.upper().replace("#", "").replace(" ", ""))

    @staticmethod
    def _extract_symbol_candidates(text: str) -> list[str]:
        """
        Extract symbol-like tokens from a message.
        Looks for: #TOKEN, ALL-CAPS words 3+ chars, known patterns.
        """
        candidates: list[str] = []
        seen: set[str] = set()

        # Hashtag tokens: #WAAREE, #TATASTL
        for m in re.finditer(r"#([A-Z0-9&_\-]{2,})", text.upper()):
            tok = m.group(1)
            if tok not in seen:
                seen.add(tok)
                candidates.append(tok)

        # ALL-CAPS words 3+ chars (likely tickers)
        for m in re.finditer(r"\b([A-Z][A-Z0-9&_\-]{2,})\b", text.upper()):
            tok = m.group(1)
            # Skip common non-symbol words
            if tok in (
                "BUY", "SELL", "CMP", "SL", "TGT", "TSL", "AT", "CE", "PE",
                "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
                "JAN", "FEB", "MAR", "THE", "AND", "FOR", "NEW", "OLD",
                "TRAIL", "MOVE", "HOLD", "EXIT", "CLOSE", "FULL", "COST",
            ):
                continue
            if tok not in seen:
                seen.add(tok)
                candidates.append(tok)

        return candidates


# Module-level singleton
symbol_manager = SymbolManager()
