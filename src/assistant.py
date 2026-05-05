import json
import re
import time
import hashlib
import datetime
import pytz
from openai import OpenAI
from .config import Config
from .logger import logger
from .models import TradeSignal
from .symbol_manager import symbol_manager
from .metrics import metrics

# ---------------------------------------------------------------------------
# Module-level LLM response cache
# ---------------------------------------------------------------------------
_LLM_CACHE: dict[str, tuple[dict, float]] = {}
_LLM_CACHE_TTL = 120.0   # seconds — covers typical burst windows
_LLM_CACHE_MAX = 128     # evict oldest when full


def _llm_cache_key(messages: list[dict]) -> str:
    payload = json.dumps(messages, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _llm_cache_get(key: str) -> dict | None:
    entry = _LLM_CACHE.get(key)
    if entry and time.monotonic() - entry[1] < _LLM_CACHE_TTL:
        return entry[0]
    return None


def _llm_cache_set(key: str, value: dict) -> None:
    if len(_LLM_CACHE) >= _LLM_CACHE_MAX:
        oldest = min(_LLM_CACHE, key=lambda k: _LLM_CACHE[k][1])
        del _LLM_CACHE[oldest]
    _LLM_CACHE[key] = (value, time.monotonic())


class Assistant:
    """
    Handles all interactions with OpenAI.
    Specializes in parsing Telegram messages into structured trade data.
    """

    # Expiry correction pattern: "this is APR expiry", "april expiry not march", etc.
    _EXPIRY_CORRECTION_RE = re.compile(
        r"\b(?:this\s+is\s+)?(?P<month>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|"
        r"JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)"
        r"\s+EXPIRY\b",
        re.IGNORECASE,
    )
    _MONTH_ABBR_MAP = {
        "JANUARY": "JAN", "FEBRUARY": "FEB", "MARCH": "MAR", "APRIL": "APR",
        "MAY": "MAY", "JUNE": "JUN", "JULY": "JUL", "AUGUST": "AUG",
        "SEPTEMBER": "SEP", "OCTOBER": "OCT", "NOVEMBER": "NOV", "DECEMBER": "DEC",
    }

    # Deterministic exit / trail detection (before LLM). Reply tail takes precedence over CONTEXT.
    _EXIT_PHRASES = (
        "TSL GONE",
        "TSL HIT",
        "SL HIT",
        "STOPLOSS HIT",
        "STOP LOSS HIT",
        "SL TRIGGERED",
        "STOP LOSS TRIGGERED",
        "BOOK PROFIT",
        "BOOKED PROFIT",
        "BOOKED",
        "SQUARE OFF",
        "SQUARED OFF",
        "CLOSE THE TRADE",
        "CLOSE TRADE",
        "TRADE CLOSED",
        "TRADE COMPLETE",
        "POSITION CLOSED",
        "EXIT TRADE",
        "EXIT COMPLETE",
        "EXIT DONE",
        "EXITED",
        "FULL EXIT",
        "STOPPED OUT",
        "CLOSED @",
        "CLOSED AT",
        "LOSS :",
        "PROFIT :",
        "BOOKED LOSS",
        "BOOKED AT",
    )
    _TRAIL_PHRASES = (
        "TRAIL SL",
        "TRAIL STOP",
        "TRAILING SL",
        "MOVE SL",
        "MOVE STOP",
        "NEW SL",
        "NEW TSL",
        "TSL TO",
        "SHIFT SL",
        "SL TO",
        "UPDATE SL",
        "REVISED SL",
    )
    # Volatility schedule early restore / re-arm true SL on broker (before LLM).
    _RESTORE_SL_PHRASES = (
        "RESTORE SL",
        "RESTORE STOP",
        "RESTORE STOPLOSS",
        "RESTORE STOP LOSS",
        "KEEP SL/TSL NOW IN SYSTEM",
        "KEEP SL TSL NOW IN SYSTEM",
        "KEEP SL NOW IN SYSTEM",
        "KEEP TSL NOW IN SYSTEM",
        "RESET SL",
        "RESET STOP",
        "REVERT SL",
        "REVERT STOP",
        "ORIGINAL SL",
        "TRUE SL",
        "CHANNEL SL",
        "BACK TO SL",
        "SL RESTORE",
        "RESTORE PROTECTION",
    )
    # Phrases that indicate an admin is updating the target for an existing trade.
    _UPDATE_TGT_PHRASES = (
        "CHANGE T1",
        "CHANGE T2",
        "CHANGE T3",
        "CHANGE TARGET",
        "CHANGE TGT",
        "UPDATE T1",
        "UPDATE T2",
        "UPDATE T3",
        "UPDATE TARGET",
        "UPDATE TGT",
        "NEW TARGET",
        "NEW TGT",
        "NEW T1",
        "NEW T2",
        "REVISE TARGET",
        "REVISED TARGET",
        "REVISED TGT",
        "MODIFY TARGET",
        "MODIFY TGT",
        "TARGET CHANGED",
        "TARGET UPDATED",
        "TGT CHANGED",
        "TGT UPDATED",
        "T1 TO",
        "T2 TO",
        "T1 CHANGED",
        "T2 CHANGED",
    )
    # Phrases that indicate an admin is extending the entry buy range, NOT updating SL.
    _RANGE_EXTEND_PHRASES = (
        "CAN BUY TILL",
        "CAN BUY UPTO",
        "CAN BUY UP TO",
        "BUY TILL",
        "BUY UPTO",
        "BUY UP TO",
        "BUY TILL",
        "ENTRY TILL",
        "ENTRY UPTO",
        "ENTRY UP TO",
        "RANGE UPTO",
        "RANGE UP TO",
        "RANGE TILL",
        "MAX ENTRY",
        "EXTEND RANGE",
        "EXTENDED RANGE",
        "RANGE EXTENDED",
        "ACCEPTABLE TILL",
        "VALID TILL",
        "VALID UPTO",
    )
    # Phrases that indicate target milestone hit (informational trigger for reconciliation).
    _TARGET_HIT_PHRASES = (
        "TARGET HIT",
        "TARGET HITS",
        "TGT HIT",
        "TGT HITS",
        "TARGET 1 HIT",
        "TARGET 1 HITS",
        "TARGET 2 HIT",
        "TARGET 2 HITS",
        "TARGET1 HIT",
        "TARGET1 HITS",
        "TARGET2 HIT",
        "TARGET2 HITS",
        "T1 HIT",
        "T1 HITS",
        "T2 HIT",
        "T2 HITS",
    )
    _ACK_ONLY_TOKENS = frozenset(
        {
            "OK",
            "DONE",
            "LOCKED",
            "YES",
            "Y",
            "K",
            "KK",
            "👍",
            "✅",
            "☑️",
            "☑",
        }
    )
    _ALLOWED_ACTIONS = frozenset(
        {
            "BUY",
            "SELL",
            "TRAIL_SL",
            "RESTORE_SL",
            "RESTORE_ALL",
            "EXIT_ALL",
            "EXTEND_RANGE",
            "UPDATE_TGT",
            "RECONCILE",
            "COMMENT",
        }
    )

    _ENTRY_PHRASES = (
        " BUY ",
        " SELL ",
        " AT ",
        " CMP ",
        " ENTRY ",
        " RANGE ",
    )
    _COMMENTARY_PHRASES = (
        "STATUS",
        "SUMMARY",
        "EXPECTATION",
        "PERFORMANCE",
        "ACTIVE STOCK OPTIONS",
        "INDEX OPTIONS",
        "WORKED WELL",
        "CARRYING",
    )
    _SIZE_OVERRIDE_FIXED_PHRASES = (
        "RISKY TRADE",
        "HIGH RISK",
        "TRADE LESS QTY",
        "LESS QTY",
        "LOW QTY",
        "SMALL QTY",
        "REDUCE QTY",
        "TRADE SMALL",
    )

    def __init__(self):
        self.api_key = Config.OPENAI_API_KEY
        if not self.api_key:
            logger.error("No OPENAI_API_KEY found in Config.")

        self.client = OpenAI(api_key=self.api_key) if self.api_key else None
        self.model = Config.OPENAI_MODEL

    def _call_openai(self, messages: list[dict]) -> dict | None:
        """
        Call the OpenAI chat completions API with:
        - A module-level LRU cache (120 s TTL, 128 entries) to skip duplicate calls.
        - Up to 3 attempts with exponential backoff (2 s, 4 s) on transient errors.
        Returns the parsed JSON dict on success, or None on failure.
        """
        if not self.client:
            logger.error("[LLM] OpenAI client not initialised — skipping call.")
            return None

        metrics.increment("llm_calls")
        cache_key = _llm_cache_key(messages)
        cached = _llm_cache_get(cache_key)
        if cached is not None:
            logger.debug("[LLM] Cache hit — skipping API call.")
            metrics.increment("llm_cache_hits")
            return cached

        last_err: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    response_format={"type": "json_object"},
                )
                data = self._extract_llm_json(response)
                if data is None:
                    logger.error("[LLM] Attempt %d: empty/invalid JSON in response.", attempt)
                    metrics.increment("llm_parse_failed")
                    return None
                _llm_cache_set(cache_key, data)
                return data
            except Exception as e:
                last_err = e
                err_str = str(e)
                is_transient = any(k in err_str.lower() for k in (
                    "rate limit", "ratelimit", "429",
                    "server error", "503", "502", "500",
                    "timeout", "connection",
                ))
                if is_transient and attempt < 3:
                    wait = 2 ** attempt  # 2s, 4s
                    logger.warning(
                        "[LLM] Attempt %d transient error (%s) — retrying in %ds.",
                        attempt, err_str[:80], wait,
                    )
                    metrics.increment("llm_retries")
                    time.sleep(wait)
                else:
                    break

        logger.error("[LLM] All attempts failed: %s", last_err, exc_info=True)
        metrics.increment("llm_failed")
        return None

    @staticmethod
    def _apply_alias_to_data(data: dict) -> dict:
        """Resolve official_symbol and stock fields via SymbolManager before symbol factory."""
        for field in ("official_symbol", "stock"):
            raw = str(data.get(field, "") or "")
            if raw:
                resolved = symbol_manager.resolve(raw)
                if resolved != raw:
                    logger.info(f"Symbol alias: {raw} → {resolved}")
                data[field] = resolved
        return data

    @staticmethod
    def _build_symbol_hint(text: str) -> str:
        """Return a targeted LLM hint for symbols detected in the message text."""
        return symbol_manager.hint_for_message(text)

    @staticmethod
    def _normalize_for_match(s: str) -> str:
        s = s.upper()
        s = re.sub(r"[^\w\s#]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @staticmethod
    def _split_context_and_tail(text: str) -> tuple[str, str]:
        """
        Listener format: [CONTEXT: <reply body>] <new message>
        Returns (context_inner_or_empty, tail_or_full).
        """
        t = text.strip()
        prefix = "[CONTEXT:"
        if not t.startswith(prefix):
            return "", t
        # First `] ` after prefix closes CONTEXT wrapper (inner text rarely contains `] `).
        close = t.find("] ", len(prefix))
        if close == -1:
            return "", t
        inner = t[len(prefix) : close].strip()
        tail = t[close + 2 :].strip()
        return inner, tail

    def _detect_forced_intent(self, tail: str, full: str) -> tuple[str, float | None] | None:
        """
        Returns ("EXIT_ALL", None), ("RESTORE_SL", None), ("TRAIL_SL", price_or_None), or None.
        Prefer tail when non-empty; then scan full string.
        """
        chunks = [tail] if tail.strip() else []
        chunks.append(full)
        for chunk in chunks:
            n = self._normalize_for_match(chunk)
            if not n:
                continue
            for phrase in self._EXIT_PHRASES:
                # Keep colon-style P&L summaries strict so generic commentary like
                # "profit booking" does not become a forced EXIT_ALL.
                if phrase in ("PROFIT :", "LOSS :"):
                    # Guard against risk lines like "MAX LOSS: 5000" / "MAX PROFIT: ...",
                    # which are not exit instructions.
                    kw = phrase.split(":")[0]
                    if re.search(rf"(?<!\bMAX\s)\b{kw}\s*:", chunk, re.IGNORECASE):
                        return ("EXIT_ALL", None)
                    continue
                if self._normalize_for_match(phrase) in n:
                    return ("EXIT_ALL", None)
            for phrase in self._RESTORE_SL_PHRASES:
                if self._normalize_for_match(phrase) in n:
                    return ("RESTORE_SL", None)
            for phrase in self._TRAIL_PHRASES:
                if self._normalize_for_match(phrase) in n:
                    sl = self._extract_trail_sl_price(chunk)
                    return ("TRAIL_SL", sl)
            # "TSL 40 …" / "TSL TO 40 …" (shorthand; avoids bare "TSL" substring false positives).
            if re.search(r"(?<![\w/])TSL(?:\s+TO)?\s+\d", chunk, re.IGNORECASE):
                if not re.search(r"\bTSL\s+(?:GONE|HIT)\b", chunk, re.IGNORECASE):
                    sl = self._extract_trail_sl_price(chunk)
                    if sl is not None:
                        return ("TRAIL_SL", sl)
        return None

    @classmethod
    def _tail_has_explicit_exit_or_trail(cls, tail: str) -> bool:
        """
        For reply-style messages, only force EXIT/TRAIL when the reply tail
        explicitly contains a known exit/trail phrase.
        """
        n = cls._normalize_for_match(tail)
        if not n:
            return False
        for phrase in cls._EXIT_PHRASES:
            if cls._normalize_for_match(phrase) in n:
                return True
        for phrase in cls._TRAIL_PHRASES:
            if cls._normalize_for_match(phrase) in n:
                return True
        if re.search(r"(?<![\w/])TSL(?:\s+TO)?\s+\d", tail, re.IGNORECASE):
            if not re.search(r"\bTSL\s+(?:GONE|HIT)\b", tail, re.IGNORECASE):
                return True
        for phrase in cls._RESTORE_SL_PHRASES:
            if cls._normalize_for_match(phrase) in n:
                return True
        return False

    @classmethod
    def _is_ack_only_tail(cls, tail: str) -> bool:
        """
        Returns True for short acknowledgement-only replies like '✅', 'ok', etc.
        These should not open/close/modify trades by themselves.
        """
        stripped = tail.strip()
        if not stripped:
            return False

        # Emoji-only or tiny acknowledgements.
        if len(stripped) <= 3 and stripped in cls._ACK_ONLY_TOKENS:
            return True

        n = cls._normalize_for_match(stripped)
        if not n:
            return True
        if n in cls._ACK_ONLY_TOKENS:
            return True
        # Cases like "#WAAREEENER ✅" -> tokens are just hashtag + ack.
        tokens = [t for t in n.split() if t]
        if not tokens:
            return True
        non_ack = [t for t in tokens if t not in cls._ACK_ONLY_TOKENS and not t.startswith("#")]
        return len(non_ack) == 0

    @classmethod
    def _detect_expiry_correction(cls, child: str) -> str | None:
        """
        Returns the corrected 3-letter month abbreviation if the child message
        is an expiry correction (e.g. "This is April Expiry not March.").
        Returns None if no correction pattern is found.
        """
        m = cls._EXPIRY_CORRECTION_RE.search(child)
        if not m:
            return None
        raw = m.group("month").upper()
        return cls._MONTH_ABBR_MAP.get(raw, raw[:3])

    @classmethod
    def _detect_typo_correction(cls, child: str) -> tuple[str, str] | None:
        """
        Detects strike typo-correction replies such as:
          - "23800 PE TYPO"
          - "TYPO 23800 PE"
          - "CORRECTION: 23800CE"
        Returns (strike, option_type) when found.
        """
        text = str(child or "")
        n = cls._normalize_for_match(text)
        if not n:
            return None
        if not any(token in n for token in ("TYPO", "CORRECT", "CORRECTION", "WRONG STRIKE", "STRIKE TYPO")):
            return None

        m = re.search(r"\b(\d{3,6})\s*(CE|PE)\b", text, re.IGNORECASE)
        if not m:
            m = re.search(r"\b(CE|PE)\s*(\d{3,6})\b", text, re.IGNORECASE)
            if not m:
                m = re.search(r"\b(\d{3,6})(CE|PE)\b", text, re.IGNORECASE)
                if not m:
                    return None
                strike, option_type = m.group(1), m.group(2)
            else:
                option_type, strike = m.group(1), m.group(2)
        else:
            strike, option_type = m.group(1), m.group(2)
        return re.sub(r"[^\d]", "", strike), str(option_type).upper()

    @staticmethod
    def _extract_trail_sl_price(text: str) -> float | None:
        # "TSL TO 55", "NEW TSL 40", "SL TO 55", "STOP LOSS: 55", etc.
        m = re.search(
            r"(?:NEW\s+)?(?:TSL|SL|STOP\s*LOSS|STOPLOSS)\s*(?:[:=]|AT|TO)?\s*(\d+(?:\.\d+)?)",
            text,
            re.IGNORECASE,
        )
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return None
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*(?:AS\s+)?(?:TSL|SL|STOP)", text, re.IGNORECASE)
        if m2:
            try:
                return float(m2.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _extract_range_extend_price(text: str) -> float | None:
        """
        Extract the upper price limit from range-extension messages.
        Handles: "CAN BUY TILL 5.85", "BUY UP TO 6", "MAX ENTRY 6.10", "RANGE UPTO 5.9"
        """
        # Pattern: keyword phrase followed by a number
        m = re.search(
            r"(?:CAN\s+BUY|BUY|ENTRY|RANGE|MAX\s+ENTRY|ACCEPTABLE|VALID|EXTEND(?:ED)?)\s+"
            r"(?:TILL|UPTO|UP\s+TO|RANGE|EXTENDED\s+TO|TO)?\s*(\d+(?:\.\d+)?)",
            text,
            re.IGNORECASE,
        )
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return None
        # Fallback: last standalone number in the message
        nums = re.findall(r"\b(\d+(?:\.\d+)?)\b", text)
        if nums:
            try:
                return float(nums[-1])
            except ValueError:
                return None
        return None

    @classmethod
    def _detect_range_extend(cls, tail: str) -> float | None:
        """
        Returns the new upper entry price if the tail is a range-extension message,
        otherwise returns None.
        Only fires when the normalised tail contains one of _RANGE_EXTEND_PHRASES.
        """
        n = cls._normalize_for_match(tail)
        if not n:
            return None
        for phrase in cls._RANGE_EXTEND_PHRASES:
            if phrase in n:
                price = cls._extract_range_extend_price(tail)
                return price  # may be None if no number found
        return None

    @staticmethod
    def _extract_update_tgt_price(text: str) -> float | None:
        """
        Extract the new target price from target-update messages.
        Handles: "CHANGE T1 TO 7.45", "NEW TARGET 8", "UPDATE TGT TO 9.50", "T1 TO 7.45"
        """
        m = re.search(
            r"(?:CHANGE|UPDATE|NEW|REVISE[D]?|MODIFY|REVISED?)\s+"
            r"(?:T[123]|TGT|TARGET)\s*(?:TO|AT|=|:)?\s*(\d+(?:\.\d+)?)",
            text,
            re.IGNORECASE,
        )
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return None
        # "TARGET 7.45" / "CHANGE TARGET 8" / "CHANGE TARGET TO 9" (admin-style shorthand)
        m3b = re.search(
            r"\b(?:CHANGE\s+TARGET(?:\s+TO)?|TARGET)\s+(\d+(?:\.\d+)?)\b",
            text,
            re.IGNORECASE,
        )
        if m3b:
            try:
                return float(m3b.group(1))
            except ValueError:
                return None
        # "T1 TO 7.45" / "T2 TO 8"
        m2 = re.search(r"\bT[123]\s+TO\s+(\d+(?:\.\d+)?)\b", text, re.IGNORECASE)
        if m2:
            try:
                return float(m2.group(1))
            except ValueError:
                return None
        # "TARGET TO 7.45" / "TGT TO 8"
        m3 = re.search(r"\b(?:TARGET|TGT)\s+(?:TO|AT|=|:)\s*(\d+(?:\.\d+)?)\b", text, re.IGNORECASE)
        if m3:
            try:
                return float(m3.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _extract_update_tgt_leg_index(text: str) -> int:
        """
        Extract explicit target leg selector from update-target text.
        Returns 1/2/3 when explicitly requested (e.g. T1, TARGET 2), else 0 for "all legs".
        """
        raw = str(text or "")
        if not raw.strip():
            return 0
        m = re.search(r"\b(?:T|TARGET)\s*([123])\b", raw, re.IGNORECASE)
        if m:
            try:
                idx = int(m.group(1))
                if idx in (1, 2, 3):
                    return idx
            except Exception:
                return 0
        return 0

    @staticmethod
    def _extract_hold_target_price(text: str) -> float | None:
        """
        Extract runner target hints from phrases like:
        - HOLD FOR 59.5
        - HOLD TILL 60
        - HOLD UPTO 61.25
        """
        m = re.search(
            r"\bHOLD\s+(?:FOR|TILL|UPTO|UP\s+TO|AT|TO)\s*(\d+(?:\.\d+)?)\b",
            text,
            re.IGNORECASE,
        )
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _extract_signal_targets(text: str) -> list[float]:
        """
        Extract explicit multi-target ladder from signal text.
        Examples: "TARGETS: 68, 88" or "TGT : 18/24/36+".
        """
        raw = str(text or "")
        if not raw.strip():
            return []
        out: list[float] = []
        seen: set[float] = set()
        for line in raw.replace("\r", "\n").split("\n"):
            m = re.search(r"\b(?:TARGETS?|TGT)\b\s*[:\-]?\s*(.+)$", line, flags=re.IGNORECASE)
            if not m:
                continue
            # Ignore "2X/3X" style RR multipliers often written after targets, e.g.
            # "TARGETS : 26, 48 [2X,3X]".
            for num in re.findall(r"(\d+(?:\.\d+)?)(?!\s*[xX]\b)\+?", m.group(1)):
                try:
                    value = float(num)
                except ValueError:
                    continue
                if value <= 0 or value in seen:
                    continue
                out.append(value)
                seen.add(value)
        return out

    @classmethod
    def _detect_update_tgt(cls, tail: str) -> float | None:
        """
        Returns the new target price if the tail is a target-update message,
        otherwise returns None.
        Only fires when the normalised tail contains one of _UPDATE_TGT_PHRASES.
        """
        raw = str(tail or "").strip()
        if not raw:
            return None

        # Guard: "TARGET 1 HIT" / "TARGET 2 HITS" are progression updates, not
        # admin target-price edits. Without this check we can misread "TARGET 1"
        # as price 1.0 and send a bad UPDATE_TGT.
        if cls._has_target_hit_phrase(raw):
            return None

        # Allow shorthand "TARGET 52" only when the reply is not a mixed
        # progression/trailing message (e.g. "TARGET 1 HITS ... HOLD FOR 52").
        if re.match(r"^\s*TARGET\s+\d", raw, re.IGNORECASE):
            if re.search(r"\b(?:HOLD|TSL|TRAIL(?:ING)?\s*SL|SL\s+TO)\b", raw, re.IGNORECASE):
                return None
            px = cls._extract_update_tgt_price(tail)
            if px is not None:
                return px
        n = cls._normalize_for_match(tail)
        if not n:
            return None
        for phrase in cls._UPDATE_TGT_PHRASES:
            if phrase in n:
                price = cls._extract_update_tgt_price(tail)
                return price  # may be None if no number found
        return None

    @classmethod
    def _has_target_hit_phrase(cls, text: str) -> bool:
        n = cls._normalize_for_match(text)
        if not n:
            return False
        for phrase in cls._TARGET_HIT_PHRASES:
            if phrase in n:
                return True
        # Also handle "TARGET 1 ... HIT" style with emojis/punctuation between words.
        return bool(re.search(r"\b(?:TARGET|TGT|T[12])\s*1?\b.*\bHIT(?:S)?\b", n))

    @staticmethod
    def _has_option_hint(text: str) -> bool:
        u = text.upper()
        return bool(re.search(r"\b\d{2,6}\s*(CE|PE)\b", u)) or (" CE" in u or " PE" in u)

    @staticmethod
    def _has_entry_range_hint(text: str) -> bool:
        u = text.upper()
        return bool(re.search(r"\b\d+(?:\.\d+)?\s*[-/]\s*\d+(?:\.\d+)?\b", u))

    def _looks_like_commentary(self, text: str) -> bool:
        n = f" {self._normalize_for_match(text)} "
        if not n.strip():
            return True
        for p in self._COMMENTARY_PHRASES:
            if p in n:
                return True
        return False

    @staticmethod
    def _normalize_action(action: str) -> str:
        a = str(action or "").upper().strip()
        aliases = {
            "EXIT": "EXIT_ALL",
            "CLOSE": "EXIT_ALL",
            "STOPLOSS": "EXIT_ALL",
            "SL": "TRAIL_SL",
            "TRAIL": "TRAIL_SL",
            "IGNORE": "COMMENT",
            "NONE": "COMMENT",
        }
        return aliases.get(a, a)

    @staticmethod
    def _valid_entry_range(data: dict) -> bool:
        er = data.get("entry_range")
        if not isinstance(er, list) or len(er) < 2:
            return False
        try:
            lo = float(er[0])
            hi = float(er[1])
            return lo > 0 and hi > 0 and hi >= lo
        except Exception:
            return False

    @staticmethod
    def _has_symbol_fields(data: dict) -> bool:
        strike = re.sub(r"[^\d]", "", str(data.get("strike", "") or ""))
        opt = str(data.get("option_type", "") or "").upper()
        under = str(data.get("official_symbol", data.get("stock", "")) or "").strip()
        return bool(under) and bool(strike) and opt in ("CE", "PE")

    def _validate_intent(
        self,
        data: dict,
        *,
        child_text: str,
        parent_text: str,
        is_reply: bool,
        forced_action: str | None = None,
    ) -> tuple[dict | None, str]:
        if not isinstance(data, dict):
            return None, "invalid_payload"
        if data.get("is_trade") is False:
            return None, "model_marked_non_trade"

        action = self._normalize_action(str(data.get("action", "") or ""))
        if forced_action:
            action = forced_action
        if action not in self._ALLOWED_ACTIONS:
            return None, f"unsupported_action:{action}"
        if action == "COMMENT":
            return None, "comment_action"

        child = child_text or ""
        if is_reply and self._looks_like_commentary(child):
            return None, "reply_commentary"

        nchild = f" {self._normalize_for_match(child)} "
        has_entry_cue = any(k in nchild for k in self._ENTRY_PHRASES) or self._has_entry_range_hint(child)

        if action in ("BUY", "SELL"):
            if is_reply and not has_entry_cue:
                return None, "reply_without_entry_cue"
            if not self._has_symbol_fields(data):
                return None, "missing_symbol_fields"
            if not self._valid_entry_range(data):
                return None, "invalid_entry_range"

        if action == "TRAIL_SL":
            # Block only pure acks; allow substantive SL-update messages through.
            if is_reply and self._is_ack_only_tail(child):
                return None, "reply_is_ack_only_not_trail"
            sl = data.get("stop_loss")
            try:
                if float(sl or 0) <= 0:
                    return None, "missing_trail_sl"
            except Exception:
                return None, "invalid_trail_sl"
            if not self._has_symbol_fields(data) and not self._has_option_hint(parent_text):
                return None, "missing_trail_symbol"

        if action == "EXIT_ALL":
            # Block only if the reply tail is a pure ack (e.g. "✅", "ok", "#SYMBOL ✅").
            # Substantive exit reports ("TRADE CLOSED @ 13.5 ... EXIT COMPLETE ✅") must pass through.
            if is_reply and self._is_ack_only_tail(child):
                return None, "reply_is_ack_only_not_exit"
            if not self._has_symbol_fields(data) and not self._has_option_hint(parent_text):
                return None, "missing_exit_symbol"

        if action == "RESTORE_SL":
            if is_reply and self._is_ack_only_tail(child):
                return None, "reply_is_ack_only_not_restore"
            if not self._has_symbol_fields(data) and not self._has_option_hint(parent_text):
                return None, "missing_restore_symbol"

        if action == "EXTEND_RANGE":
            # Must be a reply with a valid new upper price in entry_range.
            er = data.get("entry_range")
            try:
                new_max = float((er or [0, 0])[1])
                if new_max <= 0:
                    return None, "extend_range_missing_price"
            except Exception:
                return None, "extend_range_invalid_price"
            if not self._has_symbol_fields(data) and not self._has_option_hint(parent_text):
                return None, "extend_range_missing_symbol"

        if action == "UPDATE_TGT":
            # Must have a valid new target price > 0.
            try:
                new_tgt = float(data.get("target") or 0)
                if new_tgt <= 0:
                    return None, "update_tgt_missing_price"
            except Exception:
                return None, "update_tgt_invalid_price"
            if not self._has_symbol_fields(data) and not self._has_option_hint(parent_text):
                return None, "update_tgt_missing_symbol"

        if action == "RECONCILE":
            if not self._has_symbol_fields(data) and not self._has_option_hint(parent_text):
                return None, "reconcile_missing_symbol"

        data["action"] = action
        data["is_trade"] = True
        return data, "ok"

    _INDEX_UNDERLYINGS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"})

    # Weekly expiry weekday per index (0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri)
    # Source: NSE/BSE circulars effective September 2025
    _INDEX_EXPIRY_WEEKDAY: dict[str, int] = {
        "NIFTY":      1,   # Tuesday
        "FINNIFTY":   1,   # Tuesday
        "MIDCPNIFTY": 0,   # Monday
        "BANKNIFTY":  3,   # Thursday
        "SENSEX":     3,   # Thursday
        "BANKEX":     0,   # Monday
    }
    _MONTH_ABBR = {
        "1": "JAN", "2": "FEB", "3": "MAR", "4": "APR", "5": "MAY", "6": "JUN",
        "7": "JUL", "8": "AUG", "9": "SEP", "10": "OCT", "11": "NOV", "12": "DEC",
    }
    _MONTH_NUM = {v: k for k, v in _MONTH_ABBR.items()}

    # Use broker timezone (IST) for all expiry/date decisions so running the bot
    # from non-India locales (e.g. Sydney) does not skew weekly expiry roll logic.
    _IST_TZ = pytz.timezone("Asia/Kolkata")

    @classmethod
    def _now_ist(cls) -> datetime.datetime:
        return datetime.datetime.now(cls._IST_TZ)

    @classmethod
    def _today_ist(cls) -> datetime.date:
        return cls._now_ist().date()

    @classmethod
    def _safe_year_yy(cls, raw_year) -> str:
        s = str(raw_year or "")
        m4 = re.search(r"\b(\d{4})\b", s)
        if m4:
            return m4.group(1)[-2:]
        m2 = re.search(r"\b(\d{2})\b", s)
        if m2:
            return m2.group(1)
        return str(cls._now_ist().year)[-2:]

    @classmethod
    def _safe_year_4d(cls, raw_year) -> str:
        s = str(raw_year or "")
        m4 = re.search(r"\b(\d{4})\b", s)
        if m4:
            return m4.group(1)
        m2 = re.search(r"\b(\d{2})\b", s)
        if m2:
            return f"20{m2.group(1)}"
        return str(cls._now_ist().year)

    @classmethod
    def _safe_month_abbr(cls, raw_month) -> str:
        s = str(raw_month or "").strip().upper()
        if s in cls._MONTH_NUM:
            return s
        if s in cls._MONTH_ABBR:
            return cls._MONTH_ABBR[s]
        m = re.search(r"\d{1,2}", s)
        if m:
            v = str(int(m.group(0)))
            if v in cls._MONTH_ABBR:
                return cls._MONTH_ABBR[v]
        return cls._now_ist().strftime("%b").upper()

    @classmethod
    def _safe_month_num(cls, raw_month) -> str:
        abbr = cls._safe_month_abbr(raw_month)
        # Calendar month number 01-12 (used for date math). Index *weekly* Groww symbols
        # use a single-digit month 1-9 / two-digit 10-11/11-12 in the token (see _apply_symbol_factory).
        mm = cls._MONTH_NUM.get(abbr, str(cls._now_ist().month))
        mm = re.sub(r"[^\d]", "", str(mm or ""))
        if not mm:
            mm = str(cls._now_ist().month)
        return str(int(mm)).zfill(2)

    @staticmethod
    def _safe_day_2d(raw_day) -> str:
        s = str(raw_day or "").strip()
        m = re.search(r"\d{1,2}", s)
        if not m:
            return "01"
        d = int(m.group(0))
        if d < 1 or d > 31:
            return "01"
        return str(d).zfill(2)

    @classmethod
    def _current_week_expiry(cls, underlying: str = "") -> tuple[str, str, str]:
        """
        Returns (day_2d, month_abbr, year_4d) for the current week's expiry
        for the given underlying. Each index has its own expiry weekday.
        If today is past this week's expiry (or it's expiry day after 3 PM),
        rolls to the next occurrence of that weekday.
        """
        weekday = cls._INDEX_EXPIRY_WEEKDAY.get(underlying.upper(), 3)  # default Thursday
        today = cls._today_ist()
        days_ahead = (weekday - today.weekday()) % 7
        if days_ahead == 0 and cls._now_ist().hour >= 15:
            days_ahead = 7  # today's expiry has passed
        expiry = today + datetime.timedelta(days=days_ahead)
        month_abbr = expiry.strftime("%b").upper()
        return str(expiry.day).zfill(2), month_abbr, str(expiry.year)

    @classmethod
    def _expiry_is_stale(cls, raw_day, raw_month, raw_year) -> bool:
        """
        Returns True if the given expiry date is strictly in the past (before today).
        Treats ambiguous/missing dates as NOT stale so we don't over-roll.
        """
        _MONTH_NUM = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }
        try:
            month_abbr = cls._safe_month_abbr(raw_month)
            month_num = _MONTH_NUM.get(month_abbr.upper())
            if not month_num:
                return False
            year_str = str(raw_year or "").strip()
            year = (
                int(year_str)
                if len(year_str) == 4
                else (2000 + int(year_str))
                if len(year_str) == 2
                else cls._today_ist().year
            )
            # For stock options, expiry is the last Thursday of the month —
            # treat the whole month as valid until the 1st of the next month.
            # For index options, the day matters; use day=1 as safe fallback.
            day_str = str(raw_day or "").strip()
            day = int(day_str) if day_str.isdigit() and 1 <= int(day_str) <= 31 else 28
            expiry_date = datetime.date(year, month_num, day)
            return expiry_date < cls._today_ist()
        except Exception:
            return False  # If we can't parse it, don't roll

    @staticmethod
    def _current_month_expiry() -> tuple[str, str, str]:
        """
        Returns (day_2d, month_abbr, year_4d) for the current calendar month.
        Day is set to "01" as stock options use monthly expiry (last Thursday),
        and the symbol factory only needs month+year for stock options.
        """
        today = Assistant._today_ist()
        return "01", today.strftime("%b").upper(), str(today.year)

    def _apply_symbol_factory(self, data: dict) -> dict:
        strike = re.sub(r"[^\d]", "", str(data.get("strike", "") or ""))
        option_type = str(data.get("option_type", "") or "").upper()
        if option_type not in ("CE", "PE"):
            option_type = re.sub(r"[^A-Z]", "", option_type)[:2]
        underlying_raw = data.get("official_symbol", data.get("stock", ""))
        underlying = self._safe_underlying(underlying_raw, strike, option_type)

        is_index = underlying in self._INDEX_UNDERLYINGS

        # Resolve expiry defaults based on instrument type
        raw_month = data.get("expiry_month") or ""
        raw_day = data.get("expiry_day") or ""
        raw_year = data.get("expiry_year") or ""
        expiry_is_missing = not str(raw_month).strip() or str(raw_month).strip().upper() in ("", "NA", "NONE", "NULL")

        if expiry_is_missing:
            if is_index:
                raw_day, raw_month, raw_year = self._current_week_expiry(underlying)
            else:
                raw_day, raw_month, raw_year = self._current_month_expiry()
        else:
            # Expiry was given — check if it has already passed.
            # If so, roll forward to the nearest valid default (same as missing-expiry path).
            if self._expiry_is_stale(raw_day, raw_month, raw_year):
                if is_index:
                    new_day, new_month, new_year = self._current_week_expiry(underlying)
                else:
                    new_day, new_month, new_year = self._current_month_expiry()
                logger.info(
                    f"[STAGE 2] Expiry {raw_day}/{raw_month}/{raw_year} is in the past — "
                    f"rolling forward to {new_day}/{new_month}/{new_year}."
                )
                raw_day, raw_month, raw_year = new_day, new_month, new_year

        expiry_day = self._safe_day_2d(raw_day)
        expiry_month = self._safe_month_abbr(raw_month)
        yy = self._safe_year_yy(raw_year)
        yyyy = self._safe_year_4d(raw_year)

        groww_symbol = f"{underlying}{yy}{expiry_month}{strike}{option_type}"

        if is_index:
            # NSE/Groww index weekly: YY + M (no leading zero) + DD + strike + CE|PE
            # e.g. 5 May 2026 -> NIFTY2650524200CE — NOT NIFTY26050524200CE (YY+MM+DD).
            month_num = self._safe_month_num(raw_month)
            month_token = str(int(month_num))
            groww_symbol = f"{underlying}{yy}{month_token}{expiry_day}{strike}{option_type}"

        # Keep normalized expiry fields in sync with the symbol factory output.
        data["expiry_day"] = expiry_day
        data["expiry_month"] = expiry_month
        data["expiry_year"] = yyyy
        data["trading_symbol"] = groww_symbol
        data["stock"] = data["trading_symbol"]
        data["buy_range_min"] = float(data.get("entry_range", [0, 0])[0])
        data["buy_range_max"] = float(data.get("entry_range", [0, 0])[1])
        return data

    @staticmethod
    def _safe_underlying(raw: str, strike: str, option_type: str) -> str:
        s = str(raw or "").upper().strip()
        s = s.replace("#", " ")
        s = re.sub(r"\s+", "", s)
        s = re.sub(r"[^A-Z0-9&_\-]", "", s)
        if strike and option_type and s.endswith(f"{strike}{option_type}"):
            s = s[: -(len(strike) + len(option_type))]
        elif strike and s.endswith(strike):
            s = s[: -len(strike)]
        if option_type and s.endswith(option_type):
            s = s[: -len(option_type)]
        s = re.sub(r"\d+$", "", s)
        s = re.sub(r"[^A-Z&_\-]", "", s)
        return s

    @staticmethod
    def _extract_llm_json(response) -> dict | None:
        try:
            choices = getattr(response, "choices", None) or []
            if not choices:
                return None
            msg = choices[0].message
            content = getattr(msg, "content", None)
            if not content:
                return None
            return json.loads(content)
        except Exception:
            return None

    @staticmethod
    def _has_spread_cues(text: str) -> bool:
        n = re.sub(r"\s+", " ", text.upper())
        has_buy = "BUY" in n
        has_sell = "SELL" in n
        has_first_then = ("FIRST" in n and "THEN" in n) or ("1ST" in n and "2ND" in n)
        has_spread_word = "SPREAD" in n
        return (has_buy and has_sell and has_first_then) or (has_spread_word and has_buy and has_sell)

    @staticmethod
    def _build_llm_payload(parent_text: str, child_text: str, is_reply: bool, original_text: str) -> str:
        payload = {
            "is_reply": bool(is_reply),
            "parent_message": parent_text or "",
            "child_message": child_text or "",
            "original_message": original_text or "",
            "instructions": (
                "Infer action mainly from child_message. Use parent_message only to resolve instrument fields "
                "when child references an existing trade. If child_message is acknowledgement/chatter/performance "
                "commentary without actionable instruction, return is_trade=false."
            ),
        }
        return json.dumps(payload, ensure_ascii=True)

    @staticmethod
    def _current_date_context() -> str:
        now = Assistant._now_ist()
        month_name = now.strftime("%B").upper()
        month_num = now.month
        year = now.year
        next_month = (now.replace(day=1) + datetime.timedelta(days=32)).strftime("%B").upper()
        return (
            f"TODAY: {now.strftime('%d %B %Y')} | "
            f"CURRENT MONTH: {month_name} ({month_num}) | "
            f"CURRENT YEAR: {year} | "
            f"NEXT MONTH: {next_month}"
        )

    def _llm_parse_forced(self, text: str, forced_action: str, *, parent_text: str = "", child_text: str = "", is_reply: bool = False) -> dict | None:
        """LLM extracts symbol fields only; action is fixed (no BUY)."""
        if not self.api_key:
            return None
        date_ctx = self._current_date_context()
        symbol_hint = self._build_symbol_hint(f"{parent_text} {child_text} {text}")
        system = f"""
You extract structured fields from Indian F&O trading messages. The action is FIXED: "{forced_action}".

{date_ctx}

Return JSON only with these keys:
  official_symbol  — underlying ticker root only (e.g. "WAAREEENER", "SENSEX"). NO strike or CE/PE in this field.
  stock            — same as official_symbol (will be overwritten by symbol factory)
  strike           — numeric string only (e.g. "3500"). Empty string if unknown.
  option_type      — "CE" or "PE" only. Empty string if unknown.
  expiry_day       — two-digit day string (e.g. "28"). Use "01" if unknown.
  expiry_month     — three-letter month abbreviation (e.g. "APR"). Use current month if not stated.
  expiry_year      — four-digit year string (e.g. "2026"). Use current year if not stated.
  entry_range      — array of two numbers [min, max]. Use [0, 0] if unknown.
  stop_loss        — number. For {forced_action} use the new SL value if present, else 0.
  target           — number or 0.
  is_trade         — true if an option symbol can be resolved from parent_message or child_message, else false.
  action           — must be exactly "{forced_action}".

Rules:
- official_symbol must be the bare underlying root. Never include strike or CE/PE in it.
- Use parent_message to resolve the instrument when child_message only contains an update (e.g. "TSL 65" or "TSL TO 65").
- TSL / TSL TO / NEW TSL / TRAIL SL / MOVE SL all indicate a trailing stop update.
- Never set action to BUY.
- Stock options with no explicit expiry: use current month expiry (expiry_month = current month, expiry_day = "01").
- Index options with no explicit expiry, use current week's expiry day per index:
    NIFTY → Tuesday | FINNIFTY → Tuesday | MIDCPNIFTY → Monday
    BANKNIFTY → Thursday | SENSEX → Thursday | BANKEX → Monday
{symbol_hint}"""
        user_payload = self._build_llm_payload(parent_text, child_text, is_reply, text)
        data = self._call_openai([
            {"role": "system", "content": system},
            {"role": "user", "content": user_payload},
        ])
        if not isinstance(data, dict):
            logger.error("AI forced-parse error: empty/invalid model response")
            return None
        return data

    @staticmethod
    def _coerce_message_parts(message) -> tuple[str, str, bool, str]:
        """
        Returns (full_text, parent_text, is_reply, child_text).
        Backward compatible with plain text input.
        """
        if isinstance(message, dict):
            child = str(message.get("child_text", "") or "")
            parent = str(message.get("parent_text", "") or "")
            is_reply = bool(message.get("is_reply"))
            full = str(message.get("full_text", "") or "")
            if not full:
                full = f"[CONTEXT: {parent}] {child}".strip() if is_reply else child
            return full, parent, is_reply, child
        text = str(message or "")
        return text, "", False, text

    def parse_signal(self, message):
        """
        Takes a raw Telegram message and returns a structured dictionary if it's a trade.
        """
        logger.info("[STAGE 2] AI parser started.")
        text, parent_text, is_reply, child_text = self._coerce_message_parts(message)

        context_inner, tail = self._split_context_and_tail(text)
        if is_reply and parent_text and not context_inner.strip():
            context_inner = parent_text
        if is_reply and child_text and not tail.strip():
            tail = child_text
        has_context = bool(context_inner.strip()) or is_reply

        # Symbol hint
        hint_text = f"{context_inner} {tail}".strip() or text
        symbol_hint_preview = self._build_symbol_hint(hint_text).strip()
        if symbol_hint_preview:
            logger.info(f"[STAGE 2] Symbol hint: {symbol_hint_preview}")
        else:
            logger.info("[STAGE 2] Symbol hint: (none matched)")

        if has_context and self._is_ack_only_tail(tail):
            logger.info("[STAGE 2] Decision: IGNORE — reply is ack-only tail.")
            return None

        # Typo correction: "23800 PE TYPO" on a reply should amend parent instrument.
        if has_context and tail.strip():
            strike_fix = self._detect_typo_correction(tail)
            if strike_fix:
                corrected_strike, corrected_opt = strike_fix
                logger.info(
                    f"[STAGE 2] Typo correction detected: strike={corrected_strike} type={corrected_opt}."
                )
                data = self._llm_parse_forced(
                    context_inner or parent_text,
                    "BUY",
                    parent_text=context_inner or parent_text,
                    child_text=tail or child_text,
                    is_reply=has_context,
                )
                if data and data.get("is_trade"):
                    data["strike"] = corrected_strike
                    if corrected_opt in ("CE", "PE"):
                        data["option_type"] = corrected_opt
                    parent_forced = self._detect_forced_intent(
                        context_inner or parent_text, context_inner or parent_text
                    )
                    corrected_action = parent_forced[0] if parent_forced else (data.get("action") or "BUY")
                    data["action"] = corrected_action
                    if corrected_action in ("TRAIL_SL", "RESTORE_SL", "EXIT_ALL"):
                        data["entry_range"] = [0, 0]
                    data, reason = self._validate_intent(
                        data,
                        child_text=tail or child_text,
                        parent_text=context_inner or parent_text,
                        is_reply=has_context,
                        forced_action=corrected_action,
                    )
                    if not data:
                        logger.info(f"[STAGE 2] Typo-corrected signal rejected by validator: {reason}")
                        return None
                    data = self._apply_alias_to_data(data)
                    data = self._apply_symbol_factory(data)
                    logger.info(
                        f"[STAGE 2] Signal ready (typo-corrected): {data['action']} "
                        f"{data.get('stock')} strike={data.get('strike')} {data.get('option_type')}"
                    )
                    return data
                logger.info("[STAGE 2] Typo correction: could not resolve parent instrument — ignoring.")
                return None

        # Expiry correction: "This is April Expiry not March."
        # Re-parse the parent signal with the corrected month and inherit its action.
        if has_context and tail.strip():
            corrected_month = self._detect_expiry_correction(tail)
            if corrected_month:
                logger.info(f"[STAGE 2] Expiry correction detected: month overridden to {corrected_month}.")
                data = self._llm_parse_forced(
                    context_inner or parent_text,
                    "TRAIL_SL",
                    parent_text=context_inner or parent_text,
                    child_text=tail or child_text,
                    is_reply=has_context,
                )
                if data and data.get("is_trade"):
                    # Override the month with the corrected value; keep day/year from parent
                    data["expiry_month"] = corrected_month
                    # Inherit action from parent if it contains a known action, else default TRAIL_SL
                    parent_forced = self._detect_forced_intent(context_inner or parent_text, context_inner or parent_text)
                    if parent_forced:
                        data["action"] = parent_forced[0]
                        if parent_forced[0] == "TRAIL_SL" and parent_forced[1] is not None:
                            data["stop_loss"] = parent_forced[1]
                    else:
                        # Parent has no explicit exit/trail — treat as a corrected BUY re-signal
                        data["action"] = data.get("action") or "BUY"
                    logger.info(f"[STAGE 2] Expiry-corrected signal: action={data['action']} month={corrected_month} sl={data.get('stop_loss')}")
                    data, reason = self._validate_intent(
                        data,
                        child_text=tail or child_text,
                        parent_text=context_inner or parent_text,
                        is_reply=has_context,
                        forced_action=data.get("action"),
                    )
                    if not data:
                        logger.info(f"[STAGE 2] Expiry-corrected signal rejected by validator: {reason}")
                        return None
                    data = self._apply_alias_to_data(data)
                    data = self._apply_symbol_factory(data)
                    logger.info(f"[STAGE 2] Signal ready (expiry-corrected): {data['action']} {data['stock']} sl={data.get('stop_loss')}")
                    return data
                else:
                    logger.info("[STAGE 2] Expiry correction: could not resolve instrument from parent — ignoring.")
                    return None

        # Target-update detection: must be a reply and tail must match a known phrase.
        # Runs BEFORE exit/trail detection to prevent the LLM ignoring it as commentary.
        if has_context and tail.strip():
            if self._has_target_hit_phrase(tail) and not self._detect_forced_intent(tail, text):
                logger.info("[STAGE 2] Deterministic intent: RECONCILE (target hit without exit/trail).")
                data = self._llm_parse_forced(
                    text,
                    "TRAIL_SL",
                    parent_text=context_inner or parent_text,
                    child_text=tail or child_text,
                    is_reply=has_context,
                )
                if data and data.get("is_trade"):
                    data["action"] = "RECONCILE"
                    data["stop_loss"] = 0
                    data["target"] = 0
                    data = self._apply_alias_to_data(data)
                    data = self._apply_symbol_factory(data)
                    logger.info(f"[STAGE 2] Signal ready: RECONCILE {data.get('stock')} (target-hit reply).")
                    return data
                logger.info("[STAGE 2] RECONCILE: could not resolve instrument from parent — ignoring.")
                return None

            update_tgt_price = self._detect_update_tgt(tail)
            if update_tgt_price is not None:
                target_leg_index = self._extract_update_tgt_leg_index(tail)
                logger.info(
                    f"[STAGE 2] Deterministic intent: UPDATE_TGT @ {update_tgt_price} "
                    f"(target_leg_index={target_leg_index or 'ALL'})"
                )
                data = self._llm_parse_forced(
                    text,
                    "BUY",
                    parent_text=context_inner or parent_text,
                    child_text=tail or child_text,
                    is_reply=has_context,
                )
                if data and data.get("is_trade"):
                    data["action"] = "UPDATE_TGT"
                    data["target"] = update_tgt_price
                    data["target_leg_index"] = int(target_leg_index or 0)
                    data = self._apply_alias_to_data(data)
                    data = self._apply_symbol_factory(data)
                    logger.info(
                        f"[STAGE 2] Signal ready: UPDATE_TGT {data.get('stock')} "
                        f"new_target={update_tgt_price} target_leg_index={data.get('target_leg_index', 0) or 'ALL'}"
                    )
                    return data
                else:
                    logger.info("[STAGE 2] UPDATE_TGT: could not resolve instrument from parent — ignoring.")
                    return None

        # Range-extension detection: must be a reply and tail must match a known phrase.
        # This runs BEFORE exit/trail detection to prevent misclassification as TRAIL_SL.
        if has_context and tail.strip():
            extend_price = self._detect_range_extend(tail)
            if extend_price is not None:
                logger.info(f"[STAGE 2] Deterministic intent: EXTEND_RANGE @ {extend_price}")
                data = self._llm_parse_forced(
                    text,
                    "BUY",
                    parent_text=context_inner or parent_text,
                    child_text=tail or child_text,
                    is_reply=has_context,
                )
                if data and data.get("is_trade"):
                    data["action"] = "EXTEND_RANGE"
                    data["entry_range"] = [data.get("entry_range", [0, 0])[0], extend_price]
                    data = self._apply_alias_to_data(data)
                    data = self._apply_symbol_factory(data)
                    logger.info(
                        f"[STAGE 2] Signal ready: EXTEND_RANGE {data.get('stock')} "
                        f"new_max={extend_price} range={data.get('buy_range_min')}-{data.get('buy_range_max')}"
                    )
                    return data
                else:
                    logger.info("[STAGE 2] EXTEND_RANGE: could not resolve instrument from parent — ignoring.")
                    return None

        intent_scan_tail = tail if tail.strip() else text
        forced = self._detect_forced_intent(intent_scan_tail, text)
        logger.info(f"[STAGE 2] Deterministic intent: {forced[0] if forced else 'none'}")
        if has_context and forced and not self._tail_has_explicit_exit_or_trail(tail):
            logger.info("[STAGE 2] Decision: drop forced intent — tail has no explicit exit/trail phrase.")
            forced = None

        if forced and forced[0] == "EXIT_ALL":
            logger.info("[STAGE 2] Path: forced EXIT_ALL — calling LLM for symbol extraction only.")
            data = self._llm_parse_forced(
                text,
                "EXIT_ALL",
                parent_text=context_inner or parent_text,
                child_text=tail or child_text,
                is_reply=has_context,
            )
            logger.info(f"[STAGE 2] LLM raw (forced): {data}")
            if not data or not data.get("is_trade"):
                logger.info("[STAGE 2] Decision: IGNORE — forced EXIT_ALL could not resolve instrument.")
                return None
            data["action"] = "EXIT_ALL"
            data, reason = self._validate_intent(
                data,
                child_text=tail or child_text,
                parent_text=context_inner or parent_text,
                is_reply=has_context,
                forced_action="EXIT_ALL",
            )
            if not data:
                logger.info(f"[STAGE 2] Decision: IGNORE forced EXIT_ALL — validator rejected ({reason}).")
                return None
            data = self._apply_alias_to_data(data)
            data = self._apply_symbol_factory(data)
            logger.info(f"[STAGE 2] Signal ready: {data['action']} {data['stock']} @ {data['buy_range_min']}-{data['buy_range_max']}")
            return data

        if forced and forced[0] == "RESTORE_SL":
            # Broadcast re-arm phrase without symbol/reply context:
            # route as RESTORE_ALL over current active book.
            if not has_context and not self._has_option_hint(text):
                logger.info("[STAGE 2] Signal ready: RESTORE_ALL (broadcast restore phrase without symbol).")
                return {
                    "is_trade": True,
                    "action": "RESTORE_ALL",
                    "official_symbol": "",
                    "stock": "ALL",
                    "trading_symbol": "ALL",
                    "strike": "",
                    "option_type": "",
                    "expiry_day": "",
                    "expiry_month": "",
                    "expiry_year": "",
                    "entry_range": [0, 0],
                    "buy_range_min": 0.0,
                    "buy_range_max": 0.0,
                    "stop_loss": 0,
                    "target": 0,
                }
            logger.info("[STAGE 2] Path: forced RESTORE_SL — calling LLM for symbol extraction only.")
            data = self._llm_parse_forced(
                text,
                "EXIT_ALL",
                parent_text=context_inner or parent_text,
                child_text=tail or child_text,
                is_reply=has_context,
            )
            logger.info(f"[STAGE 2] LLM raw (forced): {data}")
            if not data or not data.get("is_trade"):
                logger.info("[STAGE 2] Decision: IGNORE — forced RESTORE_SL could not resolve instrument.")
                return None
            data["action"] = "RESTORE_SL"
            data["stop_loss"] = 0
            data["target"] = 0
            data, reason = self._validate_intent(
                data,
                child_text=tail or child_text,
                parent_text=context_inner or parent_text,
                is_reply=has_context,
                forced_action="RESTORE_SL",
            )
            if not data:
                logger.info(f"[STAGE 2] Decision: IGNORE forced RESTORE_SL — validator rejected ({reason}).")
                return None
            data = self._apply_alias_to_data(data)
            data = self._apply_symbol_factory(data)
            logger.info(f"[STAGE 2] Signal ready: {data['action']} {data['stock']}")
            return data

        if forced and forced[0] == "TRAIL_SL":
            logger.info("[STAGE 2] Path: forced TRAIL_SL — calling LLM for symbol extraction only.")
            data = self._llm_parse_forced(
                text,
                "TRAIL_SL",
                parent_text=context_inner or parent_text,
                child_text=tail or child_text,
                is_reply=has_context,
            )
            logger.info(f"[STAGE 2] LLM raw (forced): {data}")
            if not data or not data.get("is_trade"):
                logger.info("[STAGE 2] Decision: IGNORE — forced TRAIL_SL could not resolve instrument.")
                return None
            data["action"] = "TRAIL_SL"
            sl = forced[1]
            if sl is not None:
                data["stop_loss"] = sl
            elif not data.get("stop_loss"):
                logger.info("[STAGE 2] Decision: IGNORE — TRAIL_SL detected but no stop price found.")
                return None
            hold_tgt = self._extract_hold_target_price(tail or child_text or text)
            if hold_tgt and hold_tgt > 0:
                data["target"] = hold_tgt
                logger.info(f"[STAGE 2] TRAIL_SL companion target detected from HOLD phrase: {hold_tgt}")
            logger.info(f"[STAGE 2] Trail SL price: {data.get('stop_loss')}")
            data, reason = self._validate_intent(
                data,
                child_text=tail or child_text,
                parent_text=context_inner or parent_text,
                is_reply=has_context,
                forced_action="TRAIL_SL",
            )
            if not data:
                logger.info(f"[STAGE 2] Decision: IGNORE forced TRAIL_SL — validator rejected ({reason}).")
                return None
            data = self._apply_alias_to_data(data)
            data = self._apply_symbol_factory(data)
            logger.info(f"[STAGE 2] Signal ready: {data['action']} {data['stock']} @ sl={data.get('stop_loss')}")
            return data

        date_ctx = self._current_date_context()
        symbol_hint = self._build_symbol_hint(f"{context_inner or parent_text} {tail or child_text} {text}")
        system_prompt = f"""
You are an Indian F&O trading signal parser. Extract structured trade parameters from the input JSON and return a JSON object.

{date_ctx}

The input has these fields:
  is_reply        — true if this is a reply to an earlier signal
  parent_message  — the original signal being replied to (use for instrument resolution)
  child_message   — the new message / reply (use this to determine intent/action)

════════════════════════════════════════
OUTPUT SCHEMAS
════════════════════════════════════════

SINGLE LEG (BUY / SELL / EXIT_ALL / TRAIL_SL / RESTORE_SL):
{{
    "is_trade": true,
    "is_spread": false,
    "action": "BUY",
    "official_symbol": "WAAREEENER",
    "stock": "WAAREEENER",
    "strike": "3500",
    "option_type": "CE",
    "expiry_day": "28",
    "expiry_month": "APR",
    "expiry_year": "2026",
    "entry_range": [38.0, 42.0],
    "stop_loss": 22.0,
    "target": 60.0
}}

TRAIL SL example (reply updating stop loss):
{{
    "is_trade": true,
    "is_spread": false,
    "action": "TRAIL_SL",
    "official_symbol": "WAAREEENER",
    "stock": "WAAREEENER",
    "strike": "3500",
    "option_type": "CE",
    "expiry_day": "28",
    "expiry_month": "APR",
    "expiry_year": "2026",
    "entry_range": [0, 0],
    "stop_loss": 40.0,
    "target": 0
}}

RESTORE SL example (reply restoring broker OCO SL back to the channel/admin true SL):
{{
    "is_trade": true,
    "is_spread": false,
    "action": "RESTORE_SL",
    "official_symbol": "WAAREEENER",
    "stock": "WAAREEENER",
    "strike": "3500",
    "option_type": "CE",
    "expiry_day": "28",
    "expiry_month": "APR",
    "expiry_year": "2026",
    "entry_range": [0, 0],
    "stop_loss": 0,
    "target": 0
}}

EXTEND RANGE example (reply widening the buy entry range — NOT a stop loss update):
  Parent: "BUY #NTPC 385 CE APR AT 5-5.60 | SL: 1.5 | TGT: 7.95"
  Child:  "CAN BUY TILL 5.85"  /  "BUY UP TO 6"  /  "ENTRY VALID UPTO 6.10"
{{
    "is_trade": true,
    "is_spread": false,
    "action": "EXTEND_RANGE",
    "official_symbol": "NTPC",
    "stock": "NTPC",
    "strike": "385",
    "option_type": "CE",
    "expiry_day": "01",
    "expiry_month": "APR",
    "expiry_year": "2026",
    "entry_range": [5.0, 5.85],
    "stop_loss": 1.5,
    "target": 7.95
}}

UPDATE TARGET example (reply changing the profit target — NOT a stop loss update):
  Parent: "BUY #NTPC 385 CE APR AT 5-5.60 | SL: 1.5 | TARGETS: 7.95, 11.50"
  Child:  "CHANGE T1 TO 7.45"  /  "NEW TARGET 8"  /  "UPDATE TGT TO 9.50"
{{
    "is_trade": true,
    "is_spread": false,
    "action": "UPDATE_TGT",
    "official_symbol": "NTPC",
    "stock": "NTPC",
    "strike": "385",
    "option_type": "CE",
    "expiry_day": "01",
    "expiry_month": "APR",
    "expiry_year": "2026",
    "entry_range": [0, 0],
    "stop_loss": 0,
    "target": 7.45
}}

EXIT example:
{{
    "is_trade": true,
    "is_spread": false,
    "action": "EXIT_ALL",
    "official_symbol": "SENSEX",
    "stock": "SENSEX",
    "strike": "76500",
    "option_type": "CE",
    "expiry_day": "09",
    "expiry_month": "APR",
    "expiry_year": "2026",
    "entry_range": [0, 0],
    "stop_loss": 0,
    "target": 0
}}

SPREAD / MULTI-LEG (only when child_message explicitly says "FIRST BUY ... THEN SELL" or contains two distinct strikes with buy+sell):
{{
    "is_trade": true,
    "is_spread": true,
    "legs": [
        {{
            "official_symbol": "SENSEX",
            "stock": "SENSEX",
            "strike": "77400",
            "option_type": "CE",
            "expiry_day": "09",
            "expiry_month": "APR",
            "expiry_year": "2026",
            "action": "BUY",
            "entry_range": [280, 300],
            "stop_loss": 0,
            "target": 0
        }},
        {{
            "official_symbol": "SENSEX",
            "stock": "SENSEX",
            "strike": "77900",
            "option_type": "CE",
            "expiry_day": "09",
            "expiry_month": "APR",
            "expiry_year": "2026",
            "action": "SELL",
            "entry_range": [95, 115],
            "stop_loss": 0,
            "target": 0
        }}
    ]
}}

NOT A TRADE (return this for commentary, status, recap, acknowledgements):
{{
    "is_trade": false
}}

════════════════════════════════════════
FIELD RULES
════════════════════════════════════════

official_symbol / stock:
  - Bare underlying ticker root ONLY (e.g. "WAAREEENER", "SENSEX", "LTF", "NMDC").
  - NEVER include strike price or CE/PE in this field.
  - Prefer the F&O/broker symbol root, not NSE cash-market name variants.

strike:
  - Numeric digits only as a string (e.g. "3500", "76500"). Empty string if unknown.

option_type:
  - Exactly "CE" or "PE". Empty string if unknown.

expiry_day:
  - Two-digit string (e.g. "09", "28"). Use "01" if not stated.

expiry_month:
  - Three-letter abbreviation (e.g. "APR", "MAY").
  - Index options: use the calendar expiry date stated in the message.
    If no expiry stated, use the current week's expiry based on this schedule:
      NIFTY → Tuesday | FINNIFTY → Tuesday | MIDCPNIFTY → Monday
      BANKNIFTY → Thursday | SENSEX → Thursday | BANKEX → Monday
  - Stock options with NO expiry stated: use current month (expiry_month = current month, expiry_day = "01").

expiry_year:
  - Four-digit string (e.g. "2026"). Use current year if not stated.

entry_range:
  - Array [min, max] as numbers.
  - "CMP 38-42", "AT 38-42", "@ 38-42", "38 TO 42" all map to [38.0, 42.0].
  - Use [0, 0] for EXIT_ALL, TRAIL_SL, and RESTORE_SL.

stop_loss:
  - Number. For TRAIL_SL use the NEW stop value from child_message (e.g. "TSL 40" / "TSL TO 40" → 40.0).
  - For RESTORE_SL use 0 (the bot restores using persisted sl_true in active_trades.json).
  - "SL 22" → 22.0. Use 0 if not stated.

target:
  - First target number if multiple given (e.g. "TGT 60/75/100+" → 60.0). Use 0 if not stated.

action:
  - "BUY"          — fresh long entry signal.
  - "SELL"         — fresh short entry signal.
  - "EXIT_ALL"     — close position. Triggered by: CLOSE THE TRADE, TSL GONE, TSL HIT, SL HIT,
                      STOPLOSS HIT, BOOK PROFIT, SQUARE OFF, EXIT TRADE, FULL EXIT, STOPPED OUT.
  - "TRAIL_SL"     — update stop loss. Triggered by: TSL, TSL TO, NEW TSL, TRAIL SL, MOVE SL, SHIFT SL,
                      SL TO, TRAILING SL. Extract new SL value from child_message.
  - "RESTORE_SL"   — restore broker OCO SL to the channel/admin true SL (persisted as sl_true).
                      Triggered by replies like: RESTORE SL, RESET SL, REVERT SL, TRUE SL, CHANNEL SL.
                      Set entry_range = [0, 0], stop_loss = 0, target = 0.
  - "EXTEND_RANGE" — widen the BUY entry range. Triggered by replies like: CAN BUY TILL X,
                      BUY UP TO X, BUY UPTO X, ENTRY UPTO X, RANGE UP TO X, MAX ENTRY X,
                      ACCEPTABLE TILL X, VALID UPTO X. The price X becomes the new entry_range max.
                      CRITICAL: do NOT classify these as TRAIL_SL. The price is an entry ceiling,
                      not a stop loss. Set entry_range = [original_min, X], stop_loss = original SL.
  - "UPDATE_TGT"  — change the profit target for an existing trade. Triggered by replies like:
                      CHANGE T1 TO X, TARGET X, CHANGE TARGET X, UPDATE TGT TO X, NEW TARGET X, T1 TO X,
                      REVISE TARGET X. Extract the new target price into the target field.
                      Set entry_range = [0, 0], stop_loss = 0.
                      CRITICAL: do NOT classify these as is_trade=false or commentary.
  - For replies: determine action from child_message. Use parent_message only for instrument fields.

════════════════════════════════════════
NON-TRADE RULES (return is_trade: false)
════════════════════════════════════════
Return is_trade: false for:
  - Performance summaries, day recaps, P&L updates (e.g. "65 TO 118 | +53 PTS").
  - Status messages: "ACTIVE STOCK OPTIONS", "SPREAD STATUS", "CARRYING #NMDC...".
  - Acknowledgements: ✅, 👍, "ok", "done", "intact", "worked well", "expectation".
  - URLs, chart links, social media links.
  - Commentary without any actionable instruction.
  - Replies where child_message is only a reaction/comment on the trade outcome.
{symbol_hint}"""

        try:
            user_payload = self._build_llm_payload(
                context_inner or parent_text,
                tail or child_text,
                has_context,
                text,
            )
            logger.info("[STAGE 2] Calling LLM (%s)...", self.model)
            data = self._call_openai([
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ])
            if not isinstance(data, dict):
                logger.error("[STAGE 2] LLM returned empty/invalid response.")
                return None

            logger.info(f"[STAGE 2] LLM raw: is_trade={data.get('is_trade')} action={data.get('action')} "
                        f"symbol={data.get('official_symbol') or data.get('stock')} "
                        f"strike={data.get('strike')} type={data.get('option_type')} "
                        f"expiry={data.get('expiry_day')}/{data.get('expiry_month')}/{data.get('expiry_year')} "
                        f"entry={data.get('entry_range')} sl={data.get('stop_loss')} tgt={data.get('target')}")

            if not data.get("is_trade"):
                logger.info("[STAGE 2] Decision: IGNORE — LLM says not a trade.")
                return None

            source_for_spread = tail if has_context else text
            if data.get("is_spread") and not self._has_spread_cues(source_for_spread):
                logger.info("[STAGE 2] Decision: IGNORE — LLM returned spread but no spread cues in message.")
                return None

            if data.get("is_spread") and data.get("legs"):
                legs_out = []
                for i, leg in enumerate(data["legs"]):
                    if not isinstance(leg, dict):
                        continue
                    leg = dict(leg)
                    leg.setdefault("action", "BUY")
                    leg.setdefault("is_trade", True)
                    if leg.get("is_trade") is False:
                        continue
                    logger.info(f"[STAGE 2] Spread leg {i+1}: action={leg.get('action')} "
                                f"symbol={leg.get('official_symbol')} strike={leg.get('strike')} "
                                f"type={leg.get('option_type')}")
                    leg, reason = self._validate_intent(
                        leg,
                        child_text=tail or child_text,
                        parent_text=context_inner or parent_text,
                        is_reply=has_context,
                        forced_action=None,
                    )
                    if not leg:
                        logger.info(f"[STAGE 2] Spread leg {i+1} dropped by validator: {reason}")
                        continue
                    leg["spread_leg"] = True
                    leg = self._apply_alias_to_data(leg)
                    leg = self._apply_symbol_factory(leg)
                    logger.info(f"[STAGE 2] Spread leg {i+1} resolved: {leg.get('action')} {leg.get('stock')}")
                    legs_out.append(leg)
                if not legs_out:
                    logger.info("[STAGE 2] Decision: IGNORE — spread had no valid legs after validation.")
                    return None
                leg_summary = ", ".join([f"{l['action']} {l['stock']}" for l in legs_out])
                logger.info(f"[STAGE 2] Signal ready: SPREAD ({len(legs_out)} legs) {leg_summary}")
                return {"spread": True, "legs": legs_out}

            # Belt-and-suspenders: deterministic layer wins if reply clearly says exit/trail
            rerun = self._detect_forced_intent(intent_scan_tail, text)
            if rerun and rerun[0] == "EXIT_ALL" and data.get("action") == "BUY":
                logger.info("[STAGE 2] Deterministic override: BUY → EXIT_ALL (explicit exit phrase in tail).")
                data["action"] = "EXIT_ALL"
            elif rerun and rerun[0] == "RESTORE_SL" and data.get("action") == "BUY":
                logger.info("[STAGE 2] Deterministic override: BUY → RESTORE_SL (explicit restore phrase in tail).")
                data["action"] = "RESTORE_SL"
                data["stop_loss"] = 0
                data["target"] = 0
                data["entry_range"] = [0, 0]
            elif rerun and rerun[0] == "TRAIL_SL" and data.get("action") == "BUY":
                logger.info("[STAGE 2] Deterministic override: BUY → TRAIL_SL (explicit trail phrase in tail).")
                data["action"] = "TRAIL_SL"
                if rerun[1] is not None:
                    data["stop_loss"] = rerun[1]

            data, reason = self._validate_intent(
                data,
                child_text=tail or child_text,
                parent_text=context_inner or parent_text,
                is_reply=has_context,
                forced_action=None,
            )
            if not data:
                logger.info(f"[STAGE 2] Decision: IGNORE — validator rejected ({reason}).")
                return None

            logger.info(f"[STAGE 2] Validator passed: {reason}")
            data = self._apply_alias_to_data(data)
            data = self._apply_symbol_factory(data)
            if str(data.get("action") or "").upper() == "BUY":
                # Preserve line boundaries so "TARGETS: ..." parsing does not
                # bleed into the next appended message fragment (which can
                # introduce strike values as fake extra targets).
                ladder_source = "\n".join(
                    [
                        str(text or ""),
                        str(parent_text or ""),
                        str(child_text or ""),
                        str(tail or ""),
                    ]
                )
                signal_targets = self._extract_signal_targets(ladder_source)
                logger.info(f"[STAGE 2] Parsed ladder targets from message: {signal_targets}")
                if len(signal_targets) >= 2:
                    data["targets"] = signal_targets[:3]
                    data["target"] = float(signal_targets[0])
                    logger.info(f"[STAGE 2] Applied multi-target ladder: {data['targets']}")

            logger.info(f"[STAGE 2] Signal ready: {data['action']} {data['stock']} "
                        f"strike={data.get('strike')} {data.get('option_type')} "
                        f"expiry={data.get('expiry_day')}/{data.get('expiry_month')}/{data.get('expiry_year')} "
                        f"@ {data['buy_range_min']}-{data['buy_range_max']} "
                        f"sl={data.get('stop_loss')} tgt={data.get('target')} "
                        f"trading_symbol={data.get('trading_symbol')}")
            return data

        except Exception as e:
            logger.error(f"[STAGE 2] AI Connection Error: {str(e)}", exc_info=True)
            return None

    def parse_signal_as_event(self, message) -> TradeSignal | None:
        """
        Architecture-aligned entry point: returns a typed TradeSignal event object.
        Internally calls parse_signal() and wraps the result.
        """
        _, parent_text, is_reply, child_text = self._coerce_message_parts(message)
        raw = self.parse_signal(message)
        if raw is None:
            return None
        override_mode, override_reason = self._detect_size_override_mode(
            child_text=child_text,
            parent_text=parent_text,
        )
        if override_mode:
            raw["size_override_mode"] = override_mode
            raw["size_override_reason"] = override_reason
            if raw.get("spread") and isinstance(raw.get("legs"), list):
                for leg in raw["legs"]:
                    if isinstance(leg, dict):
                        leg["size_override_mode"] = override_mode
                        leg["size_override_reason"] = override_reason
        return TradeSignal.from_dict(
            raw,
            raw_child=child_text,
            raw_parent=parent_text,
            is_reply=is_reply,
        )

    @classmethod
    def _detect_size_override_mode(cls, *, child_text: str, parent_text: str) -> tuple[str, str]:
        text = f"{child_text or ''} {parent_text or ''}"
        n = cls._normalize_for_match(text)
        if not n:
            return "", ""
        for phrase in cls._SIZE_OVERRIDE_FIXED_PHRASES:
            if phrase in n:
                return "FIXED", phrase
        return "", ""
