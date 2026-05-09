"""
Unit tests for Assistant._extract_signal_targets and
Assistant._split_context_and_tail.
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("TELEGRAM_API_ID",       "1")
os.environ.setdefault("TELEGRAM_API_HASH",     "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID","1")
os.environ.setdefault("TELEGRAM_ADMIN_ID",     "1")
os.environ.setdefault("OPENAI_API_KEY",        "x")

from src.integrations.ai.assistant import Assistant  # noqa: E402


class TestExtractSignalTargets:
    def test_tgt_slash_format(self):
        result = Assistant._extract_signal_targets("TGT: 68/88/105")
        assert result == [68.0, 88.0, 105.0]

    def test_targets_comma_format(self):
        result = Assistant._extract_signal_targets("TARGETS: 68, 88")
        assert result == [68.0, 88.0]

    def test_targets_with_plus_suffix(self):
        result = Assistant._extract_signal_targets("TGT : 18/24/36+")
        assert result == [18.0, 24.0, 36.0]

    def test_ignores_rr_multipliers(self):
        # "2X" / "3X" should not be captured as target prices
        result = Assistant._extract_signal_targets("TARGETS : 26, 48 [2X,3X]")
        assert result == [26.0, 48.0]

    def test_empty_text_returns_empty(self):
        assert Assistant._extract_signal_targets("") == []

    def test_no_target_keyword_returns_empty(self):
        assert Assistant._extract_signal_targets("BUY NIFTY 24500 CE SL 68") == []

    def test_deduplication(self):
        result = Assistant._extract_signal_targets("TGT: 68/68/88")
        assert result == [68.0, 88.0]

    def test_multiline_picks_first_match(self):
        text = "some preamble\nTARGETS: 50, 75\nother line"
        result = Assistant._extract_signal_targets(text)
        assert result == [50.0, 75.0]


class TestSplitContextAndTail:
    def test_plain_message_returns_empty_context(self):
        ctx, tail = Assistant._split_context_and_tail("BUY NIFTY CE")
        assert ctx == ""
        assert tail == "BUY NIFTY CE"

    def test_reply_splits_correctly(self):
        text = "[CONTEXT: parent message] child message"
        ctx, tail = Assistant._split_context_and_tail(text)
        assert ctx == "parent message"
        assert tail == "child message"

    def test_no_closing_bracket_falls_back_to_full(self):
        text = "[CONTEXT: unclosed"
        ctx, tail = Assistant._split_context_and_tail(text)
        assert ctx == ""
        assert tail == text

    def test_empty_context_inner(self):
        text = "[CONTEXT: ] child only"
        ctx, tail = Assistant._split_context_and_tail(text)
        assert ctx == ""
        assert tail == "child only"


class TestUpdateTargetDetection:
    def test_detect_update_tgt_accepts_plain_target_price(self):
        assert Assistant._detect_update_tgt("TARGET 52") == 52.0

    def test_detect_update_tgt_ignores_target_hit_progression(self):
        msg = "TARGET 1 HITS | TSL TO 21.5 | HOLD FOR 52"
        assert Assistant._detect_update_tgt(msg) is None
