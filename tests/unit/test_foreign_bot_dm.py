"""Groww Telethon must ignore DMs with other bots (e.g. ORB /help)."""
from __future__ import annotations

import os
import sys
import unittest

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "x")
os.environ.setdefault("TELEGRAM_TARGET_CHAT_ID", "-100123")
os.environ.setdefault("TELEGRAM_ADMIN_ID", "1447109810")
os.environ.setdefault("OPENAI_API_KEY", "x")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.integrations.telegram.listener import (  # noqa: E402
    bot_user_id_from_token,
    is_foreign_bot_private_chat,
)


GROWW_BOT_ID = 8111111111
ORB_BOT_ID = 8222222222
ADMIN_ID = 1447109810
TARGET = -100123


class TestForeignBotDm(unittest.TestCase):
    def test_token_prefix_is_bot_user_id(self):
        self.assertEqual(bot_user_id_from_token(f"{GROWW_BOT_ID}:secret"), GROWW_BOT_ID)
        self.assertIsNone(bot_user_id_from_token(""))
        self.assertIsNone(bot_user_id_from_token(None))

    def test_orb_dm_is_ignored(self):
        self.assertTrue(
            is_foreign_bot_private_chat(
                chat_id=ORB_BOT_ID,
                is_private=True,
                is_group=False,
                is_channel=False,
                own_bot_user_id=GROWW_BOT_ID,
                admin_id=ADMIN_ID,
                target_chat_id=TARGET,
                chat_is_bot=True,
            )
        )

    def test_groww_bot_dm_is_allowed(self):
        self.assertFalse(
            is_foreign_bot_private_chat(
                chat_id=GROWW_BOT_ID,
                is_private=True,
                is_group=False,
                is_channel=False,
                own_bot_user_id=GROWW_BOT_ID,
                admin_id=ADMIN_ID,
                target_chat_id=TARGET,
                chat_is_bot=True,
            )
        )

    def test_saved_messages_and_channel_are_allowed(self):
        self.assertFalse(
            is_foreign_bot_private_chat(
                chat_id=ADMIN_ID,
                is_private=True,
                is_group=False,
                is_channel=False,
                own_bot_user_id=GROWW_BOT_ID,
                admin_id=ADMIN_ID,
                target_chat_id=TARGET,
            )
        )
        self.assertFalse(
            is_foreign_bot_private_chat(
                chat_id=TARGET,
                is_private=False,
                is_group=False,
                is_channel=True,
                own_bot_user_id=GROWW_BOT_ID,
                admin_id=ADMIN_ID,
                target_chat_id=TARGET,
            )
        )


if __name__ == "__main__":
    unittest.main()
