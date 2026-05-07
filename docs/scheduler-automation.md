# Bot Scheduler Automation

This project now includes cross-platform start/stop scripts that are safe for automation.

- Runtime files:
  - `runtime/bot.pid`
  - `runtime/bot.stop`
  - `runtime/bot.log`
- Stop flow:
  - stop script creates `runtime/bot.stop`
  - bot detects this and runs graceful shutdown
  - if it does not exit in time, script force-stops as fallback

## Windows (Task Scheduler)

Use PowerShell scripts:

- Start: `scripts/start_bot_windows.ps1`
- Stop: `scripts/stop_bot_windows.ps1`

Suggested Task Scheduler actions:

- Program/script: `powershell.exe`
- Add arguments (start):
  - `-NoProfile -ExecutionPolicy Bypass -File "C:\PK\Source\telegram-groww-ai-bot\scripts\start_bot_windows.ps1"`
- Add arguments (stop):
  - `-NoProfile -ExecutionPolicy Bypass -File "C:\PK\Source\telegram-groww-ai-bot\scripts\stop_bot_windows.ps1"`

Create two tasks:

- Start task at `08:30 IST` (weekdays)
- Stop task at `15:45 IST` (weekdays)

## Linux (cron)

Make scripts executable once:

```bash
chmod +x /path/to/telegram-groww-ai-bot/scripts/start_bot_linux.sh
chmod +x /path/to/telegram-groww-ai-bot/scripts/stop_bot_linux.sh
```

Add cron entries (weekdays):

```cron
30 8 * * 1-5 /path/to/telegram-groww-ai-bot/scripts/start_bot_linux.sh
45 15 * * 1-5 /path/to/telegram-groww-ai-bot/scripts/stop_bot_linux.sh
```

These times follow the server timezone. Set server timezone to IST if needed.
