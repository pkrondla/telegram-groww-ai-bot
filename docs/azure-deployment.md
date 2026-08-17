# Azure Container Apps Deployment

This bot is an always-on Python process, so the minimum-cost Azure shape is one
Azure Container App replica with Azure Files mounted at `/data` for durable state.

## Resources

- Azure Container App: runs `python -m app.main` from the Docker image.
- Azure Files share: mounted at `/data`.
- SQLite database: `/data/trading_bot.db`.
- Telethon session: active copy at `/tmp/session_name.session`, durable copy at `/data/session_name.session`.
- GitHub Container Registry: stores the Docker image at no extra cost.

## Naming Standard

Azure resources use this pattern:

```text
<resource-prefix>-<client-code>-<project-code>-<environment>
```

Default production naming:

| Resource | Name |
| --- | --- |
| Subscription | Set by `AZURE_SUBSCRIPTION_ID` |
| Resource group | `rg-techron-growwbot-prod` |
| Container App | `ca-techron-growwbot-prod` |
| Container Apps environment | `cae-techron-growwbot-prod` |
| Storage account | `st<techron-prefix><growwbot-prefix>prod<unique>` |
| Azure Files share | `fs-techron-growwbot-prod` |
| Container Apps storage binding | `afs-techron-growwbot-prod` |

`clientCode`, `projectCode`, and `environment` should be lowercase and short.
Use lowercase letters/numbers only for codes so the generated storage account
name is Azure-compatible.

## GitHub Actions Workflows

The repository has two deployment workflows:

- `.github/workflows/ci.yml` validates Python, runs tests, validates Bicep, builds the Docker image, and publishes the image to GitHub Container Registry on pushes to `main`.
- `.github/workflows/cd-azure.yml` runs after a successful `main` CI build, creates or updates the Azure resource group and Bicep resources, and deploys the CI image to the Container App.

## First Deployment

1. Configure the GitHub repository secrets and variables listed below.

2. Push to `main`, or run `CI - Build and Validate` manually. CI publishes:

   ```text
   ghcr.io/<owner>/<repo>:<commit-sha>
   ghcr.io/<owner>/<repo>:latest
   ```

3. The `CD - Provision Infra and Deploy` workflow provisions Azure and deploys the image. You can also run it manually with an `image_tag` input.

## Manual Local Deployment

Copy the parameter template and fill in real values:

```powershell
Copy-Item infra/azure/main.parameters.example.json infra/azure/main.parameters.json
```

Do not commit `main.parameters.json`; it contains secrets.

Deploy the Azure resources:

```powershell
az account set --subscription "<subscription-id>"
az group create --name rg-techron-growwbot-prod --location eastus
az deployment group create `
  --resource-group rg-techron-growwbot-prod `
  --template-file infra/azure/main.bicep `
  --parameters "@infra/azure/main.parameters.json"
```

Confirm the app has one active replica:

```powershell
az containerapp replica list `
  --resource-group rg-techron-growwbot-prod `
  --name ca-techron-growwbot-prod `
  --output table
```

## Telethon Session Bootstrap

Telethon requires an authenticated `.session` file. For the first Azure
bootstrap, generate a session file while no other bot instance is using that
same session, then upload it to the Azure Files share as `session_name.session`.
After upload, do not keep using that local copy.

Do not run your local bot with the same session file after uploading it. Telegram
invalidates a Telethon auth key when the same `.session` file is used from two
different IP addresses at the same time. Keep local development on a separate
session, for example:

```text
TELEGRAM_SESSION_NAME=session_local
SESSION_PATH=data/session_local
```

The Azure app uses `SESSION_PATH=/tmp/session_name` with
`SESSION_PERSISTENT_PATH=/data/session_name`, so Telethon reads and writes the
local `/tmp/session_name.session` file while the app copies it to/from Azure
Files on startup and clean shutdown.

## Environment Migration

Most runtime values from `.env.example` are passed to Azure Container Apps as
environment variables. Credential-like values are stored as Container App
secrets; operational toggles are stored as plain Container App environment
variables. Session paths are environment-specific so local and Azure sessions do
not share the same Telethon auth key.

Secret-backed variables:

| Environment variable | Secret name |
| --- | --- |
| `TELEGRAM_API_ID` | `telegram-api-id` |
| `TELEGRAM_API_HASH` | `telegram-api-hash` |
| `TELEGRAM_TARGET_CHAT_ID` | `telegram-target-chat-id` |
| `TELEGRAM_ADMIN_ID` | `telegram-admin-id` |
| `TELEGRAM_BOT_TOKEN` | `telegram-bot-token` |
| `OPENAI_API_KEY` | `openai-api-key` |
| `GROWW_ACCOUNTS_JSON` | `groww-accounts-json` |

Plain environment variables:

```text
DB_PATH=/data/trading_bot.db
SESSION_PATH=/tmp/session_name
SESSION_PERSISTENT_PATH=/data/session_name
SQLITE_JOURNAL_MODE=MEMORY
SQLITE_BUSY_TIMEOUT_MS=30000
SQLITE_LOCK_RETRY_SEC=180
TELEGRAM_SESSION_NAME=session_name
OPENAI_MODEL=gpt-4o-mini
DRY_RUN=True
INDEX_ORDER_LOTS_COUNT=1
STOCK_ORDER_LOTS_COUNT=1
ORDER_QUANTITY=1
TARGET_BUFFER_PCT=0.02
MULTI_TARGET_ENABLED=true
TARGET_SPLIT_2=20:80
TARGET_SPLIT_3=10:20:70
TARGET_PRICE_TICK_SIZE=0.05
INDEX_POSITION_SIZING_MODE=fixed
STOCK_POSITION_SIZING_MODE=fixed
RISK_PCT_PER_TRADE=0.015
MAX_TOTAL_OPEN_RISK_PCT=0.05
INDEX_MIN_LOTS=1
INDEX_MAX_LOTS=15
STOCK_MIN_LOTS=1
STOCK_MAX_LOTS=10
RISK_MODE_ON_ZERO_LOTS=fixed
LOG_LEVEL=INFO
ENTRY_QUEUE_ENABLED=true
ENTRY_QUEUE_POLL_SEC=30
ENTRY_QUEUE_MAX_WAIT_MIN=60
SIZE_ADVISORY_TTL_SEC=900
ENTRY_SINGLE_LIMIT_ORDER=true
CHASE_MIN_RR=1.0
CHASE_RR_TARGET_INDEX=2
ACTIVE_RECONCILE_ENABLED=true
ACTIVE_RECONCILE_POLL_SEC=60
AUTO_REARM_PROTECTION=false
AUTO_REARM_POLL_SEC=60
OCO_PLAN_ENFORCEMENT_MODE=ENFORCE
VOL_SL_SCHEDULE_ENABLED=false
VOL_SL_DROP_SL_TIME_IST=09:10
VOL_SL_REARM_SL_TIME_IST=09:20
VOL_SL_SCHEDULE_POLL_SEC=30
VOL_SL_SKIP_WEEKENDS=true
EOD_SUMMARY_ENABLED=true
EOD_SUMMARY_POLL_SEC=30
EOD_SUMMARY_SKIP_WEEKENDS=true
MGMT_QUEUE_ENABLED=true
MGMT_QUEUE_POLL_SEC=10
MGMT_QUEUE_MAX_RETRIES=20
MGMT_QUEUE_TTL_MIN=360
MARKET_OPEN_TIME_IST=09:15
MARKET_CLOSE_TIME_IST=15:30
BROKER_WINDOW_PREOPEN_MIN=15
BROKER_WINDOW_POSTCLOSE_MIN=15
GROWW_AUTH_POLL_SEC=60
EXTEND_RANGE_ALLOWS_ENTRY=false
```

In GitHub Actions CD, override any plain value by adding a repository variable
with the same name. For example, set `DRY_RUN=False` only when you are ready for
live trading.

No secrets are required at Docker build time. The image should be reusable across
environments; all credentials are injected by Azure Container Apps at runtime.

`DB_PATH` intentionally uses `/tmp` on Azure Container Apps. Azure Files is
SMB-backed and can hold SQLite locks indefinitely, so only the Telethon session is
kept on the `/data` Azure Files mount. Move the trade database to a managed DB
before relying on persisted trade state across restarts.

## GitHub Actions Secrets

The CD workflow needs these repository secrets:

| Secret | Purpose |
| --- | --- |
| `AZURE_CREDENTIALS` | JSON service-principal credentials for `azure/login` |
| `AZURE_SUBSCRIPTION_ID` | Dedicated Azure subscription ID for this environment, if not stored as a variable |
| `TELEGRAM_API_ID` | Telegram API ID |
| `TELEGRAM_API_HASH` | Telegram API hash |
| `TELEGRAM_TARGET_CHAT_ID` | Telegram chat/channel ID to monitor |
| `TELEGRAM_ADMIN_ID` | Telegram admin user ID |
| `TELEGRAM_BOT_TOKEN` | Optional Telegram Bot API token for notifications |
| `OPENAI_API_KEY` | OpenAI API key |
| `GROWW_ACCOUNTS_JSON` | Groww account configuration JSON |
| `GHCR_USERNAME` | GitHub username or org that can read the GHCR package, needed for private packages |
| `GHCR_TOKEN` | GitHub token with `read:packages`, needed for private packages |

The CD workflow also supports these repository variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `AZURE_LOCATION` | `eastus` | Azure deployment region |
| `AZURE_SUBSCRIPTION_ID` | none | Dedicated Azure subscription ID for this environment |
| `CLIENT_CODE` | `techron` | Client code used in resource names |
| `PROJECT_CODE` | `growwbot` | Project code used in resource names |
| `DEPLOY_ENVIRONMENT` | `prod` | Environment code used in resource names |
| `AZURE_RESOURCE_GROUP` | `rg-<client>-<project>-<env>` | Optional resource group override |
| `AZURE_CONTAINER_APP_NAME` | `ca-<client>-<project>-<env>` | Optional Container App name override |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model |
| `DRY_RUN` | `True` | Trading safety toggle |
| Bot runtime variables | `.env.example` defaults | Any non-secret variable listed in Environment Migration |
| `LOG_LEVEL` | `INFO` | Application log level |

If the GHCR package is private, either make the package public or set
`GHCR_USERNAME` and `GHCR_TOKEN`. The token must have `read:packages`.

## Cost Controls

- Keep `minReplicas=1` and `maxReplicas=1`.
- Keep resources at `0.25` CPU and `0.5Gi` memory unless logs show pressure.
- Keep the Azure Files share small; the template starts with a 1 GiB quota.
- Use GitHub Container Registry instead of Azure Container Registry to avoid the
  extra ACR Basic monthly charge.
