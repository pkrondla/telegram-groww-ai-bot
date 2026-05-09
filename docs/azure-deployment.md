# Azure Container Apps Deployment

This bot is an always-on Python process, so the minimum-cost Azure shape is one
Azure Container App replica with Azure Files mounted at `/data` for durable state.

## Resources

- Azure Container App: runs `python -m app.main` from the Docker image.
- Azure Files share: mounted at `/data`.
- SQLite database: `/data/trading_bot.db`.
- Telethon session: `/data/session_name.session`.
- GitHub Container Registry: stores the Docker image at no extra cost.

## First Deployment

1. Build and push an image to GitHub Container Registry:

   ```powershell
   docker build -t ghcr.io/<owner>/<repo>:latest .
   docker push ghcr.io/<owner>/<repo>:latest
   ```

2. Copy the parameter template and fill in real values:

   ```powershell
   Copy-Item infra/azure/main.parameters.example.json infra/azure/main.parameters.json
   ```

   Do not commit `main.parameters.json`; it contains secrets.

3. Deploy the Azure resources:

   ```powershell
   az group create --name rg-telegram-groww-bot --location eastus
   az deployment group create `
     --resource-group rg-telegram-groww-bot `
     --template-file infra/azure/main.bicep `
     --parameters "@infra/azure/main.parameters.json"
   ```

4. Confirm the app has one active replica:

   ```powershell
   az containerapp replica list `
     --resource-group rg-telegram-groww-bot `
     --name telegram-groww-ai-bot `
     --output table
   ```

## Telethon Session Bootstrap

Telethon requires an authenticated `.session` file. Generate it locally first by
running the bot once with your Telegram credentials, then upload the session file
to the Azure Files share as `session_name.session`.

The app uses `SESSION_PATH=/data/session_name`, so Telethon reads and writes
`/data/session_name.session`.

## Environment Migration

Store all credential-like values as Container App secrets. Store operational
toggles as plain environment variables.

Secret-backed environment variables:

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

| Environment variable | Azure value |
| --- | --- |
| `DB_PATH` | `/data/trading_bot.db` |
| `SESSION_PATH` | `/data/session_name` |
| `TELEGRAM_SESSION_NAME` | `session_name` |
| `OPENAI_MODEL` | `gpt-4o-mini` or your preferred model |
| `DRY_RUN` | `True` until you are ready for live trading |
| `LOG_LEVEL` | `INFO` |
| Trading controls | Copy non-secret values from `.env.example` as needed |

No secrets are required at Docker build time. The image should be reusable across
environments; all credentials are injected by Azure Container Apps at runtime.

## GitHub Actions Secrets

The workflow in `.github/workflows/azure-container-app.yml` needs these repository
secrets:

| Secret | Purpose |
| --- | --- |
| `AZURE_CREDENTIALS` | JSON service-principal credentials for `azure/login` |
| `AZURE_RESOURCE_GROUP` | Resource group containing the Container App |
| `AZURE_CONTAINER_APP_NAME` | Container App name |

If the GHCR package is private, either make the package public or set
`registryUsername` and `registryPassword` in the Bicep parameters. The registry
password should be a GitHub token with `read:packages`.

## Cost Controls

- Keep `minReplicas=1` and `maxReplicas=1`.
- Keep resources at `0.25` CPU and `0.5Gi` memory unless logs show pressure.
- Keep the Azure Files share small; the template starts with a 1 GiB quota.
- Use GitHub Container Registry instead of Azure Container Registry to avoid the
  extra ACR Basic monthly charge.
