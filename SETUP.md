# 🛠️ Setup Guide — AI Stock Market Research Assistant

> Step-by-step record of the environment setup completed before building the capstone pipeline.
> Follow this guide to reproduce the setup from scratch on any Databricks Free Edition workspace.

---

## Prerequisites

- Databricks Free Edition account ([signup](https://databricks.com))
- GitHub account
- Massive Stocks API account ([signup](https://massive.com))

---

## ✅ Step 1 — GitHub Repository

**What we did:**
Created a public GitHub repository to host all project code and notebooks.

**Settings used:**
| Field | Value |
|---|---|
| Owner | `demonjd2026-afk` |
| Repository name | `ai-stock-research-assistant` |
| Visibility | Public |
| Description | AI-powered stock market research assistant built on Databricks — real-time ingestion via Massive Stocks API, semantic RAG over earnings/news with Vector Search, Lakebase + CDF analytics, and an agentic frontend on Databricks Apps. |
| Initialize with | README.md |

**Repo URL:**
```
https://github.com/demonjd2026-afk/ai-stock-research-assistant
```

---

## ✅ Step 2 — LinkedIn Identity Verification

**What we did:**
Verified identity via LinkedIn on the Databricks Free Edition workspace to unlock outbound internet access.

**Why this is required:**
Databricks Free Edition restricts outbound internet by default. Without verification, notebooks cannot call external APIs (e.g. Massive Stocks API). LinkedIn verification unlocks this restriction.

**How to do it:**
1. Log into your Databricks workspace
2. Click **"Verify identity"** in the top-right header
3. Complete the LinkedIn OAuth flow
4. Outbound internet access is unlocked automatically

---

## ✅ Step 3 — Databricks Personal Access Token

**What we did:**
Generated a Databricks Personal Access Token (PAT) for API and Git integration.

**Settings used:**
| Field | Value |
|---|---|
| Name | `capstone-token` |
| Lifetime | 90 days |
| Scope | Other APIs |
| API scope | `all-apis` |

**How to generate:**
1. Go to **Settings → User → Developer**
2. Click **"Generate new token"**
3. Fill in the settings above
4. Copy the `dapi...` token immediately — it is shown only once
5. Store it securely (e.g. local password manager)

> **Note:** `all-apis` scope is flagged as "not recommended" for enterprise environments
> but is appropriate for a personal capstone project where all features are needed.

---

## ✅ Step 4 — Connect GitHub Repo to Databricks

**What we did:**
Linked the GitHub repository to the Databricks workspace using Git Folders (Repos), enabling notebook and file sync directly from the workspace.

**GitHub was already linked via OAuth** (visible under Settings → User → Linked accounts).

**Steps to add the Git folder:**
1. Go to **Workspace** in the left sidebar
2. Click **"Create" → "Git folder"**
3. Fill in:
   ```
   Git repository URL : https://github.com/demonjd2026-afk/ai-stock-research-assistant
   Git provider       : GitHub
   Git folder name    : ai-stock-research-assistant  (auto-filled)
   Sparse checkout    : unchecked
   ```
4. Click **"Create Git folder"**
5. Verify `README.md` is visible inside the folder in Databricks Workspace

> **Sync note:** Databricks Git folders do NOT auto-sync.
> Every time you push to GitHub, manually Pull inside the Git folder in Databricks.

---

## ✅ Step 5 — Massive Stocks API Key

**What we did:**
Registered for a free Massive Stocks API account and obtained the API key.

**API details:**
| Field | Value |
|---|---|
| Provider | [Massive Stocks API](https://massive.com) |
| Tier | Free |
| Key length | 32 characters |
| Usage | Real-time ticks, OHLCV, fundamentals, news articles |

---

## ✅ Step 6 — Databricks Secrets Setup

**What we did:**
Stored the Massive Stocks API key securely in a Databricks Secret Scope so notebooks can access it without hardcoding credentials.

**Secret scope details:**
| Field | Value |
|---|---|
| Scope name | `capstone` |
| Secret key | `massive_api_key` |
| Value | Massive Stocks API key (32 chars) |

**How to store secrets securely (using `getpass`):**

Create a temporary notebook named `setup_secrets` in your personal workspace folder (NOT in the Git repo):

```python
import getpass
from databricks.sdk import WorkspaceClient

# Masked input — key never appears in code or output
api_key = getpass.getpass("Paste Massive API key: ")

w = WorkspaceClient()

# Create scope
try:
    w.secrets.create_scope(scope="capstone")
    print("Scope created")
except Exception:
    print("Scope already exists — continuing")

# Store secret
w.secrets.put_secret(
    scope="capstone",
    key="massive_api_key",
    string_value=api_key
)

# Clear from memory immediately
api_key = None
print("Done — secret stored safely")
```

**Verification cell (separate cell):**
```python
key = dbutils.secrets.get(scope="capstone", key="massive_api_key")
print(f"Secret loaded OK — {len(key)} characters")
# Output: Secret loaded OK — 32 characters ✅
```

**After storing:**
- Delete the `setup_secrets` notebook (Move to Trash)
- The secret persists in Databricks Secrets permanently

**How to access the secret in any notebook going forward:**
```python
api_key = dbutils.secrets.get(scope="capstone", key="massive_api_key")
```

> **Security note:** Never hardcode API keys in notebook cells or commit them to Git.
> Always use `dbutils.secrets.get()` to retrieve secrets at runtime.
> Use `getpass.getpass()` for any interactive one-time secret entry.

---

## ✅ Step 7 — Lakebase Project Created

**What we did:**
Created a Lakebase (Postgres OLTP) project inside Databricks to store all relational data for the application.

**Project settings:**
| Field | Value |
|---|---|
| Project name | `stock-assistant` |
| Type | Autoscaling |
| Branch | `production` (default) |
| Database | `databricks_postgres` |
| Postgres version | 17 |
| Compute | 1 CU, scale to zero |
| Region | AWS (us-east-2) |
| History retention | 7 days |

**How to navigate to Lakebase:**
```
https://YOUR-WORKSPACE-URL/lakebase/projects
```
Or use the direct URL:
```
https://dbc-291b687e-da89.cloud.databricks.com/lakebase/projects
```

---

## ✅ Step 8 — Lakebase Schema Executed

**What we did:**
Ran `lakebase/05_schema_ddl.sql` in the Lakebase SQL Editor to create all 8 relational tables in the `stock_assistant` schema.

**Execution result:**
- 32 statements executed successfully in ~350ms
- 8 tables created and verified

**Tables created and verified:**
| # | Table | Size | Purpose |
|---|---|---|---|
| 1 | `analysis_reports` | 40 kB | Agent-generated reports per session |
| 2 | `companies` | 24 kB | Company profiles and fundamentals |
| 3 | `news_articles` | 32 kB | Raw news text (also embedded for RAG) |
| 4 | `price_snapshots` | 32 kB | OHLCV snapshots per ticker |
| 5 | `research_notes` | 32 kB | User-authored notes per ticker |
| 6 | `users` | 32 kB | Registered users |
| 7 | `watchlist_tickers` | 32 kB | Tickers within each watchlist |
| 8 | `watchlists` | 32 kB | Named watchlists per user |

**How to run the schema:**
1. Navigate to `stock-assistant → production → SQL Editor` in Lakebase
2. Paste contents of `lakebase/05_schema_ddl.sql`
3. Click **Run** (not Explain or Analyze)
4. Run verification query to confirm 8 tables exist:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'stock_assistant'
ORDER BY table_name;
```

> `REPLICA IDENTITY FULL` is set on all tables to enable Change Data Feed (CDF)
> which powers the analytics Delta table in Phase 6.

---

## ⬜ Next Steps — Pipeline Build Phases

```
⬜ Phase 2 — Bronze ingestion pipeline    (pipeline/01_bronze_ingestion.py)
⬜ Phase 3 — Silver transformation        (pipeline/02_silver_transform.py)
⬜ Phase 4 — Gold aggregates              (pipeline/03_gold_aggregates.py)
⬜ Phase 5 — Embeddings + Vector Search   (embeddings/04_embed_and_index.py)
⬜ Phase 6 — Lakebase CDF → Delta         (cdf/06_cdf_to_delta.py)
⬜ Phase 7 — AI Agent with tools          (agent/07_agent_tools.py)
⬜ Phase 8 — Databricks App frontend      (app/app.py)
```

---

## Workspace Reference

| Item | Value |
|---|---|
| Workspace URL | `dbc-291b687e-da89.cloud.databricks.com` |
| Cloud | AWS |
| Edition | Databricks Free Edition |
| Unity Catalog | Enabled (default) |
| Secret scope | `capstone` |
| Secret key | `massive_api_key` |
| Git repo | `demonjd2026-afk/ai-stock-research-assistant` |
| Runtime | Serverless |
| Lakebase project | `stock-assistant` |
| Lakebase branch | `production` |
| Lakebase database | `databricks_postgres` |
| Lakebase schema | `stock_assistant` |

---

## Security Checklist

- [x] No API keys hardcoded in any notebook
- [x] No secrets committed to GitHub
- [x] `setup_secrets` notebook deleted after use
- [x] All secrets accessed via `dbutils.secrets.get()` at runtime
- [x] `getpass` used for interactive secret entry
- [x] `.gitignore` covers any local config files

---

## File Structure (current)

```
ai-stock-research-assistant/
├── README.md                       ✅ Architecture + tech stack overview
├── SETUP.md                        ✅ This file — step-by-step setup guide
└── lakebase/
    └── 05_schema_ddl.sql           ✅ Lakebase table definitions (8 tables)
```

---

*Last updated: August 2026*
