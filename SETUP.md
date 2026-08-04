# 🛠️ Setup Guide — AI Stock Market Research Assistant

> Step-by-step record of the environment setup and pipeline build.
> Follow this guide to reproduce the project from scratch on any Databricks Free Edition workspace.

---

## Prerequisites

- Databricks Free Edition account ([signup](https://databricks.com))
- GitHub account
- Massive Stocks API account ([signup](https://massive.com))

---

## ✅ Step 1 — GitHub Repository

| Field | Value |
|---|---|
| Owner | `demonjd2026-afk` |
| Repository name | `ai-stock-research-assistant` |
| Visibility | Public |
| Repo URL | `https://github.com/demonjd2026-afk/ai-stock-research-assistant` |

---

## ✅ Step 2 — LinkedIn Identity Verification

Databricks Free Edition restricts outbound internet by default. LinkedIn verification unlocks it.

1. Log into Databricks workspace
2. Click **"Verify identity"** in the top-right header
3. Complete the LinkedIn OAuth flow

---

## ✅ Step 3 — Databricks Personal Access Token

| Field | Value |
|---|---|
| Name | `capstone-token` |
| Lifetime | 90 days |
| Scope | Other APIs |
| API scope | `all-apis` |

**How:** Settings → User → Developer → Generate new token

---

## ✅ Step 4 — Connect GitHub Repo to Databricks

1. Workspace → Create → Git folder
2. URL: `https://github.com/demonjd2026-afk/ai-stock-research-assistant`
3. Provider: GitHub (already linked via OAuth)

> Databricks does NOT auto-sync. Pull manually after every GitHub push.

---

## ✅ Step 5 — Massive Stocks API Key

| Field | Value |
|---|---|
| Provider | [Massive Stocks API](https://massive.com) (rebranded from Polygon.io) |
| Tier | Free (5 requests/minute) |
| Key length | 32 characters |
| Working base URL | `https://api.polygon.io` |

> **Note:** `api.massive.com` is blocked by Databricks Free Edition network filter.
> Use `api.polygon.io` — same API key works (Massive rebranded from Polygon.io Oct 2025).

---

## ✅ Step 6 — Databricks Secrets Setup

| Field | Value |
|---|---|
| Scope name | `capstone` |
| Secret key | `massive_api_key` |
| Value | Massive Stocks API key (32 chars) |

**Secure storage using `getpass` (never hardcode):**

```python
import getpass
from databricks.sdk import WorkspaceClient

api_key = getpass.getpass("Paste Massive API key: ")
w = WorkspaceClient()
try:
    w.secrets.create_scope(scope="capstone")
except Exception:
    pass
w.secrets.put_secret(scope="capstone", key="massive_api_key", string_value=api_key)
api_key = None
print("Done")
```

**Access in any notebook:**
```python
api_key = dbutils.secrets.get(scope="capstone", key="massive_api_key")
```

---

## ✅ Step 7 — Lakebase Project Created

| Field | Value |
|---|---|
| Project name | `stock-assistant` |
| Type | Autoscaling |
| Branch | `production` |
| Database | `databricks_postgres` |
| Postgres version | 17 |
| Compute | 1 CU, scale to zero |
| Region | AWS (us-east-2) |

**URL:** `https://dbc-291b687e-da89.cloud.databricks.com/lakebase/projects`

---

## ✅ Step 8 — Lakebase Schema Executed

Run `lakebase/05_schema_ddl.sql` in the Lakebase SQL Editor.

**Tables created (8 total):**

| Table | Size | Purpose |
|---|---|---|
| `analysis_reports` | 40 kB | Agent-generated reports per session |
| `companies` | 24 kB | Company profiles and fundamentals |
| `news_articles` | 32 kB | Raw news text (also embedded for RAG) |
| `price_snapshots` | 32 kB | OHLCV snapshots per ticker |
| `research_notes` | 32 kB | User-authored notes per ticker |
| `users` | 32 kB | Registered users |
| `watchlist_tickers` | 32 kB | Tickers within each watchlist |
| `watchlists` | 32 kB | Named watchlists per user |

> `REPLICA IDENTITY FULL` set on all tables to enable Change Data Feed (CDF).

---

## ✅ Phase 2 — Bronze Ingestion Pipeline

**Files:**
- `pipeline/00_setup_config.ipynb` — ticker registry in Unity Catalog
- `pipeline/01_bronze_ingestion.ipynb` — raw data ingestion

### Ticker Config Table (`main.config.ticker_config`)

Tickers are stored in Unity Catalog — not hardcoded in notebooks.

| Field | Value |
|---|---|
| Table | `main.config.ticker_config` |
| Active tickers | 5 (AAPL, GOOGL, META, MSFT, NVDA) |
| Inactive tickers | 15 (activate anytime via SQL) |

**To activate more tickers:**
```sql
-- All tickers
UPDATE main.config.ticker_config SET active = true

-- One sector
UPDATE main.config.ticker_config SET active = true WHERE sector = 'Finance'

-- One ticker
UPDATE main.config.ticker_config SET active = true WHERE ticker = 'JPM'
```

### Bronze Delta Tables (`main.bronze`)

| Table | Rows (first run) | Source Endpoint |
|---|---|---|
| `raw_companies` | 5 | `GET /v3/reference/tickers/{ticker}` |
| `raw_price_snapshots` | 5 | `GET /v2/aggs/ticker/{ticker}/prev` |
| `raw_news_articles` | 50 | `GET /v2/reference/news` |

**Sample data confirmed:**

| Ticker | Close | Market Cap |
|---|---|---|
| AAPL | $303.42 | $4.43T |
| NVDA | $206.64 | $5.00T |
| MSFT | $487.65 | $3.62T |

**Key design decisions:**
- `raw_json` column stores full API response — nothing lost at Bronze layer
- `batch_id` on every row enables lineage tracking per run
- `mode("append")` — Bronze is immutable, never overwrites
- `mergeSchema("true")` — handles API schema changes gracefully
- 13-second sleep between API calls — respects free tier rate limit (5 req/min)
- All numeric fields explicitly cast to prevent Spark type inference errors

### Known Issues & Fixes

| Issue | Cause | Fix |
|---|---|---|
| `api.massive.com` returns 404 | Blocked by Databricks Free Edition network filter | Use `api.polygon.io` instead |
| `WRONG_COLUMN_DEFAULTS_FOR_DELTA` | Delta column defaults not enabled on Free Edition | Removed `DEFAULT` from DDL |
| `CANNOT_MERGE_TYPE DoubleType/LongType` | Spark infers mixed types from API response | Explicit `float()`/`int()` casts on all numeric fields |

---

## ⬜ Next Steps

```
⬜ Phase 3 — Silver transformation    (pipeline/02_silver_transform.ipynb)
⬜ Phase 4 — Gold aggregates          (pipeline/03_gold_aggregates.ipynb)
⬜ Phase 5 — Embeddings + Vector Search (embeddings/04_embed_and_index.ipynb)
⬜ Phase 6 — Lakebase CDF → Delta     (cdf/06_cdf_to_delta.ipynb)
⬜ Phase 7 — AI Agent with tools      (agent/07_agent_tools.ipynb)
⬜ Phase 8 — Databricks App frontend  (app/app.py)
```

---

## Workspace Reference

| Item | Value |
|---|---|
| Workspace URL | `dbc-291b687e-da89.cloud.databricks.com` |
| Cloud | AWS |
| Edition | Databricks Free Edition |
| Unity Catalog | Enabled |
| Secret scope | `capstone` |
| Secret key | `massive_api_key` |
| API base URL | `https://api.polygon.io` |
| Git repo | `demonjd2026-afk/ai-stock-research-assistant` |
| Runtime | Serverless |
| Lakebase project | `stock-assistant` |
| Lakebase schema | `stock_assistant` |

---

## Security Checklist

- [x] No API keys hardcoded in any notebook
- [x] No secrets committed to GitHub
- [x] All secrets via `dbutils.secrets.get()` at runtime
- [x] `getpass` used for interactive secret entry
- [x] `setup_secrets` notebook deleted after use

---

## File Structure

```
ai-stock-research-assistant/
├── .gitignore
├── README.md
├── SETUP.md
├── lakebase/
│   └── 05_schema_ddl.sql           ✅ 8 Lakebase tables
└── pipeline/
    ├── 00_setup_config.ipynb       ✅ Ticker registry (Unity Catalog)
    └── 01_bronze_ingestion.ipynb   ✅ Raw ingestion (Massive/Polygon API)
```

---

*Last updated: August 2026*
