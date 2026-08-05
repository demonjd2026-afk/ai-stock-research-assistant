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

### Notebook Logic — `01_bronze_ingestion.ipynb`

The notebook is structured into 8 cells:

**Cell 1 — Imports and config**
Imports `requests`, `json`, `uuid`, `time`, `datetime`. Generates a unique `BATCH_ID` (UUID) per run used for lineage tracking across all three tables. Sets `RUN_DATE` and `INGESTED_AT` timestamps.

**Cell 2 — Load API key**
Loads the Massive API key securely from Databricks Secrets:
```python
API_KEY = dbutils.secrets.get(scope="capstone", key="massive_api_key")
BASE_URL = "https://api.polygon.io"  # api.massive.com blocked on Free Edition
```
Auth is passed as a query parameter `?apiKey=KEY` — not a Bearer header.

**Cell 3 — Load active tickers from Unity Catalog**
Instead of hardcoding tickers, the notebook reads from `main.config.ticker_config`:
```python
TICKERS = [
    row.ticker for row in
    spark.sql("SELECT ticker FROM main.config.ticker_config WHERE active = true ORDER BY ticker")
    .collect()
]
```
This means adding/removing tickers never requires code changes.

**Cell 4 — API helper function**
`api_get(endpoint, params, retries=3)` wraps every API call with:
- Automatic retry (3 attempts)
- 429 rate limit detection → waits 60 seconds then retries
- Timeout of 15 seconds per request
- Returns parsed JSON or `None` on failure

Rate limit sleep of 13 seconds between each ticker call respects the free tier limit of 5 requests/minute.

**Cell 5 — Company fundamentals ingestion**
Calls `GET /v3/reference/tickers/{ticker}` per ticker.
Extracts: name, exchange, market_cap, description, homepage_url, total_employees, list_date, sic_code, sic_description, locale, currency_name, active, type.
Adds `batch_id`, `run_date`, `raw_json` (full API response), `ingested_at`.
Writes to `main.bronze.raw_companies` in **append** mode.

**Cell 6 — OHLCV price snapshots ingestion**
Calls `GET /v2/aggs/ticker/{ticker}/prev` per ticker.
Extracts: open (o), high (h), low (l), close (c), volume (v), vwap (vw), transactions (n), timestamp_ms (t).
**Critical fix:** All numeric fields explicitly cast to `float()` or `int()` to prevent Spark type inference conflicts:
```python
"open"   : float(r["o"]) if r.get("o") is not None else None,
"volume" : float(r["v"]) if r.get("v") is not None else None,
"transactions": int(r["n"]) if r.get("n") is not None else None,
```
Writes to `main.bronze.raw_price_snapshots` in **append** mode.

**Cell 7 — News articles ingestion**
Calls `GET /v2/reference/news` with params: `ticker`, `published_utc.gte` (7 days ago), `order=desc`, `limit=10`.
Extracts: article_id, title, author, published_utc, article_url, description, keywords (as JSON string), publisher_name, sentiment (from insights array).
Writes to `main.bronze.raw_news_articles` in **append** mode.

**Cell 8 — Verification**
Prints row counts per table (this run vs total) and shows 5-row samples from each table to confirm data quality.

### Notebook Logic — `00_setup_config.ipynb`

**Cell 1** — Creates `main.config` schema in Unity Catalog.

**Cell 2** — Creates `main.config.ticker_config` Delta table with columns: ticker, name, sector, active (BOOLEAN), added_at, notes. No DEFAULT values (not supported on Free Edition Delta).

**Cell 3** — Seeds 20 tickers: 5 active (Technology), 15 inactive. Uses `DELETE` then `append` write for idempotency — safe to re-run anytime without duplicates.

**Cell 4** — Displays active and inactive tickers via `spark.sql` for verification.

**Cell 5** — Documents SQL commands to activate tickers (commented out, run as needed).

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
