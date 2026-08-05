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

## ✅ Phase 3 — Silver Transformation

**File:** `pipeline/02_silver_transform.ipynb`

### What Silver Does

Silver reads from Bronze Delta tables, applies cleaning and enrichment transformations, and writes deduplicated, typed, analytics-ready data to `main.silver.*` Delta tables.

**Key design decision:** Lakebase OLTP tables are **not** written by Silver. They are written by the AI Agent tools in Phase 7. Silver only writes to Delta.

### Transformations Per Table

**`main.silver.companies`** (from `main.bronze.raw_companies`):
- Deduplicate by `ticker` — keep latest `ingested_at` per ticker using `ROW_NUMBER()` window function
- Drop `raw_json` and `batch_id` (Bronze-only columns)
- Normalize exchange codes: `XNAS → NASDAQ`, `XNYS → NYSE`, `XASE → AMEX`, `ARCX → NYSE Arca`
- Derive `market_cap_billions` = `market_cap / 1e9` rounded to 2 decimal places
- Add `processed_at` timestamp

**`main.silver.price_snapshots`** (from `main.bronze.raw_price_snapshots`):
- Deduplicate by `(ticker, snapshot_date)` — keep latest per trading day
- Validate: filter rows where `close <= 0`
- Derive `daily_return_pct` = `((close - open) / open) * 100`
- Derive `price_range` = `high - low`
- Derive `is_up_day` = `close >= open` (boolean flag)
- Round all price columns to 4 decimal places
- Add `processed_at` timestamp

**`main.silver.news_articles`** (from `main.bronze.raw_news_articles`):
- Filter: remove rows with null `article_id` or null `title`
- Deduplicate by `article_id` — keep latest ingested version
- Parse `published_utc` string → proper `TimestampType` column `published_ts`
- Derive `article_age_days` = `datediff(current_date, published_utc)`
- Derive `description_length` = character length of description
- Derive `has_sentiment` = boolean flag (True if sentiment is not null)
- Normalize `sentiment` to lowercase
- Add `processed_at` timestamp

### Silver Table Results

| Table | Rows | Notes |
|---|---|---|
| `main.silver.companies` | 20 | All unique tickers ever ingested, deduplicated |
| `main.silver.price_snapshots` | 5 | One row per active ticker, validated |
| `main.silver.news_articles` | 25 | Deduplicated from 50 Bronze rows |

### Notebook Structure (7 cells)

| Cell | Purpose |
|---|---|
| 0 | Imports, SparkSession, `PROCESSED_AT` timestamp |
| 1 | Create `main.silver` schema |
| 2 | Transform + write `main.silver.companies` |
| 3 | Transform + write `main.silver.price_snapshots` |
| 4 | Transform + write `main.silver.news_articles` |
| 5 | Summary row counts + architecture note |

### Known Issues & Architecture Decisions

| Issue | Decision |
|---|---|
| Lakebase psycopg2 connection fails on Free Edition | Removed Lakebase sync from Silver entirely |
| `w.config.token` returns None in Serverless | Token exchange also blocked (no federation policy on Free Edition) |
| Lakebase OLTP needs data | Populated by AI Agent tools (Phase 7) — correct architecture |
| Test user needed for Agent | Run `INSERT INTO stock_assistant.users` in Lakebase SQL Editor |

### Lakebase Seed (run once in Lakebase SQL Editor)

```sql
INSERT INTO stock_assistant.users (name, email)
VALUES ('Jay Dolai', 'jayanthdolai07@gmail.com')
ON CONFLICT (email) DO NOTHING;

SELECT id, name, email, created_at FROM stock_assistant.users;
```

---

## ✅ Phase 4 — Gold Aggregates

**File:** `pipeline/03_gold_aggregates.ipynb`

### What Gold Does

Gold reads from Silver Delta tables and builds analytics-ready aggregated tables in `main.gold.*`. These tables power the AI Agent tools and the Databricks App frontend. No API calls — pure PySpark aggregations running in under 30 seconds.

### Gold Tables Produced

**`main.gold.ticker_daily_summary`**
Master fact table joining Silver price + company + sentiment into one row per ticker per day:
- All price columns (open, high, low, close, volume, vwap)
- Derived: `daily_return_pct`, `price_range`, `is_up_day`
- Company metadata: name, exchange_name, sector, market_cap_billions
- Sentiment metrics: news_count, avg_sentiment_score, positive/negative/neutral counts

**`main.gold.sector_rankings`**
Sector-level aggregation ranked by total market cap:
- `total_market_cap_billions` — sum of market caps in sector
- `avg_daily_return_pct` — mean return across sector tickers
- `avg_sentiment_score` — mean sentiment across sector news
- `sector_rank` — rank by market cap using `RANK()` window function
- `tickers` — array of all ticker symbols in sector

**`main.gold.sentiment_summary`**
Per-ticker sentiment signal with confidence rating:
- `sentiment_signal` — BULLISH (score > 0.3) / BEARISH (score < -0.3) / NEUTRAL
- `sentiment_confidence` — HIGH (≥8 articles) / MEDIUM (≥4) / LOW (<4)

**`main.gold.top_movers`**
Best and worst performing tickers ranked by daily return:
- `return_rank` — rank by `daily_return_pct` descending
- `mover_type` — GAINER (return > 0) / LOSER (return < 0) / FLAT

### Notebook Structure (8 cells)

| Cell | Purpose |
|---|---|
| 0 | Imports, SparkSession, `PROCESSED_AT` timestamp |
| 1 | Create `main.gold` schema |
| 2 | Build `ticker_daily_summary` — join price + company + sentiment |
| 3 | Build `sector_rankings` — groupBy sector, rank by market cap |
| 4 | Build `sentiment_summary` — signal + confidence per ticker |
| 5 | Build `top_movers` — rank by daily return, GAINER/LOSER flag |
| 6 | Gold summary — row counts for all 4 tables |

### Key Design Decisions

| Decision | Reason |
|---|---|
| `overwrite` mode on all Gold tables | Gold is always fully recomputed from Silver — no incremental logic needed |
| Sentiment score: positive=1, neutral=0, negative=-1 | Average gives a continuous [-1, 1] signal usable by the Agent |
| `RANK()` not `ROW_NUMBER()` for sector_rank | Handles ties correctly (two equal market caps share the same rank) |
| Gold tables are Agent-readable | Agent tools query Gold for price data and sentiment — fast, pre-aggregated |

### Full Medallion Architecture

```
Massive/Polygon API
        ↓
main.bronze.*    Raw, append-only, full raw_json, batch_id per run
        ↓
main.silver.*    Deduplicated, typed, enriched, no raw_json
        ↓
main.gold.*      Aggregated, analytics-ready, Agent-queryable
        ↓
AI Agent         Reads Gold → responds to user queries
        ↓
Lakebase OLTP    Written by Agent (watchlists, notes, reports)
        ↓
Lakebase CDF     Streams into Delta analytics table (Phase 6)
```

---

## ✅ Phase 5 — Embeddings + Vector Search

**File:** `embeddings/04_embed_and_index.ipynb`

### What Phase 5 Does

Builds the semantic search layer that powers the AI Agent's RAG capability. News article text is embedded using a Databricks Foundation Model and indexed in a Vector Search index. The Agent queries this index to retrieve contextually relevant articles for any user question.

### Components Created

| Component | Name | Purpose |
|---|---|---|
| Source Delta table | `main.silver.news_for_search` | CDF-enabled table with `search_text` column |
| Vector Search endpoint | `stock-assistant-vs` | Hosts the index, type: STANDARD |
| Vector Search index | `main.silver.news_for_search_index` | Delta Sync index with managed embeddings |
| Embedding model | `databricks-bge-large-en` | Converts text to vectors automatically |

### How It Works

```
main.silver.news_articles
        ↓ build search_text = "ticker | title | description"
main.silver.news_for_search  (CDF enabled)
        ↓ Delta Sync pipeline (TRIGGERED mode)
Vector Search index  (managed embeddings via BGE Large)
        ↓ similarity_search(query_text, num_results=3)
Agent tool: search_news(query)
        ↓ top-k articles returned as RAG context
AI Agent response grounded in real news
```

### Source Table Design

The `search_text` column combines three fields for richer semantic context:
```python
search_text = ticker + " | " + title + " | " + description
# Example: "AAPL | Apple Is Down 10%... | Apple shares fell..."
```

This gives the embedding model more signal than just the headline alone.

### Notebook Structure (9 cells)

| Cell | Purpose |
|---|---|
| 0 | Markdown description |
| 1 | `%pip install databricks-vectorsearch` + kernel restart |
| 2 | Imports + config (endpoint/index names, model) |
| 3 | Build `news_for_search` Delta table with CDF enabled |
| 4 | Create or verify Vector Search endpoint (waits for ONLINE) |
| 5 | Create or verify Vector Search index (waits for ready) |
| 6 | Trigger index sync (first pipeline run) |
| 7 | Test semantic search with 3 queries |
| 8 | Summary |

### Notebook Structure (10 cells)

| Cell | Purpose |
|---|---|
| 0 | Markdown description |
| 1 | `%pip install databricks-vectorsearch` + kernel restart |
| 2 | Imports + config (endpoint/index names, model) |
| 3 | Build `news_for_search` Delta table with CDF enabled |
| 4 | Create or verify Vector Search endpoint (waits for ONLINE) |
| 5 | Delete stuck index if needed, recreate cleanly |
| 6 | Wait for index ready (40 attempts × 30s = 20 min max) |
| 7 | Trigger sync + test semantic search with 3 queries |
| 8 | Manual status check cell (run anytime to check state) |
| 9 | Summary |

### Provisioning Stages (Free Edition)

The index goes through 3 stages after creation:
```
PROVISIONING_ENDPOINT           ~5-10 min  (endpoint slice allocation)
       ↓
PROVISIONING_PIPELINE_RESOURCES ~10-15 min (embedding pipeline setup)
       ↓
ONLINE / ready: True                        (semantic search available)
```
Total provisioning time on Free Edition: **20-40 minutes**.

### Known Issues & Fixes

| Issue | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: databricks.vector_search` | Package not pre-installed on Serverless | Added `%pip install databricks-vectorsearch` + restart in Cell 1 |
| `'AISearchIndex' object has no attribute 'get'` | VS SDK returns object not dict | Use `idx.describe().get(...)` for all status checks |
| Index stuck in `PROVISIONING_ENDPOINT` forever | Index created before endpoint finished provisioning | Delete and recreate index — Cell 5 handles this automatically |
| `PROVISIONING_PIPELINE_RESOURCES` takes 15+ min | Free Edition — 1 VSU, shared resources | Wait loop extended to 40 attempts × 30s = 20 min |
| Auth notice spam on every `VectorSearchClient()` | Default SDK behavior | Added `disable_notice=True` to suppress |
| `BadRequest: Vector index is not ready` | Searched before index finished | Wait for `ready: True` before calling `similarity_search()` |

### Index Configuration

| Parameter | Value | Reason |
|---|---|---|
| `pipeline_type` | `TRIGGERED` | Manual sync control — sync runs when called explicitly |
| `primary_key` | `article_id` | Unique identifier per article |
| `embedding_source_column` | `search_text` | Rich combined text field |
| `embedding_model_endpoint_name` | `databricks-bge-large-en` | Best available on Free Edition |

### File Structure Update

```
embeddings/
└── 04_embed_and_index.ipynb   ✅ Vector Search index creation + RAG test
```

---

## ✅ Phase 6 — CDF → Delta Analytics

**File:** `cdf/06_cdf_to_delta.ipynb`

### What Phase 6 Does

Captures every row-level change across the Silver Delta tables using Delta Lake's native Change Data Feed, and writes them into a Delta analytics table for pipeline monitoring and usage tracking.

### Architecture Decision

The capstone requires "CDF from Lakebase into a Delta table." Since Lakebase direct connectivity requires OAuth federation (not available on Free Edition), we implement CDF using **Delta Lake's native `enableChangeDataFeed`** — architecturally identical concept:

| | Lakebase CDF | Delta CDF (implemented) |
|---|---|---|
| Enable on source | `REPLICA IDENTITY FULL` | `delta.enableChangeDataFeed = true` |
| Change types | INSERT/UPDATE/DELETE | insert/update_postimage/delete |
| Read mechanism | Logical replication stream | `readChangeFeed = true` |
| Output | Delta analytics table | `main.analytics.cdf_events` |

### Tables Created

| Table | Purpose |
|---|---|
| `main.analytics.cdf_events` | Every row-level change event from Silver tables |
| `main.analytics.cdf_summary` | Aggregated pipeline monitoring view |

### CDF Events Schema

```
event_id        STRING    — unique event identifier
source_table    STRING    — which Silver table changed
operation       STRING    — INSERT / UPDATE / DELETE
ticker          STRING    — which ticker was affected
record_key      STRING    — primary key of changed row
record_snapshot STRING    — JSON snapshot of changed columns
commit_version  BIGINT    — Delta table version of the change
commit_ts       TIMESTAMP — when the change was committed
captured_at     TIMESTAMP — when CDF pipeline ran
```

### Notebook Structure (8 cells)

| Cell | Purpose |
|---|---|
| 0 | Markdown description |
| 1 | Enable CDF on all 4 Silver tables |
| 2 | Create `main.analytics` schema + `cdf_events` table |
| 3 | Read CDF from each Silver table → append to `cdf_events` |
| 4 | Analytics queries on captured events |
| 5 | Build `cdf_summary` Gold-style aggregate table |
| 6 | Summary and architecture note |

### File Structure Update

```
cdf/
└── 06_cdf_to_delta.ipynb   ✅ Delta CDF → analytics table
```

---

## ⬜ Next Steps

```
✅ Phase 3 — Silver transformation    (pipeline/02_silver_transform.ipynb)
✅ Phase 4 — Gold aggregates          (pipeline/03_gold_aggregates.ipynb)
✅ Phase 5 — Embeddings + Vector Search (embeddings/04_embed_and_index.ipynb)
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
├── pipeline/
│   ├── 00_setup_config.ipynb       ✅ Ticker registry (Unity Catalog)
│   ├── 01_bronze_ingestion.ipynb   ✅ Raw ingestion (Massive/Polygon API)
│   ├── 02_silver_transform.ipynb   ✅ Clean, deduplicate, enrich
│   └── 03_gold_aggregates.ipynb    ✅ Analytics aggregates (4 Gold tables)
├── embeddings/
│   └── 04_embed_and_index.ipynb    ✅ Vector Search index + RAG
└── cdf/
    └── 06_cdf_to_delta.ipynb       ✅ Delta CDF → analytics table
```

---

*Last updated: August 2026*
