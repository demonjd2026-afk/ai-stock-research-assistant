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

### Results

| Table | Rows | Content |
|---|---|---|
| `main.analytics.cdf_events` | 50 | 20 companies + 5 prices + 25 news (all as INSERT) |
| `main.analytics.cdf_summary` | 3 | One row per source_table+operation combination |

### Notebook Structure (9 cells)

| Cell | Purpose |
|---|---|
| 0 | Markdown description |
| 1 | Enable CDF on all 4 Silver tables |
| 2 | Create `main.analytics` schema + `cdf_events` table |
| 3 | Check if table already populated (idempotent guard) |
| 4 | Initial snapshot — treats existing rows as INSERT events |
| 5 | Analytics queries on captured events |
| 6 | Build `cdf_summary` aggregate table |
| 7 | Summary and architecture note |

### Known Issue & Fix

| Issue | Cause | Fix |
|---|---|---|
| `DELTA_MISSING_CHANGE_DATA` on version 0 | CDF only records changes AFTER it is enabled — existing rows at version 0 have no CDF data | Treat all current Silver rows as INSERT events via snapshot function — writes 50 initial events |

### Idempotency

The notebook checks `cdf_events.count()` before running the snapshot. If data already exists it skips the snapshot — safe to re-run anytime.

### File Structure Update

```
cdf/
└── 06_cdf_to_delta.ipynb   ✅ Delta CDF → analytics table
```

---

## ✅ Phase 7 — AI Agent with Tools

**File:** `agent/07_agent_tools.ipynb`

### What Phase 7 Does

Implements a fully agentic AI assistant using Databricks Foundation Models API with OpenAI-compatible function calling. The agent uses an agentic loop — it calls tools iteratively, receives results, and continues until it produces a final answer.

### Model

| Property | Value |
|---|---|
| Model | `databricks-meta-llama-3-3-70b-instruct` |
| API | Databricks Foundation Models (OpenAI-compatible) |
| Auth | Notebook context token via `dbutils` |
| Tool calling | OpenAI function-calling format |
| Max rounds | 5 per query |

### Tools Implemented (8 total)

**Read tools (5):**

| Tool | Source | What it returns |
|---|---|---|
| `get_price_data(ticker)` | `main.gold.ticker_daily_summary` | close, open, high, low, volume, daily_return_pct, market_cap_billions |
| `get_sentiment(ticker)` | `main.gold.sentiment_summary` | sentiment_signal, sentiment_confidence, avg_sentiment_score, news_count |
| `compare_tickers(tickers)` | `main.gold.ticker_daily_summary` | Side-by-side comparison sorted by daily return |
| `get_top_movers(limit)` | `main.gold.top_movers` | Top gainers and losers with return rank |
| `search_news(query, num_results)` | Vector Search index (BGE Large embeddings) | Semantically relevant articles with sentiment. Falls back to keyword search if index not ready |

**Write tools (3):**

| Tool | Writes to | What it stores |
|---|---|---|
| `add_to_watchlist(ticker, watchlist_name)` | `main.agent.watchlists` | ticker + watchlist name + timestamp |
| `save_research_note(ticker, note)` | `main.agent.research_notes` | free-text note per ticker |
| `save_analysis_report(ticker, report_text)` | `main.agent.analysis_reports` | full report + model name + timestamp |

### Agent Write Tables

Three Delta tables in `main.agent` schema (Lakebase connection not available on Free Edition — Delta used as equivalent):

```sql
main.agent.watchlists        -- ticker, watchlist, user_email, added_at
main.agent.research_notes    -- ticker, note, user_email, created_at
main.agent.analysis_reports  -- ticker, report_text, agent_model, generated_at
```

### Agentic Loop Implementation

```python
def run_agent(user_query):
    messages = [system_prompt, user_query]
    for round in range(5):
        response = llm(messages, tools=TOOLS)
        if no tool_calls → return response   # done
        for each tool_call:
            result = execute_tool(tool_call)
            messages.append(tool_result)
        # loop again with updated messages
```

### Notebook Structure (14 cells)

| Cell | Purpose |
|---|---|
| 0 | Markdown description |
| 1 | `%pip install openai` + kernel restart |
| 2 | Imports + OpenAI client → Databricks Foundation Models |
| 3 | Create `main.agent.*` write tables |
| 4 | 8 tool function implementations |
| 5 | Tool schemas in OpenAI function-calling format |
| 6 | Agent runner (agentic loop) |
| 7 | Test 1 — Apple price + sentiment |
| 8 | Test 2 — MSFT vs NVDA comparison |
| 9 | Test 3 — Top movers |
| 10 | Test 4 — News search + save note for NVDA |
| 11 | Test 5 — Add AAPL + MSFT to watchlist |
| 12 | Verify write tables |
| 13 | Summary |

### Actual Agent Test Results

**Test 1 — Apple price + sentiment:**
- Called `get_price_data` → AAPL close $303.42, daily_return -1.99%
- Called `get_sentiment` → neutral signal, high confidence, 10 articles
- Proactively called `add_to_watchlist` without being asked

**Test 2 — MSFT vs NVDA comparison:**
- Called `compare_tickers` → NVDA wins (+4.53%) vs MSFT (+2.42%)
- Proactively saved research note + added NVDA to watchlist

**Test 3 — Top movers:**
- META +4.98% top gainer, AAPL -1.99% only loser

**Test 4 — AI news RAG search:**
- Called `search_news` with real Vector Search semantic query
- Retrieved NVDA AI article, saved intelligent research note from content

**Test 5 — Watchlist write:**
- Added AAPL + MSFT to "Tech Watchlist" in 2 separate parallel tool calls

### Write Table Results

| Table | Rows | Content |
|---|---|---|
| `main.agent.watchlists` | 4 | AAPL (My Watchlist), NVDA (My Watchlist), AAPL + MSFT (Tech Watchlist) |
| `main.agent.research_notes` | 2 | NVDA momentum note + NVDA AI sector note |
| `main.agent.analysis_reports` | 0 | Agent chose not to write a report (correct behavior) |

### Known Issues & Fixes

| Issue | Cause | Fix |
|---|---|---|
| `UNSUPPORTED_DATATYPE TEXT` | Delta doesn't support TEXT type | Replace `TEXT` with `STRING` in all CREATE TABLE statements |
| `databricks-vectorsearch deprecated` | Package renamed to `databricks-ai-search` | Updated pip install + import to use new name |

### File Structure Update

```
agent/
└── 07_agent_tools.ipynb   ✅ AI Agent with 8 tools (5 read + 3 write)
```

---

## ✅ Phase 8 — Databricks App Frontend

**File:** `app/app.py`

### What Phase 8 Does

Deploys a Gradio-based chat interface as a Databricks App. The frontend wraps the Phase 7 AI Agent and provides a live, shareable web UI for stock research.

### Architecture

```
Databricks App (Gradio)
       ↓ user message
   run_agent() — agentic loop
       ↓ tool calls
   run_sql() via SDK Statement Execution API
       ↓ Gold tables / Vector Search / agent write tables
   Response + tool trace displayed in UI
```

### App Features

**Chat Panel (left):**
- Natural language chat with the AI Agent
- Tool call trace shown after each response (which tools were called + results)
- 5 example query buttons for quick start
- Send via button or Enter key

**Sidebar (right):**
- Live market summary — all tracked tickers with price, return %, sentiment signal
- Watchlist — current user watchlist from `main.agent.watchlists`
- Refresh buttons for both panels

### Key Differences from Phase 7 Notebook

| Aspect | Phase 7 Notebook | Phase 8 App |
|---|---|---|
| Spark session | `SparkSession.builder` | Not used |
| Data access | PySpark DataFrame API | SDK Statement Execution API (`run_sql()`) |
| Auth | `dbutils` token | `DATABRICKS_TOKEN` env var (automatic in Apps) |
| Vector Search | `databricks.ai_search` | `databricks.ai_search` (same) |
| Deployment | Run in notebook | Deployed as Databricks App with public URL |

### Deployment Steps

1. Push `app/app.py` to GitHub
2. Pull in Databricks Git folder
3. Go to **Compute → Apps → Create App**
4. Name: `stock-research-assistant`
5. Source: point to `app/app.py` in your workspace
6. Click **Deploy** — Databricks builds and hosts the app
7. Open the generated URL to access the live chat interface

### File Structure Update

```
app/
└── app.py   ✅ Gradio frontend (chat + market summary + watchlist sidebar)
```

---

## ✅ Unity Catalog Grants

**File:** `lakebase/grants.sql`

### When to run

Run this file **once** in the following situations:

| When | Why |
|---|---|
| After running all pipeline notebooks for the first time | App cannot see tables without grants |
| After creating a new schema or table | New objects need explicit grants |
| Before deploying or testing the Databricks App | App runs under `account users` identity |

### How to run

1. Go to **SQL Editor** in Databricks
2. Make sure the warehouse is **Serverless**
3. Paste contents of `lakebase/grants.sql`
4. Select all → Click **Run selected**
5. All statements should return **OK**

### What is granted

| Grant | Tables | Privilege |
|---|---|---|
| Catalog access | `main` | `USE CATALOG` |
| Schema access | `main.gold`, `main.silver`, `main.agent`, `main.analytics`, `main.bronze`, `main.config` | `USE SCHEMA` |
| Gold tables | `ticker_daily_summary`, `sentiment_summary`, `top_movers`, `sector_rankings` | `SELECT` |
| Silver tables | `news_articles`, `news_for_search` | `SELECT` |
| Agent tables | `watchlists`, `research_notes`, `analysis_reports` | `SELECT + MODIFY` |
| Config table | `ticker_config` | `SELECT` |

### Why `MODIFY` instead of `INSERT`

Unity Catalog metastore version 1.0 (used on Free Edition) does not support `INSERT` as a table privilege. Use `MODIFY` which covers INSERT, UPDATE, and DELETE on Delta tables.

### Known issue

```
ErrorClass=INVALID_PARAMETER_VALUE.PRIVILEGE_NOT_APPLICABLE_TO_ENTITY
Privilege INSERT is not applicable to this entity
```
**Fix:** Replace `INSERT` with `MODIFY` in all GRANT statements.

---

## ✅ Workflow Automation — Daily Pipeline

**Job name:** `stock-assistant-daily-pipeline`
**Job ID:** `620376219385835`

### Tasks (run in sequence)

| # | Task | Notebook | Compute | Depends on |
|---|---|---|---|---|
| 1 | `bronze_ingestion` | `pipeline/01_bronze_ingestion` | Serverless | — |
| 2 | `silver_transform` | `pipeline/02_silver_transform` | Serverless | bronze_ingestion |
| 3 | `gold_aggregates` | `pipeline/03_gold_aggregates` | Serverless | silver_transform |
| 4 | `sync_vs_index` | `pipeline/05_sync_index` | Serverless | gold_aggregates |

### Schedule

```
Cron     : 0 0 22 ? * MON-FRI
Timezone : Asia/Calcutta (UTC+05:30)
Meaning  : 10 PM IST every weekday = after US market close (4:30 PM UTC)
```

### How to trigger manually

Go to **Jobs & Pipelines → stock-assistant-daily-pipeline → Run now**

### Expected runtime

```
bronze_ingestion  ~13 min  (API rate limit: 5 tickers × 3 endpoints × 13s sleep)
silver_transform  ~30 sec
gold_aggregates   ~30 sec
sync_vs_index     ~2 min
──────────────────────────
Total             ~16 min
```

### What it does end to end

```
Polygon/Massive API (5 active tickers)
        ↓ bronze_ingestion
main.bronze.raw_companies / raw_price_snapshots / raw_news_articles
        ↓ silver_transform
main.silver.companies / price_snapshots / news_articles
        ↓ gold_aggregates
main.gold.ticker_daily_summary / sector_rankings / sentiment_summary / top_movers
        ↓ sync_vs_index
main.silver.news_for_search (refreshed) → Vector Search index synced
        ↓
Databricks App sidebar shows fresh prices on next Refresh click
```

### File Structure Update

```
pipeline/
├── 00_setup_config.ipynb       ✅ Ticker registry
├── 01_bronze_ingestion.ipynb   ✅ Task 1 — Raw ingestion
├── 02_silver_transform.ipynb   ✅ Task 2 — Clean + enrich
├── 03_gold_aggregates.ipynb    ✅ Task 3 — Analytics tables
└── 05_sync_index.ipynb         ✅ Task 4 — VS index sync
```

---

## ✅ Project Complete

```
✅ Phase 0  — Workspace setup
✅ Phase 1  — Lakebase schema (8 tables)
✅ Phase 2  — Bronze ingestion (Polygon API)
✅ Phase 3  — Silver transformation
✅ Phase 4  — Gold aggregates (4 tables)
✅ Phase 5  — Embeddings + Vector Search
✅ Phase 6  — CDF → Delta analytics
✅ Phase 7  — AI Agent (8 tools: 5 read + 3 write)
✅ Phase 8  — Databricks App (live, market data loading)
✅ Workflow — Daily automated pipeline (Mon-Fri 10 PM IST)
✅ Grants   — Unity Catalog access for App
```

```
✅ Phase 3 — Silver transformation    (pipeline/02_silver_transform.ipynb)
✅ Phase 4 — Gold aggregates          (pipeline/03_gold_aggregates.ipynb)
✅ Phase 5 — Embeddings + Vector Search (embeddings/04_embed_and_index.ipynb)
⬜ Phase 6 — Lakebase CDF → Delta     (cdf/06_cdf_to_delta.ipynb)
✅ Phase 7 — AI Agent with tools      (agent/07_agent_tools.ipynb)
✅ Phase 8 — Databricks App frontend  (app/app.py)
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
├── cdf/
│   └── 06_cdf_to_delta.ipynb       ✅ Delta CDF → analytics table
├── agent/
│   └── 07_agent_tools.ipynb        ✅ AI Agent (8 tools: 5 read + 3 write)
└── app/
    └── app.py                      ✅ Gradio frontend (Databricks App)
```

---

*Last updated: August 2026*
