# 📈 AI Stock Market Research Assistant

> An AI-powered stock research platform built on the **Databricks Lakehouse** — end-of-day market data ingestion via the Massive Stocks API (Polygon.io), a Bronze→Silver→Gold medallion pipeline, semantic RAG over financial news, an 11-tool AI Agent on Llama 3.3 70B, and a live Gradio frontend deployed on Databricks Apps.

**Live App:** [stock-research-assistant-7474654640109575.aws.databricksapps.com](https://stock-research-assistant-7474654640109575.aws.databricksapps.com)
**GitHub:** [demonjd2026-afk/ai-stock-research-assistant](https://github.com/demonjd2026-afk/ai-stock-research-assistant)
**Capstone:** [Databricks AI Bootcamp](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) by Zach Wilson
**Workspace:** `dbc-291b687e-da89.cloud.databricks.com` (Databricks Free Edition, AWS)

---

## 🖼️ Live Application

![Databricks App — chat, market summary, watchlist](screenshots/09_app_watchlist_write.png)

The deployed Gradio app: 8 suggestion buttons (left), agent chat (center), and a live Market Summary + Watchlist sidebar (right). The screenshot shows the agent executing a **write** tool — `add_to_watchlist("MSFT")` — with the result landing in `main.agent.watchlists` and surfacing in the sidebar on refresh.

---

## ⚠️ Free Edition Constraints & Workarounds

This project was built entirely on **Databricks Free Edition**. Three notable constraints were encountered:

| # | Constraint | Impact | Workaround |
|---|---|---|---|
| 1 | `api.massive.com` blocked by outbound domain filter | Cannot call Massive API directly | Use `api.polygon.io` — same API key works (Massive rebranded from Polygon.io in Oct 2025) |
| 2 | Massive **free tier = end-of-day data only** | No intraday/real-time prices | Pipeline ingests previous trading day's OHLCV close prices — standard for daily pipelines |
| 3 | OAuth JWT federation unavailable | Cannot connect Lakebase CDF to Delta directly | Delta Lake's native `enableChangeDataFeed` used — architecturally identical CDC pattern |

> **On data recency:** Massive's free subscription tier provides end-of-day (EOD) snapshots, not real-time tick data. Real-time trades and quotes require a paid Massive plan. The pipeline runs daily after US market close (10 PM IST / 4:30 PM UTC) and ingests the previous day's closing prices — this is accurate, real market data, just not intraday.

---

## 🚦 Build Status

| Phase | Description | Status |
|---|---|---|
| **Phase 0** | Workspace setup — GitHub, Databricks, Secrets | ✅ Complete |
| **Phase 1** | Lakebase schema — 8 Postgres tables with `REPLICA IDENTITY FULL` | ✅ Complete |
| **Phase 2** | Bronze ingestion — Polygon API → Delta (3 tables) | ✅ Complete |
| **Phase 3** | Silver transformation — clean, deduplicate, enrich | ✅ Complete |
| **Phase 4** | Gold aggregates — 4 analytics-ready tables | ✅ Complete |
| **Phase 5** | Embeddings — BGE Large → AI Search index (Online, 144 rows) | ✅ Complete |
| **Phase 6** | CDF pipeline — Delta CDF → analytics tables | ✅ Complete |
| **Phase 7** | AI Agent — 11 tools (7 read + 4 write) | ✅ Complete |
| **Phase 8** | Databricks App — live Gradio frontend | ✅ Complete |
| **Workflow** | Daily automated pipeline, 5 tasks (Mon–Fri) | ✅ Complete |

---

## 🏗️ Architecture

```
Massive/Polygon Stocks API  (20 tickers — 5 sectors)
           │  REST · EOD OHLCV · News · Fundamentals
           │  Free tier: end-of-day data (prev trading day close)
           ▼
    ┌─────────────┐  PySpark MERGE upsert (idempotent)
    │   BRONZE    │  raw_companies · raw_price_snapshots · raw_news_articles
    └──────┬──────┘
           │  Dedup · Cast · Enrich
           ▼
    ┌─────────────┐  Delta overwrite · CDF enabled
    │   SILVER    │  companies · price_snapshots · news_articles · news_for_search
    └──┬────────┬─┴──────────────┐
       │        │                │ readChangeFeed (watermarked)
       │        │                ▼
       │        │        ┌───────────────────────────┐
       │        │        │   ANALYTICS (Delta CDF)   │
       │        │        │  cdf_events · cdf_summary │
       │        │        │  cdf_watermarks           │
       │        │        └───────────────────────────┘
       │        │
       │        └────────────────┐
       ▼                         ▼
┌─────────────┐         ┌────────────────────────┐
│    GOLD     │         │  Databricks AI Search  │
│ ticker_daily│         │  (BGE Large embeddings)│
│ _summary    │         │  news_for_search_index │
│ sector_rank │         │  Endpoint: stock-      │
│ sentiment   │         │  assistant-vs          │
│ top_movers  │         └───────────┬────────────┘
└──────┬──────┘                     │ semantic RAG
       │                            │
       ▼                            ▼
    ┌──────────────────────────────────────┐
    │            AI AGENT                  │
    │  Llama 3.3 70B · Function Calling    │
    │  7 read tools · 4 write tools        │
    └──────────────┬───────────────────────┘
                   │ writes
          ┌────────┼──────────┐
          ▼        ▼          ▼
    watchlists  research   analysis
                _notes     _reports
          (main.agent.*)
                   │
                   ▼
    ┌──────────────────────────┐
    │   DATABRICKS APP         │
    │   Gradio · 3-column UI   │
    │   Suggestions · Chat     │
    │   Market · Watchlist     │
    └──────────────────────────┘

    ┌──────────────────────────┐
    │  LAKEBASE (Postgres 17)  │
    │  8 OLTP tables           │
    │  stock_assistant schema  │
    │  REPLICA IDENTITY FULL   │
    └──────────────────────────┘
```

---

## 📊 Tickers Covered (20 — 5 Sectors)

| Sector | Tickers |
|---|---|
| Technology | AAPL · GOOGL · META · MSFT · NVDA |
| Finance | BAC · GS · JPM · MS · V |
| Healthcare | ABBV · JNJ · MRK · PFE · UNH |
| Consumer | AMZN · TSLA · WMT |
| Energy | CVX · XOM |

Tickers live in Unity Catalog (`main.config.ticker_config`), not in code. `00_setup_config.ipynb` seeds all 20 with the 5 Technology tickers active; the remaining 15 are activated with one SQL statement:

```sql
UPDATE main.config.ticker_config SET active = true;              -- all 20
UPDATE main.config.ticker_config SET active = true WHERE sector = 'Finance';
```

All 20 are active in the current deployment — the latest pipeline run ingested all of them.

---

## 🥉🥈🥇 Medallion Pipeline

### Bronze Layer (`main.bronze`) — 3 tables

Raw ingestion — nothing is lost or transformed at this layer. Writes are an **idempotent MERGE** on each feed's natural key, so a job retry or same-day re-run updates the existing row instead of appending a duplicate.

| Table | Source Endpoint | Merge key | Description |
|---|---|---|---|
| `raw_companies` | `GET /v3/reference/tickers/{ticker}` | `(ticker, run_date)` | Company fundamentals (name, exchange, market cap, SIC) |
| `raw_price_snapshots` | `GET /v2/aggs/ticker/{ticker}/prev` | `(ticker, snapshot_date)` | Previous trading day's OHLCV close |
| `raw_news_articles` | `GET /v2/reference/news` | `(article_id, ticker)` | Last 7 days of news per ticker |

![Unity Catalog — Bronze schema](screenshots/02_unity_catalog_bronze.png)

> **Data note:** the `/prev` endpoint returns the **previous trading day's** closing OHLCV — the standard EOD data available on Massive/Polygon's free tier.

### Silver Layer (`main.silver`) — 4 tables + 1 managed index

Cleaned, deduplicated, typed, analytics-ready tables. Change Data Feed is enabled on every table.

| Table | Key transformations |
|---|---|
| `companies` | Dedup by ticker · exchange code normalization (`XNAS→NASDAQ`) · `market_cap_billions` derived |
| `price_snapshots` | Dedup by (ticker, snapshot_date) · `daily_return_pct` · `price_range` · `is_up_day` flag |
| `news_articles` | Dedup by article_id · sentiment normalized · `article_age_days` · `published_ts` parsed |
| `news_for_search` | `search_text` = ticker + title + description — union of news articles **and** company profiles; source for AI Search |
| `news_for_search_index` | Managed Vector Index (Delta Sync) — see Phase 5 |

![Unity Catalog — Silver schema](screenshots/03a_unity_catalog_silver.png)

### Gold Layer (`main.gold`) — 4 tables

Aggregated, Agent-queryable analytics tables, fully recomputed on every run.

| Table | Description |
|---|---|
| `ticker_daily_summary` | Price + company + sentiment joined — 1 row per ticker per day |
| `sector_rankings` | Market cap + return + sentiment aggregated and ranked by sector |
| `sentiment_summary` | BULLISH / NEUTRAL / BEARISH signal + HIGH / MEDIUM / LOW confidence |
| `top_movers` | Daily return ranked — GAINER / LOSER / FLAT per ticker |

![Unity Catalog — Gold schema](screenshots/03b_unity_catalog_gold.png)

### Analytics Layer (`main.analytics`) — 2 tables

| Table | Description |
|---|---|
| `cdf_events` | Row-level change events captured from the Silver tables via Delta CDF |
| `cdf_summary` | Aggregated change counts per source table + operation |
| `cdf_watermarks` | Last Delta version already captured per source table — drives incremental `readChangeFeed` |

---

## 🔌 API Integration

The Bronze notebook loads the API key from Databricks Secrets — never hardcoded — and calls `api.polygon.io` with the key as a query parameter.

![Bronze ingestion — API integration code](screenshots/04a_api_integration_code.png)

Every call goes through `api_get()`, which handles 3 retries, 429 rate-limit backoff (60s), and a 15s timeout. A 13-second sleep between ticker calls respects the free tier's 5 requests/minute limit.

**Verified ingestion run** (batch `073b93e9-3080-4ea1-baa1-203487f6b10b`, run date 2026-08-06):

| Bronze table | This run | Cumulative total |
|---|---|---|
| `main.bronze.raw_companies` | 20 | 115 |
| `main.bronze.raw_price_snapshots` | 20 | 95 |
| `main.bronze.raw_news_articles` | 118 | 630 |

![Bronze ingestion output](screenshots/04b_api_ingestion_output.png)

---

## 🔍 Vector Search & Semantic RAG

| Component | Value |
|---|---|
| Endpoint | `stock-assistant-vs` — Standard, **Ready** |
| Index | `main.silver.news_for_search_index` — Delta Sync, **Online** |
| Source table | `main.silver.news_for_search` |
| Embedding model | `databricks-bge-large-en` — **Ready** |
| Sync schedule | Triggered (fired by the `sync_vs_index` workflow task) |
| Rows indexed | 144 |

![AI Search index online](screenshots/05a_vector_search_index_online.png)

![Embedding model endpoint ready](screenshots/05b_vector_search_embedding_model.png)

![AI Search endpoint ready](screenshots/05c_vector_search_endpoint_ready.png)

The source table unions **news articles and company profiles**, so the agent can answer both "what's the latest AI news?" and "what does Chevron do?" from the same index:

![news_for_search source rows](screenshots/06a_semantic_search_source.png)

Each row carries a `search_text` field combining ticker, title, and description — richer embedding signal than a headline alone:

![search_text column](screenshots/06b_semantic_search_text.png)

---

## 🔄 Change Data Feed → Delta Analytics

Delta Lake's native `enableChangeDataFeed` captures every row-level change on the Silver tables and lands it in `main.analytics.cdf_events`:

![cdf_events table](screenshots/07a_cdf_analytics_table.png)

Aggregated into `main.analytics.cdf_summary` — 50 events from the initial snapshot:

![cdf_summary aggregation](screenshots/07c_cdf_analytics_summary.png)

| Source table | Operation | Events |
|---|---|---|
| `main.silver.news_articles` | INSERT | 25 |
| `main.silver.companies` | INSERT | 20 |
| `main.silver.price_snapshots` | INSERT | 5 |

---

## 🤖 AI Agent Tools

**Model:** `databricks-meta-llama-3-3-70b-instruct` (Databricks Foundation Models API, OpenAI-compatible function calling)

| Tool | Type | Source | Description |
|---|---|---|---|
| `get_price_data(ticker)` | Read | `main.gold.ticker_daily_summary` | EOD close, OHLC, volume, return, market cap |
| `get_sentiment(ticker)` | Read | `main.gold.sentiment_summary` | Sentiment signal + confidence + news count |
| `compare_tickers(tickers)` | Read | `main.gold.ticker_daily_summary` | Side-by-side comparison sorted by return |
| `get_top_movers(limit)` | Read | `main.gold.top_movers` | Top gainers and losers |
| `search_news(query, num_results)` | Read | AI Search index (RAG) | Semantic news search — falls back to keyword `LIKE` if index unavailable |
| `get_sector_rankings()` | Read | `main.gold.sector_rankings` | Sector market cap + avg return + sentiment |
| `flag_price_moves(ticker?, threshold_pct)` | Read | `main.gold.ticker_daily_summary` | Omit `ticker` to scan every stock; pass one to check it alone. Default threshold 2% |
| `add_to_watchlist(ticker, watchlist_name)` | Write | `main.agent.watchlists` | Save ticker to a named watchlist |
| `remove_from_watchlist(ticker, watchlist_name)` | Write | `main.agent.watchlists` | Remove ticker (all watchlists if name omitted) |
| `save_research_note(ticker, note)` | Write | `main.agent.research_notes` | Persist a research note |
| `save_analysis_report(ticker, report_text)` | Write | `main.agent.analysis_reports` | Log an agent-generated report |

All 11 tools expose the **same signature and return shape** in `agent/07_agent_tools.ipynb` and `app/app.py`, so the model's tool contract does not drift between the notebook and the deployed app.

**Agentic loop:** user query → LLM picks tools → execute → results appended to messages → repeat up to 5 rounds → final response.

**SQL safety:** every query in `app/app.py` binds values through named parameter markers (`:ticker`, `:email`, …) via the Statement Execution API. No user- or LLM-supplied value is concatenated into a statement; `LIMIT` sizes are clamped to bounded integers in Python, where markers aren't permitted.

### Agent writes, verified

Write tools land real rows in Unity Catalog. `main.agent.watchlists` after app interaction:

![main.agent.watchlists rows](screenshots/10_agent_watchlist_table.png)

```sql
main.agent.watchlists        -- id, user_email, watchlist, ticker, added_at
main.agent.research_notes    -- id, user_email, ticker, note, created_at
main.agent.analysis_reports  -- id, user_email, ticker, report_text, agent_model, generated_at
```

---

## ⚙️ Workflow Automation

**Job:** `stock-assistant-daily-pipeline` — **Job ID** `620376219385835`

| # | Task | Notebook | Depends on | Duration |
|---|---|---|---|---|
| 1 | `bronze_ingestion` | `pipeline/01_bronze_ingestion` | — | ~13m 14s |
| 2 | `silver_transform` | `pipeline/02_silver_transform` | bronze_ingestion | ~23s |
| 3 | `cdf_to_delta` | `cdf/06_cdf_to_delta` | silver_transform | ~19s |
| 4 | `gold_aggregates` | `pipeline/03_gold_aggregates` | silver_transform | ~27s |
| 5 | `sync_vs_index` | `pipeline/05_sync_index` | gold_aggregates | ~1m 37s |

Tasks 3 and 4 both depend on `silver_transform` and run in parallel. All tasks run on Serverless compute.

**Schedule:** `0 0 22 ? * MON-FRI` (Asia/Calcutta) — 10 PM IST every weekday, after US market close.

**Latest run** — run ID `165389275773657`, Aug 06 2026 03:04–03:20 PM, **15m 45s, Succeeded**:

![Workflow run with CDF task](screenshots/08_workflow_with_cdf.png)

The earlier 4-task version of the job (run ID `417545250444084`, 15m 49s) before `cdf_to_delta` was wired in:

![Workflow run success](screenshots/01_workflow_run_success.png)

Nearly all of the runtime is the Bronze API rate limit (20 tickers × 3 endpoints × 13s sleep).

---

## 📁 Repository Structure

```
ai-stock-research-assistant/
├── README.md
├── SETUP.md                        Step-by-step build log & reproduction guide
├── RESUBMISSION.md                 Reviewer note — what changed since the graded snapshot
├── capstone_final.pdf              Capstone submission document
├── lakebase/
│   ├── 05_schema_ddl.sql           8 Lakebase Postgres tables (REPLICA IDENTITY FULL)
│   ├── grants.sql                  Unity Catalog grants for the App identity
│   └── verify_agent_writes.sql     Proof queries: agent writes, Bronze replay, CDF watermarks
├── pipeline/
│   ├── 00_setup_config.ipynb       Ticker registry in Unity Catalog
│   ├── 01_bronze_ingestion.ipynb   Task 1 — raw ingestion from Polygon API
│   ├── 02_silver_transform.ipynb   Task 2 — clean, deduplicate, enrich
│   ├── 03_gold_aggregates.ipynb    Task 4 — 4 Gold analytics tables
│   └── 05_sync_index.ipynb         Task 5 — refresh source + sync AI Search index
├── embeddings/
│   └── 04_embed_and_index.ipynb    AI Search endpoint + index creation
├── cdf/
│   └── 06_cdf_to_delta.ipynb       Task 3 — Delta CDF → analytics tables
├── agent/
│   └── 07_agent_tools.ipynb        AI Agent — 11 tools + agentic loop + tests
├── app/
│   ├── app.py                      Gradio frontend (Databricks App)
│   └── requirements.txt            openai · databricks-ai-search · databricks-sdk
└── screenshots/                    Proof-of-execution screenshots used in the docs
```

---

## 🚀 Deployment Guide

### Prerequisites
- Databricks Free Edition account — [signup](https://databricks.com)
- LinkedIn verified (unlocks outbound internet)
- Massive/Polygon API key — [signup](https://massive.com) (free tier = EOD data)

### Quick Start

**1. Clone repo into Databricks:**
```
Workspace → Create → Git folder
URL: https://github.com/demonjd2026-afk/ai-stock-research-assistant
```

**2. Store API key securely:**
```python
import getpass
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.secrets.create_scope(scope="capstone")
api_key = getpass.getpass("Paste Massive API key: ")
w.secrets.put_secret(scope="capstone", key="massive_api_key", string_value=api_key)
api_key = None
```

**3. Run Lakebase schema:**
- Open Lakebase → `stock-assistant` → SQL Editor
- Run `lakebase/05_schema_ddl.sql`

**4. Run notebooks in order (Serverless compute):**
```
00_setup_config → 01_bronze_ingestion → 02_silver_transform →
03_gold_aggregates → 04_embed_and_index → 06_cdf_to_delta → 07_agent_tools
```
> Phase 5 (`04_embed_and_index`) takes 20–40 minutes on Free Edition while the AI Search endpoint and index provision.

**5. Run Unity Catalog grants:**
```
SQL Editor → run lakebase/grants.sql
```

**6. Deploy App:**
```
Compute → Apps → Create App
Name: stock-research-assistant
Source: app/  |  Entry point: app.py  |  Branch: main
```

**7. Create the daily job:** 5 tasks as listed in [Workflow Automation](#️-workflow-automation), all Serverless, cron `0 0 22 ? * MON-FRI`.

Full step-by-step detail, every error hit, and every fix are in [SETUP.md](SETUP.md).

---

## 📱 App Features

- **3-column layout** — Suggestions · Chat · Market data
- **Suggestion panel** — 8 pre-built quick-question buttons (Apple stock, Top movers, MSFT vs NVDA, AI stocks news, Add to watchlist, Sector view, Notable moves, Best pick today)
- **Chat** — blue user bubbles · dark assistant bubbles · conversation state preserved across turns
- **Input** — press Enter or click ↑ to send; 🗑️ clears the conversation
- **Market Summary** — EOD close and daily return for all 20 tickers, sorted by return, with 🟢/🔴 indicators
- **Watchlist** — live view of `main.agent.watchlists`, populated by the agent's write tools
- **Data access** — SDK Statement Execution API (no Spark session needed inside the App)

---

## 👤 Author

**Jayanth Dolai** — Senior Software Engineer
6+ years | Azure · Databricks · Microsoft Fabric
Certifications: Databricks Data Engineer Associate · DP-700 · DP-600 · DP-900 · Generative AI Associate

[LinkedIn](https://www.linkedin.com/in/jayanth-dolai-7b115213a/) · [GitHub](https://github.com/demonjd2026-afk)

---

## 📄 License

MIT — see [LICENSE](LICENSE)
