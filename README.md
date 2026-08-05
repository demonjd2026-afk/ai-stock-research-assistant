# 📈 AI Stock Market Research Assistant

> An AI-powered stock research platform built on the **Databricks Lakehouse** — combining end-of-day market data ingestion via the Massive Stocks API (Polygon.io), semantic RAG over financial news, a multi-tool AI Agent, and a live Gradio frontend deployed on Databricks Apps.

**GitHub:** [demonjd2026-afk/ai-stock-research-assistant](https://github.com/demonjd2026-afk/ai-stock-research-assistant)  
**Capstone:** [Databricks AI Bootcamp](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) by Zach Wilson

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
| **Phase 1** | Lakebase schema — 8 Postgres tables with CDF | ✅ Complete |
| **Phase 2** | Bronze ingestion — Polygon API → Delta | ✅ Complete |
| **Phase 3** | Silver transformation — clean, deduplicate, enrich | ✅ Complete |
| **Phase 4** | Gold aggregates — 4 analytics-ready tables | ✅ Complete |
| **Phase 5** | Embeddings — BGE Large → AI Search index | ✅ Complete |
| **Phase 6** | CDF pipeline — Delta CDF → analytics table | ✅ Complete |
| **Phase 7** | AI Agent — 11 tools (7 read + 4 write) | ✅ Complete |
| **Phase 8** | Databricks App — live Gradio frontend | ✅ Complete |
| **Workflow** | Daily automated pipeline (Mon–Fri) | ✅ Complete |

---

## 🏗️ Architecture

```
Massive/Polygon Stocks API  (20 tickers — 5 sectors)
           │  REST · EOD OHLCV · News · Fundamentals
           │  Free tier: end-of-day data (prev trading day close)
           ▼
    ┌─────────────┐  PySpark append
    │   BRONZE    │  raw_companies · raw_price_snapshots · raw_news_articles
    └──────┬──────┘
           │  Dedup · Cast · Enrich
           ▼
    ┌─────────────┐  Delta overwrite
    │   SILVER    │  companies · price_snapshots · news_articles · news_for_search
    └──────┬──────┘
           │  Aggregate · Join · Rank
           ▼
    ┌─────────────┐  Delta overwrite
    │    GOLD     │  ticker_daily_summary · sector_rankings
    │             │  sentiment_summary · top_movers
    └──────┬──────┘
           │                          │
           │                    ┌─────▼──────────────────┐
           │                    │  Databricks AI Search  │
           │                    │  (BGE Large embeddings)│
           │                    │  news_for_search_index │
           │                    └─────┬──────────────────┘
           │                          │ semantic RAG
           ▼                          ▼
    ┌──────────────────────────────────────┐
    │            AI AGENT                  │
    │  Llama 3.3 70B · Function Calling    │
    │  7 read tools · 4 write tools        │
    └──────────────┬───────────────────────┘
                   │ writes
          ┌────────┼──────────┐
          ▼        ▼          ▼
    watchlists  notes    reports
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
    │  8 OLTP tables · CDF     │
    │  stock_assistant schema  │
    └──────────┬───────────────┘
               │ Delta CDF
               ▼
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

---

## 🥉🥈🥇 Medallion Pipeline

### Bronze Layer (`main.bronze`)
Raw append-only ingestion — nothing is lost or transformed at this layer.

| Table | Source Endpoint | Description |
|---|---|---|
| `raw_companies` | `GET /v3/reference/tickers/{ticker}` | Company fundamentals (name, exchange, market cap, SIC) |
| `raw_price_snapshots` | `GET /v2/aggs/ticker/{ticker}/prev` | Previous day's OHLCV close |
| `raw_news_articles` | `GET /v2/reference/news` | Last 7 days of news per ticker |

> **Data note:** `/prev` endpoint returns the **previous trading day's** closing OHLCV — this is the standard EOD data available on Massive/Polygon's free tier.

### Silver Layer (`main.silver`)
Cleaned, deduplicated, typed, analytics-ready tables.

| Table | Key transformations |
|---|---|
| `companies` | Dedup by ticker · exchange code normalization · market_cap_billions derived |
| `price_snapshots` | Dedup by (ticker, snapshot_date) · daily_return_pct · is_up_day flag |
| `news_articles` | Dedup by article_id · sentiment normalized · article_age_days derived |
| `news_for_search` | search_text = ticker + title + description — source for Vector Search |

### Gold Layer (`main.gold`)
Aggregated, Agent-queryable analytics tables.

| Table | Description |
|---|---|
| `ticker_daily_summary` | Price + company + sentiment joined — 1 row per ticker (latest date) |
| `sector_rankings` | Market cap + return + sentiment aggregated by sector |
| `sentiment_summary` | BULLISH / NEUTRAL / BEARISH signal + HIGH / MEDIUM / LOW confidence |
| `top_movers` | Daily return ranked — GAINER / LOSER / FLAT per ticker |

---

## 🤖 AI Agent Tools

| Tool | Type | Source | Description |
|---|---|---|---|
| `get_price_data` | Read | `main.gold.ticker_daily_summary` | EOD close, return, market cap |
| `get_sentiment` | Read | `main.gold.sentiment_summary` | Sentiment signal + confidence |
| `compare_tickers` | Read | `main.gold.ticker_daily_summary` | Side-by-side comparison |
| `get_top_movers` | Read | `main.gold.top_movers` | Gainers and losers |
| `search_news` | Read | AI Search index (RAG) | Semantic news search |
| `get_sector_rankings` | Read | `main.gold.sector_rankings` | Sector market cap + avg return |
| `flag_price_moves` | Read | `main.gold.ticker_daily_summary` | Alert if ticker moved > N% in a day |
| `add_to_watchlist` | Write | `main.agent.watchlists` | Save ticker to watchlist |
| `remove_from_watchlist` | Write | `main.agent.watchlists` | Remove ticker from watchlist |
| `save_research_note` | Write | `main.agent.research_notes` | Persist research note |
| `save_analysis_report` | Write | `main.agent.analysis_reports` | Log agent report |

**Agentic loop:** User query → LLM picks tools → execute → results back to LLM → repeat up to 5 rounds → final response.

**Read tools (7):** `get_price_data` · `get_sentiment` · `compare_tickers` · `get_top_movers` · `search_news` · `get_sector_rankings` · `flag_price_moves`

**Write tools (4):** `add_to_watchlist` · `remove_from_watchlist` · `save_research_note` · `save_analysis_report`

---

## ⚙️ Workflow Automation

**Job:** `stock-assistant-daily-pipeline`

| Task | Notebook | Depends on |
|---|---|---|
| `bronze_ingestion` | `pipeline/01_bronze_ingestion` | — |
| `silver_transform` | `pipeline/02_silver_transform` | bronze_ingestion |
| `gold_aggregates` | `pipeline/03_gold_aggregates` | silver_transform |
| `sync_vs_index` | `pipeline/05_sync_index` | gold_aggregates |

**Schedule:** `0 0 22 ? * MON-FRI` — 10 PM IST daily (after US market close, ingests that day's EOD data)

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
- Open Lakebase → stock-assistant → SQL Editor
- Run `lakebase/05_schema_ddl.sql`

**4. Run notebooks in order (Serverless compute):**
```
00_setup_config → 01_bronze_ingestion → 02_silver_transform →
03_gold_aggregates → 04_embed_and_index → 06_cdf_to_delta → 07_agent_tools
```

**5. Run Unity Catalog grants:**
```
SQL Editor → run lakebase/grants.sql
```

**6. Deploy App:**
```
Compute → Apps → Create App
Name: stock-research-assistant
Source: app/app.py  |  Branch: main  |  Path: app
```

---

## 📱 App Features

- **3-column layout** — Suggestions · Chat · Market data
- **Suggestion panel** — 8 pre-built quick-question buttons
- **Chat** — Blue user bubbles · Dark assistant bubbles
- **Input** — Press Enter to send (Claude-style)
- **Market Summary** — EOD prices for all 20 tickers (refreshed daily)
- **Watchlist** — Shows saved tickers from agent interactions

---

## 👤 Author

**Jayanth Dolai** — Senior Software Engineer  
6+ years | Azure · Databricks · Microsoft Fabric  
Certifications: Databricks Data Engineer Associate · DP-700 · DP-600 · DP-900 · Generative AI Associate

[LinkedIn](https://www.linkedin.com/in/jayanth-dolai-7b115213a/) · [GitHub](https://github.com/demonjd2026-afk)

---

## 📄 License

MIT — see [LICENSE](LICENSE)
