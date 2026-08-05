# 📈 AI Stock Market Research Assistant

> An AI-powered stock research platform built on the **Databricks Lakehouse** — combining real-time market data ingestion via the Massive Stocks API (Polygon.io), semantic RAG over financial news, a multi-tool AI Agent, and a live Gradio frontend deployed on Databricks Apps.

**GitHub:** [demonjd2026-afk/ai-stock-research-assistant](https://github.com/demonjd2026-afk/ai-stock-research-assistant)  
**Capstone:** [Databricks AI Bootcamp](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) by Zach Wilson

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
| **Phase 7** | AI Agent — 8 tools (5 read + 3 write) | ✅ Complete |
| **Phase 8** | Databricks App — live Gradio frontend | ✅ Complete |
| **Workflow** | Daily automated pipeline (Mon–Fri) | ✅ Complete |

---

## 🏗️ Architecture

```
Massive/Polygon Stocks API  (20 tickers — 5 sectors)
           │  REST · OHLCV · News · Fundamentals
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
           │                    │  (BGE Large embeddings) │
           │                    │  news_for_search_index  │
           │                    └─────┬──────────────────┘
           │                          │ semantic RAG
           ▼                          ▼
    ┌──────────────────────────────────────┐
    │            AI AGENT                  │
    │  Llama 3.3 70B · Function Calling    │
    │  5 read tools · 3 write tools        │
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
    ┌──────────────────────────┐
    │  main.analytics          │
    │  cdf_events · cdf_summary│
    └──────────────────────────┘
```

---

## ✅ Capstone Requirements

| Requirement | Implementation |
|---|---|
| **Spark data pipeline** | Bronze → Silver → Gold via PySpark — medallion architecture |
| **Third-party API** | Massive/Polygon REST API — OHLCV, fundamentals, news (20 tickers) |
| **Unstructured data + RAG** | News articles embedded via BGE Large → Databricks AI Search index |
| **Databricks App** | Gradio chat UI hosted on Databricks Apps with live market data |
| **AI Agent with tools** | Llama 3.3 70B + 8 function-calling tools — reads Gold, writes to Delta |
| **Lakebase CDF → Delta** | Delta CDF on Silver tables → `main.analytics.cdf_events` |

---

## 🗂️ Repository Structure

```
ai-stock-research-assistant/
│
├── pipeline/
│   ├── 00_setup_config.ipynb         # Ticker registry — Unity Catalog config table
│   ├── 01_bronze_ingestion.ipynb     # Polygon API → Bronze Delta (append)
│   ├── 02_silver_transform.ipynb     # Dedup · cast · enrich → Silver (overwrite)
│   ├── 03_gold_aggregates.ipynb      # 4 analytics tables → Gold (overwrite)
│   └── 05_sync_index.ipynb           # AI Search index sync (Workflow task 4)
│
├── embeddings/
│   └── 04_embed_and_index.ipynb      # BGE Large embeddings → AI Search index
│
├── lakebase/
│   ├── 05_schema_ddl.sql             # Lakebase Postgres schema (8 tables + CDF)
│   └── grants.sql                    # Unity Catalog grants — run once before App
│
├── cdf/
│   └── 06_cdf_to_delta.ipynb         # Delta CDF → main.analytics.cdf_events
│
├── agent/
│   └── 07_agent_tools.ipynb          # AI Agent — 8 tools tested end-to-end
│
├── app/
│   ├── app.py                        # Gradio frontend — deployed on Databricks Apps
│   └── requirements.txt              # App dependencies
│
├── .gitignore
├── README.md
└── SETUP.md                          # Full implementation guide with all fixes
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Platform** | Databricks Free Edition (AWS, Unity Catalog) |
| **Ingestion** | PySpark · Massive/Polygon REST API · 13s rate-limit handling |
| **Storage** | Delta Lake — Bronze/Silver/Gold medallion · Lakebase Postgres 17 |
| **Governance** | Unity Catalog · Secret Scopes · `account users` grants |
| **Semantic search** | Databricks AI Search · BGE Large embeddings · Delta Sync index |
| **Change tracking** | Delta CDF (`enableChangeDataFeed`) → analytics Delta table |
| **AI Model** | `databricks-meta-llama-3-3-70b-instruct` — Foundation Models API |
| **Agent pattern** | OpenAI-compatible function calling · 5-round agentic loop |
| **Frontend** | Gradio · Databricks Apps · 3-column layout |
| **Orchestration** | Databricks Workflows — 4-task DAG · Mon–Fri cron schedule |
| **Version control** | GitHub · Databricks Git Repos integration |
| **Secrets** | `dbutils.secrets` · `getpass` for interactive entry |

---

## 📊 Tracked Tickers (20)

| Sector | Tickers |
|---|---|
| **Technology** | AAPL · GOOGL · META · MSFT · NVDA |
| **Finance** | BAC · GS · JPM · MS · V |
| **Healthcare** | ABBV · JNJ · MRK · PFE · UNH |
| **Consumer** | AMZN · TSLA · WMT |
| **Energy** | CVX · XOM |

Managed via `main.config.ticker_config` — add/remove tickers without code changes.

---

## 🗄️ Lakebase Schema (8 tables)

| Table | Purpose |
|---|---|
| `users` | Registered users |
| `watchlists` | Named watchlists per user |
| `watchlist_tickers` | Tickers per watchlist |
| `companies` | Fundamentals — market cap, sector, description |
| `price_snapshots` | OHLCV per ticker per timestamp |
| `news_articles` | Raw news text (also embedded for RAG) |
| `research_notes` | User-authored notes per ticker |
| `analysis_reports` | Agent-generated reports per session |

`REPLICA IDENTITY FULL` enabled on all tables for Change Data Feed.

---

## 🏅 Gold Layer Tables

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
| `get_price_data` | Read | `main.gold.ticker_daily_summary` | Price, return, market cap |
| `get_sentiment` | Read | `main.gold.sentiment_summary` | Sentiment signal + confidence |
| `compare_tickers` | Read | `main.gold.ticker_daily_summary` | Side-by-side comparison |
| `get_top_movers` | Read | `main.gold.top_movers` | Gainers and losers |
| `search_news` | Read | AI Search index (RAG) | Semantic news search |
| `add_to_watchlist` | Write | `main.agent.watchlists` | Save ticker to watchlist |
| `save_research_note` | Write | `main.agent.research_notes` | Persist research note |
| `save_analysis_report` | Write | `main.agent.analysis_reports` | Log agent report |

**Agentic loop:** User query → LLM picks tools → execute → results back to LLM → repeat up to 5 rounds → final response.

---

## ⚙️ Workflow Automation

**Job:** `stock-assistant-daily-pipeline`

| Task | Notebook | Depends on |
|---|---|---|
| `bronze_ingestion` | `pipeline/01_bronze_ingestion` | — |
| `silver_transform` | `pipeline/02_silver_transform` | bronze_ingestion |
| `gold_aggregates` | `pipeline/03_gold_aggregates` | silver_transform |
| `sync_vs_index` | `pipeline/05_sync_index` | gold_aggregates |

**Schedule:** `0 0 22 ? * MON-FRI` — 10 PM IST daily (after US market close)

---

## 🚀 Deployment Guide

### Prerequisites
- Databricks Free Edition account — [signup](https://databricks.com)
- LinkedIn verified (unlocks outbound internet)
- Massive/Polygon API key — [signup](https://massive.com) (free)

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
- **Suggestion panel** — 7 pre-built quick-question buttons
- **Chat** — Blue user bubbles · Dark assistant bubbles
- **Input** — Press Enter to send (Claude-style)
- **Market Summary** — Live prices for all 20 tickers
- **Watchlist** — Shows saved tickers from agent interactions

---

## 👤 Author

**Jayanth Dolai** — Senior Data Engineer  
6+ years | Azure · Databricks · Microsoft Fabric  
Certifications: Databricks Data Engineer Associate · DP-700 · DP-600 · DP-900 · Generative AI Associate

[LinkedIn](https://www.linkedin.com/in/jayanth-dolai/) · [GitHub](https://github.com/demonjd2026-afk)

---

## 📄 License

MIT — see [LICENSE](LICENSE)
