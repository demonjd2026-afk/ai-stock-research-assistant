# 📈 AI Stock Market Research Assistant

> An AI-powered stock research platform built on the **Databricks Lakehouse** — combining real-time market data ingestion via the Massive Stocks API, semantic RAG over financial news and earnings filings, and an agentic frontend that reads and writes to a Lakebase-backed pipeline.

Built as part of the [Databricks AI Bootcamp Capstone](https://github.com/EcZachly/databricks-ai-bootcamp-capstone) by Zach Wilson.

---

## 🚦 Build Progress

| Phase | Description | Status |
|---|---|---|
| **Phase 0** | Workspace setup — GitHub, Databricks, Lakebase, Secrets | ✅ Complete |
| **Phase 1** | Lakebase schema — 8 tables, CDF enabled | ✅ Complete |
| **Phase 2** | Bronze ingestion — Massive Stocks API → Delta | ✅ Complete |
| **Phase 3** | Silver transformation — cleaning + normalization | ✅ Complete |
| **Phase 4** | Gold aggregates — analytics-ready tables | ✅ Complete |
| **Phase 5** | Embeddings — news/filings → Vector Search index | ✅ Complete |
| **Phase 6** | CDF pipeline — Lakebase → Delta analytics table | ✅ Complete |
| **Phase 7** | AI Agent — tools for read + write actions | ✅ Complete |
| **Phase 8** | Databricks App — Gradio frontend | ✅ Complete |
| **Workflow** | Daily automated pipeline (Mon-Fri 10 PM IST) | ✅ Complete |

---

## 🏗️ Architecture Overview

```
                        ┌─────────────────────┐
                        │   Massive Stocks API  │
                        │  (REST — ticks, OHLCV,│
                        │  fundamentals, news)  │
                        └──────────┬────────────┘
                                   │  PySpark Ingestion
                    ┌──────────────▼──────────────┐
                    │           BRONZE             │
                    │   Raw Delta tables (append)  │
                    └──────────────┬───────────────┘
                                   │  Cleaning + Dedup
                    ┌──────────────▼──────────────┐
                    │           SILVER             │
                    │  Normalized + typed tables   │
                    └──────────────┬───────────────┘
                                   │  Aggregation
                    ┌──────────────▼──────────────┐
                    │            GOLD              │
                    │  Analytics-ready aggregates  │
                    └──────┬──────────────┬────────┘
                           │              │
             ┌─────────────▼──┐    ┌──────▼──────────────┐
             │    Lakebase     │    │   Vector Search      │
             │ (Postgres OLTP) │    │  Index (news text +  │
             │  8 core tables  │    │  earnings summaries) │
             └────────┬────────┘    └──────────┬───────────┘
                      │  CDF                    │  Semantic RAG
          ┌───────────▼────────┐                │
          │  Analytics Delta   │       ┌─────────▼──────────┐
          │  (usage tracking,  │       │      AI Agent       │
          │  agent tool calls) │       │  Tools: read price, │
          └────────────────────┘       │  search news, write │
                                       │  watchlists/notes   │
                                       └─────────┬───────────┘
                                                 │
                                       ┌─────────▼───────────┐
                                       │   Databricks App     │
                                       │  (Gradio Frontend)   │
                                       └─────────────────────┘
```

---

## ✅ Capstone Requirements Coverage

| Requirement | Implementation |
|---|---|
| **Spark data pipeline** | Bronze → Silver → Gold via PySpark (medallion architecture) |
| **Third-party API** | Massive Stocks API — real-time ticks, OHLCV, fundamentals, news |
| **Unstructured data** | Embeddings over news articles + earnings call text → Vector Search index |
| **Databricks App frontend** | Gradio UI hosted and served via Databricks Apps |
| **AI Agent with tools** | Mosaic AI Agent — reads market data, writes watchlists + research notes |
| **Lakebase CDF → Delta** | Change Data Feed streams Lakebase writes into a Delta analytics table |

---

## 🗂️ Project Structure

```
ai-stock-research-assistant/
│
├── pipeline/
│   ├── 00_setup_config.ipynb         # Ticker registry in Unity Catalog
│   ├── 01_bronze_ingestion.ipynb     # Polygon API → Bronze Delta tables
│   ├── 02_silver_transform.ipynb     # Clean, deduplicate, enrich → Silver
│   ├── 03_gold_aggregates.ipynb      # Analytics aggregates → Gold (4 tables)
│   └── 05_sync_index.ipynb           # Vector Search index sync (daily workflow)
│
├── embeddings/
│   └── 04_embed_and_index.ipynb      # BGE embeddings → Databricks AI Search index
│
├── lakebase/
│   ├── 05_schema_ddl.sql             # Lakebase Postgres schema (8 tables)
│   └── grants.sql                    # Unity Catalog grants (run once)
│
├── cdf/
│   └── 06_cdf_to_delta.ipynb         # Delta CDF → analytics table
│
├── agent/
│   └── 07_agent_tools.ipynb          # AI Agent — 8 tools (5 read + 3 write)
│
├── app/
│   ├── app.py                        # Databricks App — Gradio chat frontend
│   └── requirements.txt              # App Python dependencies
│
├── .gitignore
├── README.md
└── SETUP.md                          # Full setup + implementation guide
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Cloud platform** | Microsoft Azure |
| **Data platform** | Databricks (Unity Catalog enabled) |
| **Compute** | DBR 15.4 LTS ML, Serverless SQL Warehouse |
| **Ingestion** | PySpark, Massive Stocks REST API |
| **Storage — batch** | Delta Lake (Bronze / Silver / Gold medallion) |
| **Storage — OLTP** | Lakebase (Postgres-compatible, Unity Catalog managed) |
| **Semantic search** | Databricks Vector Search + BGE embeddings |
| **Change tracking** | Lakebase Change Data Feed → Delta table |
| **AI agent** | Databricks Mosaic AI Agent Framework (MLflow-traced) |
| **Frontend** | Databricks Apps (Gradio) |
| **Orchestration** | Databricks Workflows (multi-task job) |
| **Secrets** | Databricks Secret Scopes (`dbutils.secrets`) |
| **Version control** | GitHub + Databricks Git Repos integration |

---

## 🗄️ Lakebase Schema

Eight relational tables in Postgres-compatible Lakebase:

| Table | Purpose |
|---|---|
| `users` | Registered users (id, name, email, created_at) |
| `watchlists` | Named watchlists per user |
| `watchlist_tickers` | Tickers within each watchlist + add date |
| `companies` | Company profiles, sector, market cap, fundamentals snapshot |
| `price_snapshots` | OHLCV snapshots per ticker per timestamp |
| `news_articles` | Raw news text per ticker (also embedded for RAG) |
| `research_notes` | User-authored notes per ticker |
| `analysis_reports` | Agent-generated reports logged per session |

Change Data Feed is enabled on all tables, streaming writes into a Delta analytics table for usage and agent-action tracking.

---

## 🤖 AI Agent Capabilities

The agent uses **Databricks Foundation Models API** (`databricks-meta-llama-3-3-70b-instruct`) with OpenAI-compatible function calling. An agentic loop executes tools iteratively until the model produces a final response.

### Tools

| Tool | Type | Data Source | Description |
|---|---|---|---|
| `get_price_data` | **Read** | `main.gold.ticker_daily_summary` | Current price, daily return, market cap |
| `get_sentiment` | **Read** | `main.gold.sentiment_summary` | BULLISH/NEUTRAL/BEARISH signal + confidence |
| `compare_tickers` | **Read** | `main.gold.ticker_daily_summary` | Side-by-side ticker comparison |
| `get_top_movers` | **Read** | `main.gold.top_movers` | Today's top gainers and losers |
| `search_news` | **Read** | Vector Search index (RAG) | Semantic search over news articles |
| `add_to_watchlist` | **Write** | `main.agent.watchlists` | Save ticker to user watchlist |
| `save_research_note` | **Write** | `main.agent.research_notes` | Persist user note per ticker |
| `save_analysis_report` | **Write** | `main.agent.analysis_reports` | Log agent-generated report |

### Agentic Loop

```
User query
     ↓
LLM (Llama 3.3 70B) decides which tools to call
     ↓
Tool execution → results sent back to LLM
     ↓
LLM synthesizes response (repeats up to 5 rounds)
     ↓
Final response returned to user
```

### Confirmed Working (Test Run)

| Query | Tools Called | Result |
|---|---|---|
| Apple price + sentiment | `get_price_data` + `get_sentiment` + `add_to_watchlist` | AAPL $303.42, -1.99%, neutral sentiment |
| MSFT vs NVDA comparison | `compare_tickers` + `save_research_note` + `add_to_watchlist` | NVDA wins (+4.53% vs +2.42%) |
| Top movers today | `get_top_movers` | META +4.98% top gainer, AAPL -1.99% loser |
| AI news search + note | `search_news` (RAG) + `save_research_note` | Semantic search returned NVDA AI articles |
| Add to watchlist | `add_to_watchlist` × 2 | AAPL + MSFT added to Tech Watchlist |

**Agent write tables after test run:**
- `main.agent.watchlists` — 4 rows (MSFT + AAPL in Tech Watchlist, NVDA + AAPL in My Watchlist)
- `main.agent.research_notes` — 2 rows (NVDA momentum + AI sector notes)
- `main.agent.analysis_reports` — 0 rows (agent chose not to write a report)

### Live App Output (confirmed working)

```
Market Summary (Refresh Market button):
  META:  $590.24 (+4.98%) [UP]
  NVDA:  $206.64 (+4.53%) [UP]
  GOOGL: $377.65 (+2.56%) [UP]
  MSFT:  $492.81 (+2.48%) [UP]
  AAPL:  $303.42 (-1.99%) [DOWN]

Watchlist (Refresh Watchlist button):
  MSFT - Tech Watchlist
  AAPL - Tech Watchlist
  NVDA - My Watchlist
  AAPL - My Watchlist
```

---

## 🚀 Setup & Deployment

### Prerequisites

- Databricks workspace on **Azure** with **Unity Catalog** enabled
- Serverless SQL Warehouse configured
- Databricks Runtime **15.4 LTS ML** or later
- **Databricks Apps** enabled in the workspace
- Free [Massive Stocks API key](https://massive.com)

### Step 1 — Clone into Databricks Repos

In your Databricks workspace navigate to **Workspace → Repos → Add Repo** and paste:

```
https://github.com/demonjd2026-afk/ai-stock-research-assistant
```

### Step 2 — Store API key as a Databricks Secret

```bash
databricks secrets create-scope capstone
databricks secrets put --scope capstone --key massive_api_key
```

Reference in notebooks:

```python
api_key = dbutils.secrets.get(scope="capstone", key="massive_api_key")
```

### Step 3 — Run notebooks in order

```
pipeline/00_setup_config.ipynb      →  configure ticker registry (Unity Catalog)
pipeline/01_bronze_ingestion.ipynb  →  ingest raw market data (Polygon API)
pipeline/02_silver_transform.ipynb  →  clean, deduplicate, enrich
pipeline/03_gold_aggregates.ipynb   →  build 4 analytics aggregates
embeddings/04_embed_and_index.ipynb →  create Vector Search index (BGE Large)
lakebase/05_schema_ddl.sql          →  create Lakebase tables (Lakebase SQL Editor)
cdf/06_cdf_to_delta.ipynb           →  CDF → Delta analytics table
agent/07_agent_tools.ipynb          →  AI Agent with 8 tools
app/app.py                          →  deploy via Databricks Apps (Phase 8)
```

### Step 4 — Deploy the Databricks App

1. In Databricks navigate to **Compute → Apps → Create App**
2. Name: `stock-research-assistant`
3. Point it at `app/app.py` in your Git folder
4. The app auto-detects Gradio and deploys with a public URL
5. Open the URL — the chat interface loads with live market data

---

## 📦 Requirements

```
databricks-sdk>=0.20.0
mlflow>=2.13.0
gradio>=4.0.0
pandas>=2.0.0
requests>=2.31.0
psycopg2-binary>=2.9.0
```

---

## 📊 Analytics (CDF → Delta)

The **Change Data Feed** on every Lakebase table streams row-level changes into a `capstone.analytics.cdf_events` Delta table, capturing:

- Which agent tool was called and by whom
- Watchlist additions and removals over time
- Research note creation frequency
- Price snapshot ingestion volume per run

This Delta table powers a Gold-layer dashboard notebook showing app usage and agent behaviour over time.

---

## 👤 Author

**Jayanth Dolai** — Senior Data Engineer  
6+ years | Azure · Databricks · Microsoft Fabric  
Certifications: Databricks Data Engineer Associate · DP-700 · DP-600 · DP-900 · Databricks Generative AI Associate

[LinkedIn](https://www.linkedin.com/in/jayanth-dolai/) · [GitHub](https://github.com/demonjd2026-afk)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
