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
| **Phase 5** | Embeddings — news/filings → Vector Search index | ⬜ Pending |
| **Phase 6** | CDF pipeline — Lakebase → Delta analytics table | ⬜ Pending |
| **Phase 7** | AI Agent — tools for read + write actions | ⬜ Pending |
| **Phase 8** | Databricks App — Gradio frontend | ⬜ Pending |

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
│   ├── 01_bronze_ingestion.py        # Massive Stocks API → raw Delta tables (PySpark)
│   ├── 02_silver_transform.py        # Cleaning, typing, deduplication
│   └── 03_gold_aggregates.py         # Sector rollups, moving averages, analytics tables
│
├── embeddings/
│   └── 04_embed_and_index.py         # Chunk + embed news/filings → Databricks Vector Index
│
├── lakebase/
│   └── 05_schema_ddl.sql             # Lakebase (Postgres OLTP) CREATE TABLE statements
│
├── cdf/
│   └── 06_cdf_to_delta.py            # Lakebase Change Data Feed → Delta analytics table
│
├── agent/
│   └── 07_agent_tools.py             # AI agent definition + tool implementations
│
├── app/
│   └── app.py                        # Databricks App — Gradio frontend
│
├── requirements.txt                  # Python dependencies
└── README.md
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

The agent is built with the **Databricks Mosaic AI Agent Framework** and has the following tools:

| Tool | Type | Description |
|---|---|---|
| `get_price_data` | **Read** | Pull current + historical OHLCV for a ticker |
| `search_news` | **Read** | Semantic search over embedded news articles and filings |
| `compare_tickers` | **Read** | Compare fundamentals or price action across multiple tickers |
| `flag_price_moves` | **Read** | Surface notable moves or news since the user's last visit |
| `add_to_watchlist` | **Write** | Add a ticker to a named watchlist in Lakebase |
| `remove_from_watchlist` | **Write** | Remove a ticker from a watchlist |
| `save_research_note` | **Write** | Persist a user note tied to a ticker |
| `save_analysis_report` | **Write** | Log an agent-generated report for a session |

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
01_bronze_ingestion.py      →  ingest raw market data into Delta
02_silver_transform.py      →  normalize and deduplicate
03_gold_aggregates.py       →  build analytics aggregates
04_embed_and_index.py       →  create Vector Search index from news text
05_schema_ddl.sql           →  create Lakebase tables (run in SQL editor)
06_cdf_to_delta.py          →  start CDF stream into analytics Delta table
07_agent_tools.py           →  register agent and tools
app.py                      →  deploy via Databricks Apps
```

### Step 4 — Deploy the Databricks App

In Databricks navigate to **Apps → Create App** and point it at `app/app.py`.

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

[LinkedIn](https://linkedin.com/in/YOUR_LINKEDIN_HANDLE) · [GitHub](https://github.com/demonjd2026-afk)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
