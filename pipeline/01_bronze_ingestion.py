# =============================================================
# AI Stock Market Research Assistant — Bronze Ingestion Pipeline
# File    : pipeline/01_bronze_ingestion.py
# Layer   : Bronze (raw, append-only Delta tables)
# Schedule: Run daily via Databricks Workflow
# =============================================================
# What this notebook does:
#   1. Reads Massive Stocks API key from Databricks Secrets
#   2. Pulls company fundamentals, OHLCV price snapshots, and
#      news articles for a defined set of tickers
#   3. Writes raw JSON-derived data to three Bronze Delta tables
#      under the main.bronze schema
# =============================================================

# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer — Raw Ingestion from Massive Stocks API
# MAGIC **Catalog:** `main` | **Schema:** `bronze`
# MAGIC
# MAGIC Tables written:
# MAGIC - `main.bronze.raw_companies`
# MAGIC - `main.bronze.raw_price_snapshots`
# MAGIC - `main.bronze.raw_news_articles`

# COMMAND ----------

# ------------------------------------------------------------------
# 0. Imports and config
# ------------------------------------------------------------------
import requests
import json
import uuid
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, DateType, BooleanType
)

spark = SparkSession.builder.getOrCreate()

# Batch ID — unique per run, used for lineage tracking
BATCH_ID   = str(uuid.uuid4())
RUN_DATE   = date.today().isoformat()          # e.g. "2026-08-04"
INGESTED_AT = datetime.utcnow().isoformat()

print(f"Batch ID   : {BATCH_ID}")
print(f"Run date   : {RUN_DATE}")
print(f"Ingested at: {INGESTED_AT}")

# COMMAND ----------

# ------------------------------------------------------------------
# 1. Load API key from Databricks Secrets (never hardcoded)
# ------------------------------------------------------------------
API_KEY  = dbutils.secrets.get(scope="capstone", key="massive_api_key")
BASE_URL = "https://api.massive.com/v1"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

print(f"API key loaded: {len(API_KEY)} characters")

# COMMAND ----------

# ------------------------------------------------------------------
# 2. Tickers to track
#    Diversified across sectors — adjust freely
# ------------------------------------------------------------------
TICKERS = [
    # Technology
    "AAPL", "MSFT", "GOOGL", "NVDA", "META",
    # Finance
    "JPM", "BAC", "GS", "MS", "V",
    # Healthcare
    "JNJ", "UNH", "PFE", "ABBV", "MRK",
    # Energy
    "XOM", "CVX",
    # Consumer
    "AMZN", "TSLA", "WMT"
]

print(f"Tracking {len(TICKERS)} tickers: {', '.join(TICKERS)}")

# COMMAND ----------

# ------------------------------------------------------------------
# 3. Helper — safe API call with retry and error logging
# ------------------------------------------------------------------
def api_get(endpoint: str, params: dict = None, retries: int = 3) -> dict | None:
    """
    Makes a GET request to the Massive API.
    Returns parsed JSON dict on success, None on failure.
    """
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                print(f"  Rate limited on {endpoint} — waiting 10s (attempt {attempt})")
                import time; time.sleep(10)
            else:
                print(f"  HTTP {resp.status_code} on {endpoint}: {resp.text[:200]}")
                return None
        except requests.RequestException as e:
            print(f"  Request error on {endpoint} (attempt {attempt}): {e}")
    return None

# COMMAND ----------

# ------------------------------------------------------------------
# 4. Setup Bronze schema in Unity Catalog
# ------------------------------------------------------------------
spark.sql("CREATE CATALOG IF NOT EXISTS main")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.bronze")
print("Schema main.bronze ready")

# COMMAND ----------

# ------------------------------------------------------------------
# 5. Ingest Company Fundamentals → main.bronze.raw_companies
# ------------------------------------------------------------------
print("\n--- Ingesting company fundamentals ---")

companies_rows = []

for ticker in TICKERS:
    print(f"  Fetching company: {ticker}", end=" ")
    data = api_get(f"stocks/tickers/{ticker}", params={"date": RUN_DATE})

    if data and "results" in data:
        r = data["results"]
        companies_rows.append({
            "batch_id"        : BATCH_ID,
            "run_date"        : RUN_DATE,
            "ticker"          : ticker,
            "name"            : r.get("name"),
            "exchange"        : r.get("primary_exchange"),
            "market_cap"      : r.get("market_cap"),
            "share_class_shares_outstanding": r.get("share_class_shares_outstanding"),
            "weighted_shares_outstanding"   : r.get("weighted_shares_outstanding"),
            "description"     : r.get("description"),
            "homepage_url"    : r.get("homepage_url"),
            "total_employees" : r.get("total_employees"),
            "list_date"       : r.get("list_date"),
            "sic_code"        : r.get("sic_code"),
            "sic_description" : r.get("sic_description"),
            "locale"          : r.get("locale"),
            "active"          : r.get("active"),
            "raw_json"        : json.dumps(r),
            "ingested_at"     : INGESTED_AT
        })
        print("✓")
    else:
        print("✗ skipped")

# Write to Delta
if companies_rows:
    df_companies = spark.createDataFrame(companies_rows)
    (
        df_companies
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("main.bronze.raw_companies")
    )
    print(f"\nWritten {len(companies_rows)} company records to main.bronze.raw_companies")
else:
    print("No company records to write")

# COMMAND ----------

# ------------------------------------------------------------------
# 6. Ingest Price Snapshots (OHLCV) → main.bronze.raw_price_snapshots
# ------------------------------------------------------------------
print("\n--- Ingesting OHLCV price snapshots ---")

# Fetch last 7 days to backfill any missed trading days
date_range = [
    (date.today() - timedelta(days=i)).isoformat()
    for i in range(7)
]

price_rows = []

for ticker in TICKERS:
    print(f"  Fetching OHLCV: {ticker}", end=" ")

    data = api_get(
        f"stocks/aggregates/daily-ticker-summary",
        params={
            "ticker"    : ticker,
            "date"      : RUN_DATE,
            "adjusted"  : "true"
        }
    )

    if data and "results" in data:
        r = data["results"]
        price_rows.append({
            "batch_id"    : BATCH_ID,
            "run_date"    : RUN_DATE,
            "ticker"      : ticker,
            "open"        : r.get("o"),
            "high"        : r.get("h"),
            "low"         : r.get("l"),
            "close"       : r.get("c"),
            "volume"      : r.get("v"),
            "vwap"        : r.get("vw"),
            "transactions": r.get("n"),
            "pre_market"  : r.get("preMarket"),
            "after_hours" : r.get("afterHours"),
            "snapshot_date": RUN_DATE,
            "raw_json"    : json.dumps(r),
            "ingested_at" : INGESTED_AT
        })
        print("✓")
    else:
        print("✗ skipped")

# Write to Delta
if price_rows:
    df_prices = spark.createDataFrame(price_rows)
    (
        df_prices
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("main.bronze.raw_price_snapshots")
    )
    print(f"\nWritten {len(price_rows)} price records to main.bronze.raw_price_snapshots")
else:
    print("No price records to write")

# COMMAND ----------

# ------------------------------------------------------------------
# 7. Ingest News Articles → main.bronze.raw_news_articles
# ------------------------------------------------------------------
print("\n--- Ingesting news articles ---")

news_rows = []

for ticker in TICKERS:
    print(f"  Fetching news: {ticker}", end=" ")

    data = api_get(
        "stocks/news",
        params={
            "ticker"     : ticker,
            "published_utc.gte": (date.today() - timedelta(days=7)).isoformat(),
            "order"      : "desc",
            "limit"      : 10
        }
    )

    if data and "results" in data:
        articles = data["results"]
        for article in articles:
            news_rows.append({
                "batch_id"      : BATCH_ID,
                "run_date"      : RUN_DATE,
                "ticker"        : ticker,
                "article_id"    : article.get("id"),
                "title"         : article.get("title"),
                "author"        : article.get("author"),
                "published_utc" : article.get("published_utc"),
                "article_url"   : article.get("article_url"),
                "image_url"     : article.get("image_url"),
                "description"   : article.get("description"),
                "keywords"      : json.dumps(article.get("keywords", [])),
                "publisher_name": article.get("publisher", {}).get("name"),
                "publisher_url" : article.get("publisher", {}).get("homepage_url"),
                "sentiment"     : article.get("insights", [{}])[0].get("sentiment") if article.get("insights") else None,
                "sentiment_reasoning": article.get("insights", [{}])[0].get("sentiment_reasoning") if article.get("insights") else None,
                "raw_json"      : json.dumps(article),
                "ingested_at"   : INGESTED_AT
            })
        print(f"✓ ({len(articles)} articles)")
    else:
        print("✗ skipped")

# Write to Delta
if news_rows:
    df_news = spark.createDataFrame(news_rows)
    (
        df_news
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("main.bronze.raw_news_articles")
    )
    print(f"\nWritten {len(news_rows)} news records to main.bronze.raw_news_articles")
else:
    print("No news records to write")

# COMMAND ----------

# ------------------------------------------------------------------
# 8. Verification — row counts per Bronze table
# ------------------------------------------------------------------
print("\n=== Bronze Ingestion Summary ===")
print(f"Batch ID : {BATCH_ID}")
print(f"Run date : {RUN_DATE}")
print()

for table in ["raw_companies", "raw_price_snapshots", "raw_news_articles"]:
    try:
        count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM main.bronze.{table} "
            f"WHERE batch_id = '{BATCH_ID}'"
        ).collect()[0]["cnt"]
        total = spark.sql(
            f"SELECT COUNT(*) as cnt FROM main.bronze.{table}"
        ).collect()[0]["cnt"]
        print(f"  main.bronze.{table:<25} this run: {count:>4}  |  total: {total:>6}")
    except Exception as e:
        print(f"  main.bronze.{table:<25} ERROR: {e}")

print("\nBronze ingestion complete ✓")
