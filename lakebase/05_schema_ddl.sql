-- =============================================================
-- AI Stock Market Research Assistant — Lakebase Schema DDL
-- File    : lakebase/05_schema_ddl.sql
-- Engine  : Lakebase (Databricks-managed Postgres 17)
-- Database: databricks_postgres
-- Run in  : Lakebase SQL Editor (stock-assistant → production → SQL Editor)
-- =============================================================
-- Run statements in order top to bottom.
-- REPLICA IDENTITY FULL enables Change Data Feed (CDF) on every
-- table so that 06_cdf_to_delta.py can consume row-level changes.
-- =============================================================


-- -------------------------------------------------------------
-- 0. Schema
-- -------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS stock_assistant;
SET search_path TO stock_assistant;


-- -------------------------------------------------------------
-- 1. users
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.users (
    id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(120)  NOT NULL,
    email       VARCHAR(255)  NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

ALTER TABLE stock_assistant.users REPLICA IDENTITY FULL;


-- -------------------------------------------------------------
-- 2. watchlists
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.watchlists (
    id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID          NOT NULL REFERENCES stock_assistant.users(id) ON DELETE CASCADE,
    name        VARCHAR(120)  NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    UNIQUE (user_id, name)
);

ALTER TABLE stock_assistant.watchlists REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS idx_watchlists_user
    ON stock_assistant.watchlists(user_id);


-- -------------------------------------------------------------
-- 3. watchlist_tickers
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.watchlist_tickers (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    watchlist_id  UUID        NOT NULL REFERENCES stock_assistant.watchlists(id) ON DELETE CASCADE,
    ticker        VARCHAR(10) NOT NULL,
    added_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (watchlist_id, ticker)
);

ALTER TABLE stock_assistant.watchlist_tickers REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS idx_wt_watchlist
    ON stock_assistant.watchlist_tickers(watchlist_id);
CREATE INDEX IF NOT EXISTS idx_wt_ticker
    ON stock_assistant.watchlist_tickers(ticker);


-- -------------------------------------------------------------
-- 4. companies
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.companies (
    ticker        VARCHAR(10)   PRIMARY KEY,
    name          VARCHAR(255)  NOT NULL,
    sector        VARCHAR(120),
    industry      VARCHAR(180),
    exchange      VARCHAR(20),
    market_cap    NUMERIC(20,2),
    description   TEXT,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

ALTER TABLE stock_assistant.companies REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS idx_companies_sector
    ON stock_assistant.companies(sector);


-- -------------------------------------------------------------
-- 5. price_snapshots
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.price_snapshots (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker       VARCHAR(10) NOT NULL REFERENCES stock_assistant.companies(ticker) ON DELETE CASCADE,
    open         NUMERIC(12,4),
    high         NUMERIC(12,4),
    low          NUMERIC(12,4),
    close        NUMERIC(12,4),
    volume       BIGINT,
    snapshot_ts  TIMESTAMPTZ NOT NULL,
    UNIQUE (ticker, snapshot_ts)
);

ALTER TABLE stock_assistant.price_snapshots REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS idx_ps_ticker
    ON stock_assistant.price_snapshots(ticker);
CREATE INDEX IF NOT EXISTS idx_ps_ts
    ON stock_assistant.price_snapshots(snapshot_ts DESC);


-- -------------------------------------------------------------
-- 6. news_articles
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.news_articles (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker        VARCHAR(10)  NOT NULL REFERENCES stock_assistant.companies(ticker) ON DELETE CASCADE,
    headline      VARCHAR(512) NOT NULL,
    source        VARCHAR(120),
    url           TEXT,
    body          TEXT,
    published_at  TIMESTAMPTZ  NOT NULL,
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE stock_assistant.news_articles REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS idx_na_ticker
    ON stock_assistant.news_articles(ticker);
CREATE INDEX IF NOT EXISTS idx_na_pub
    ON stock_assistant.news_articles(published_at DESC);


-- -------------------------------------------------------------
-- 7. research_notes
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.research_notes (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID         NOT NULL REFERENCES stock_assistant.users(id) ON DELETE CASCADE,
    ticker      VARCHAR(10)  NOT NULL REFERENCES stock_assistant.companies(ticker) ON DELETE CASCADE,
    note        TEXT         NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE stock_assistant.research_notes REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS idx_rn_user
    ON stock_assistant.research_notes(user_id);
CREATE INDEX IF NOT EXISTS idx_rn_ticker
    ON stock_assistant.research_notes(ticker);


-- -------------------------------------------------------------
-- 8. analysis_reports
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_assistant.analysis_reports (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID         NOT NULL REFERENCES stock_assistant.users(id) ON DELETE CASCADE,
    ticker        VARCHAR(10)  NOT NULL REFERENCES stock_assistant.companies(ticker) ON DELETE CASCADE,
    report_text   TEXT         NOT NULL,
    agent_model   VARCHAR(80)  NOT NULL DEFAULT 'databricks-meta-llama-3-3-70b-instruct',
    generated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE stock_assistant.analysis_reports REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS idx_ar_user
    ON stock_assistant.analysis_reports(user_id);
CREATE INDEX IF NOT EXISTS idx_ar_ticker
    ON stock_assistant.analysis_reports(ticker);
CREATE INDEX IF NOT EXISTS idx_ar_gen
    ON stock_assistant.analysis_reports(generated_at DESC);


-- =============================================================
-- Verification — run after all tables are created
-- Should return 8 rows, one per table
-- =============================================================
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(
        (quote_ident('stock_assistant') || '.' || quote_ident(table_name))::regclass
    )) AS size
FROM information_schema.tables
WHERE table_schema = 'stock_assistant'
ORDER BY table_name;
