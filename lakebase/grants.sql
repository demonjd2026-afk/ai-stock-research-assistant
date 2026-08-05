-- =============================================================
-- AI Stock Market Research Assistant — Unity Catalog Grants
-- File    : lakebase/grants.sql
-- Purpose : Grants required for the Databricks App to access
--           Delta tables. Run ONCE after initial pipeline setup.
-- Run in  : SQL Editor → Serverless warehouse
-- When    : After running all pipeline notebooks for the first time
--           AND before deploying or testing the Databricks App
-- =============================================================

-- -------------------------------------------------------------
-- 1. Catalog and schema access
--    Required so the App can navigate to the tables
-- -------------------------------------------------------------
GRANT USE CATALOG ON CATALOG main TO `account users`;
GRANT USE SCHEMA ON SCHEMA main.gold    TO `account users`;
GRANT USE SCHEMA ON SCHEMA main.silver  TO `account users`;
GRANT USE SCHEMA ON SCHEMA main.agent   TO `account users`;
GRANT USE SCHEMA ON SCHEMA main.analytics TO `account users`;
GRANT USE SCHEMA ON SCHEMA main.bronze  TO `account users`;
GRANT USE SCHEMA ON SCHEMA main.config  TO `account users`;

-- -------------------------------------------------------------
-- 2. Gold tables — read only
--    Used by the App sidebar (market summary, top movers)
--    and by the AI Agent tools (get_price_data, get_sentiment,
--    compare_tickers, get_top_movers)
-- -------------------------------------------------------------
GRANT SELECT ON TABLE main.gold.ticker_daily_summary TO `account users`;
GRANT SELECT ON TABLE main.gold.sentiment_summary    TO `account users`;
GRANT SELECT ON TABLE main.gold.top_movers           TO `account users`;
GRANT SELECT ON TABLE main.gold.sector_rankings      TO `account users`;

-- -------------------------------------------------------------
-- 3. Silver tables — read only
--    Used by the Agent search_news tool (fallback keyword search)
--    and by the Vector Search sync notebook
-- -------------------------------------------------------------
GRANT SELECT ON TABLE main.silver.news_articles   TO `account users`;
GRANT SELECT ON TABLE main.silver.news_for_search TO `account users`;

-- -------------------------------------------------------------
-- 4. Agent write tables — read + write
--    The AI Agent writes watchlists, notes, and reports here.
--    MODIFY is used instead of INSERT (Unity Catalog 1.0)
-- -------------------------------------------------------------
GRANT SELECT, MODIFY ON TABLE main.agent.watchlists        TO `account users`;
GRANT SELECT, MODIFY ON TABLE main.agent.research_notes    TO `account users`;
GRANT SELECT, MODIFY ON TABLE main.agent.analysis_reports  TO `account users`;

-- -------------------------------------------------------------
-- 5. Config table — read only
--    Pipeline reads active tickers from here
-- -------------------------------------------------------------
GRANT SELECT ON TABLE main.config.ticker_config TO `account users`;

-- =============================================================
-- Verification — run after grants to confirm access
-- =============================================================
SHOW GRANTS ON TABLE main.gold.ticker_daily_summary;
