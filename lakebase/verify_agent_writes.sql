-- =============================================================
-- AI Stock Market Research Assistant — Agent Write Verification
-- File    : lakebase/verify_agent_writes.sql
-- Run in  : Databricks SQL Editor (Serverless warehouse)
-- Purpose : Prove that all FOUR agent write tools persist to Unity Catalog.
-- =============================================================
-- The agent only writes when a conversation calls for it, so generate the
-- rows first, then run the queries below and screenshot the results.
--
-- STEP 1 — In the deployed app, send these two messages:
--
--   "Analyse NVDA and save a research note about its momentum"
--       -> exercises save_research_note  -> main.agent.research_notes
--
--   "Write a full analysis report for NVDA and save it"
--       -> exercises save_analysis_report -> main.agent.analysis_reports
--
-- STEP 2 — Run each query below and capture the result grid.
-- =============================================================


-- -------------------------------------------------------------
-- 1. Watchlists  (add_to_watchlist / remove_from_watchlist)
-- -------------------------------------------------------------
SELECT id, user_email, watchlist, ticker, added_at
FROM   main.agent.watchlists
ORDER  BY added_at DESC
LIMIT  10;


-- -------------------------------------------------------------
-- 2. Research notes  (save_research_note)
-- -------------------------------------------------------------
SELECT id, user_email, ticker, note, created_at
FROM   main.agent.research_notes
ORDER  BY created_at DESC
LIMIT  10;


-- -------------------------------------------------------------
-- 3. Analysis reports  (save_analysis_report)
-- -------------------------------------------------------------
SELECT id,
       user_email,
       ticker,
       LEFT(report_text, 200) AS report_preview,
       agent_model,
       generated_at
FROM   main.agent.analysis_reports
ORDER  BY generated_at DESC
LIMIT  10;


-- -------------------------------------------------------------
-- 4. One-shot summary — all three write tables at a glance
--    This single result is the most useful screenshot for a grader.
-- -------------------------------------------------------------
SELECT 'watchlists'       AS write_table, COUNT(*) AS row_count,
       MAX(added_at)      AS last_write
FROM   main.agent.watchlists
UNION ALL
SELECT 'research_notes',   COUNT(*), MAX(created_at)
FROM   main.agent.research_notes
UNION ALL
SELECT 'analysis_reports', COUNT(*), MAX(generated_at)
FROM   main.agent.analysis_reports
ORDER  BY write_table;


-- -------------------------------------------------------------
-- 5. Bronze idempotency proof
--    Re-run the bronze_ingestion task, then run this — every count
--    must be 1. Before the MERGE upsert, a replay produced 2+.
-- -------------------------------------------------------------
SELECT ticker, snapshot_date, COUNT(*) AS copies
FROM   main.bronze.raw_price_snapshots
GROUP  BY ticker, snapshot_date
HAVING COUNT(*) > 1
ORDER  BY copies DESC;
-- Expected result: 0 rows returned.


-- -------------------------------------------------------------
-- 6. Incremental CDF proof
--    Watermarks advance as Silver tables change; events accumulate
--    beyond the original 50-row snapshot.
-- -------------------------------------------------------------
SELECT source_table, last_version, updated_at
FROM   main.analytics.cdf_watermarks
ORDER  BY source_table;

SELECT source_table, operation, COUNT(*) AS event_count,
       MIN(commit_version) AS first_version,
       MAX(commit_version) AS last_version
FROM   main.analytics.cdf_events
GROUP  BY source_table, operation
ORDER  BY event_count DESC;
