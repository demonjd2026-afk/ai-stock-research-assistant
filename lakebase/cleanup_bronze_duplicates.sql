-- =============================================================
-- AI Stock Market Research Assistant — One-Time Bronze Dedupe
-- File    : lakebase/cleanup_bronze_duplicates.sql
-- Run in  : Databricks SQL Editor (Serverless warehouse)
-- =============================================================
-- Removes duplicate rows left behind by the original append-only Bronze writes.
-- The MERGE upsert in 01_bronze_ingestion.ipynb prevents NEW duplicates; this
-- clears the backlog that predates it.
--
-- Keep rule: the EARLIEST ingested_at per key, so the batch_id of the run that
-- first saw each row survives — the same lineage the insert-if-absent news path
-- now protects going forward.
--
-- THIS DELETES ROWS. Run section by section, not all at once.
-- Every step is reversible via Delta time travel — see section 4 first.
-- =============================================================


-- -------------------------------------------------------------
-- 0. BEFORE — record these numbers, they are your proof
-- -------------------------------------------------------------
SELECT 'raw_companies'       AS tbl, COUNT(*) AS rows,
       COUNT(DISTINCT CONCAT(ticker,'|',run_date))      AS distinct_keys
FROM   main.bronze.raw_companies
UNION ALL
SELECT 'raw_price_snapshots', COUNT(*),
       COUNT(DISTINCT CONCAT(ticker,'|',snapshot_date))
FROM   main.bronze.raw_price_snapshots
UNION ALL
SELECT 'raw_news_articles',   COUNT(*),
       COUNT(DISTINCT CONCAT(article_id,'|',ticker))
FROM   main.bronze.raw_news_articles
ORDER  BY tbl;
-- rows > distinct_keys is the backlog. After section 2 they must be equal.


-- -------------------------------------------------------------
-- 1. Note the current version of each table (for rollback)
-- -------------------------------------------------------------
DESCRIBE HISTORY main.bronze.raw_companies       LIMIT 1;
DESCRIBE HISTORY main.bronze.raw_price_snapshots LIMIT 1;
DESCRIBE HISTORY main.bronze.raw_news_articles   LIMIT 1;
-- Write down the `version` for each. Section 4 restores to these.


-- -------------------------------------------------------------
-- 2. Dedupe — run one block at a time, verify, then move on
-- -------------------------------------------------------------

-- 2a. raw_companies — key (ticker, run_date)
CREATE OR REPLACE TABLE main.bronze._dedup_staging AS
SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY ticker, run_date
               ORDER BY ingested_at ASC) AS rn
    FROM main.bronze.raw_companies
) WHERE rn = 1;

INSERT OVERWRITE main.bronze.raw_companies
SELECT * FROM main.bronze._dedup_staging;

DROP TABLE main.bronze._dedup_staging;


-- 2b. raw_price_snapshots — key (ticker, snapshot_date)
CREATE OR REPLACE TABLE main.bronze._dedup_staging AS
SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY ticker, snapshot_date
               ORDER BY ingested_at ASC) AS rn
    FROM main.bronze.raw_price_snapshots
) WHERE rn = 1;

INSERT OVERWRITE main.bronze.raw_price_snapshots
SELECT * FROM main.bronze._dedup_staging;

DROP TABLE main.bronze._dedup_staging;


-- 2c. raw_news_articles — key (article_id, ticker)
-- Collapses multi-day copies of the same article down to first-seen, which is
-- exactly what update_existing=False now guarantees for future runs.
CREATE OR REPLACE TABLE main.bronze._dedup_staging AS
SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY article_id, ticker
               ORDER BY ingested_at ASC) AS rn
    FROM main.bronze.raw_news_articles
) WHERE rn = 1;

INSERT OVERWRITE main.bronze.raw_news_articles
SELECT * FROM main.bronze._dedup_staging;

DROP TABLE main.bronze._dedup_staging;


-- -------------------------------------------------------------
-- 3. AFTER — rows must now equal distinct_keys everywhere
-- -------------------------------------------------------------
SELECT 'raw_companies'       AS tbl, COUNT(*) AS rows,
       COUNT(DISTINCT CONCAT(ticker,'|',run_date))      AS distinct_keys
FROM   main.bronze.raw_companies
UNION ALL
SELECT 'raw_price_snapshots', COUNT(*),
       COUNT(DISTINCT CONCAT(ticker,'|',snapshot_date))
FROM   main.bronze.raw_price_snapshots
UNION ALL
SELECT 'raw_news_articles',   COUNT(*),
       COUNT(DISTINCT CONCAT(article_id,'|',ticker))
FROM   main.bronze.raw_news_articles
ORDER  BY tbl;

-- Explicit duplicate check — all three must return 0 rows.
SELECT ticker, run_date, COUNT(*) FROM main.bronze.raw_companies
GROUP BY ticker, run_date HAVING COUNT(*) > 1;

SELECT ticker, snapshot_date, COUNT(*) FROM main.bronze.raw_price_snapshots
GROUP BY ticker, snapshot_date HAVING COUNT(*) > 1;

SELECT article_id, ticker, COUNT(*) FROM main.bronze.raw_news_articles
GROUP BY article_id, ticker HAVING COUNT(*) > 1;

-- Lineage survived: several distinct batch_ids should remain, not just the newest.
SELECT batch_id, MIN(run_date) AS first_run, COUNT(*) AS rows
FROM   main.bronze.raw_news_articles
GROUP  BY batch_id ORDER BY first_run;


-- -------------------------------------------------------------
-- 4. ROLLBACK — if anything looks wrong
-- -------------------------------------------------------------
-- Substitute the versions recorded in section 1:
--
--   RESTORE TABLE main.bronze.raw_companies       TO VERSION AS OF <v>;
--   RESTORE TABLE main.bronze.raw_price_snapshots TO VERSION AS OF <v>;
--   RESTORE TABLE main.bronze.raw_news_articles   TO VERSION AS OF <v>;


-- -------------------------------------------------------------
-- 5. AFTERWARDS — rebuild downstream
-- -------------------------------------------------------------
-- Silver already dedups, so Silver/Gold row counts should not change; rerun to
-- confirm the pipeline is consistent end to end:
--   Jobs & Pipelines -> stock-assistant-daily-pipeline -> Run now
--
-- Then re-run lakebase/verify_agent_writes.sql query 5 — it should return 0 rows.
