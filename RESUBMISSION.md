# Reviewer Note — Changes Since the Graded Snapshot

> **Please re-pull `main` before reviewing.** The previous evaluation was made against an
> earlier snapshot of this repository. Several items marked as missing or incomplete are
> addressed on the current `main`, and three of them existed at the time but weren't
> visible in the artifacts that were provided.

---

## 1. Items previously flagged as "Evidence Gaps"

These files were already in the repository — they were simply not part of the artifact set
supplied for review. No action was needed beyond pointing at them:

| Previously reported as missing | Where it lives |
|---|---|
| DDL for `main.config.ticker_config` | `pipeline/00_setup_config.ipynb`, cell 4 |
| Lakebase schema SQL | `lakebase/05_schema_ddl.sql` — 8 tables, `REPLICA IDENTITY FULL` on each |
| Shareable app URL | [stock-research-assistant-7474654640109575.aws.databricksapps.com](https://stock-research-assistant-7474654640109575.aws.databricksapps.com) — also in the README header |

The prior review also described the job as a **4-task DAG**. It is now **5 tasks**:
`cdf_to_delta` runs in parallel with `gold_aggregates`, both downstream of `silver_transform`.
See `screenshots/08_workflow_with_cdf.png` for the current run graph (run ID `165389275773657`).

Screenshots proving writes to `research_notes` and `analysis_reports` — the one genuine
evidence gap — can be reproduced with `lakebase/verify_agent_writes.sql`, which includes the
exact prompts to trigger those two tools before querying.

---

## 2. Deductions addressed

### Spark Data Pipeline (−1): Bronze was append-only

Bronze now writes through an **idempotent MERGE upsert** (`upsert_bronze()`,
`pipeline/01_bronze_ingestion.ipynb` cell 6) keyed on each feed's natural grain:

| Table | Merge key |
|---|---|
| `raw_companies` | `(ticker, run_date)` |
| `raw_price_snapshots` | `(ticker, snapshot_date)` |
| `raw_news_articles` | `(article_id, ticker)` |

A job retry or same-day re-run updates in place instead of appending a duplicate. The batch
is deduplicated on the key before the MERGE (the same article can arrive under two tickers,
which would otherwise fail with a multiple-source-rows-matched error), and the join uses
null-safe `<=>`. Query 5 in `lakebase/verify_agent_writes.sql` proves replay-safety.

News uses `update_existing=False` (insert-if-absent). Its key deliberately omits `run_date`
because the same article is re-returned for days; combined with `whenMatchedUpdateAll` that
would have rewritten every historical copy's `batch_id` and destroyed per-run lineage.
Prices and company profiles keep update-on-match — their keys include the date, so a match
is a same-day re-fetch where the newer value should win.

**Verified on 2026-08-06.** Delta's own MERGE metrics on `main.bronze.raw_companies`
(`DESCRIBE HISTORY`, version 9):

```
numSourceRows               : 20
numTargetRowsMatchedUpdated : 40
numTargetRowsInserted       : 0
```

Twenty source rows produced **zero inserts** against a table that already held two
append-era copies of that day. `raw_price_snapshots` shows the same 20 / 40 / 0.

The duplicate backlog left by the original append-only writes was then cleared once with
`lakebase/cleanup_bronze_duplicates.sql`:

| Table | Before | After | Key |
|---|---|---|---|
| `raw_companies` | 135 | 60 | `(ticker, run_date)` |
| `raw_price_snapshots` | 115 | 45 | `(ticker, snapshot_date)` |
| `raw_news_articles` | 757 | 212 | `(article_id, ticker)` |

`rows = distinct_keys` on all three afterwards, and six distinct `batch_id` values survived
across the three ingestion dates — the cleanup keeps the earliest `ingested_at` per key, so
the run that first saw each row is still identifiable.

### AI Agent Quality (−1): tool drift and string-built SQL

**Tool contract unified.** `flag_price_moves` had diverged between the notebook
(`ticker, threshold_pct`) and the app (`hours_back`). Both now expose one signature:

```python
flag_price_moves(ticker=None, threshold_pct=2.0)
#   ticker given   -> check that one ticker against the threshold
#   ticker omitted -> scan every tracked ticker
```

with a matching return shape (`threshold_pct`, `flagged`, `movers[]`, `message`) and an
identical OpenAI function schema on both sides.

**SQL hardened.** Every statement in `app/app.py` now binds values through named parameter
markers via the Statement Execution API:

```python
run_sql("... WHERE t.ticker = :ticker LIMIT 1", {"ticker": ticker.upper()})
```

No user- or LLM-supplied value is concatenated into a statement. `IN` lists generate marker
names from the loop index (never from caller input), and `LIMIT` sizes — where SQL forbids
markers — are clamped to bounded integers in Python. This removed the unescaped
`watchlist_name` path in `add_to_watchlist` / `remove_from_watchlist`.

**Verified on 2026-08-06.** A single natural-language request in the deployed app —
*"Write a full analysis report on NVDA covering price, sentiment and momentum, and save it"* —
caused the agent to select `save_analysis_report` on its own and persist row `ee14d000…` to
`main.agent.analysis_reports`. All three write tables are now populated
(`screenshots/18_agent_write_tables_summary.png`), closing the one genuine evidence gap.

The same session exercised `flag_price_moves` with no ticker argument, which scanned all 20
tickers and returned 5 movers — the unified contract reproducing the behaviour the app-only
`hours_back` variant used to provide (`screenshots/19_app_flag_price_moves.png`). The saved
report opens "As of the latest close", showing the end-of-day prompt change taking effect in
model output rather than only in configuration.

Reaching that point required one further fix: neither system prompt mentioned the write
tools, so the model almost never chose `save_analysis_report` and the table had stayed empty
through every prior test run. Both prompts now state the routing explicitly.

### CDF → Delta (−1): snapshot only, no incremental read

`cdf/06_cdf_to_delta.ipynb` gained `main.analytics.cdf_watermarks` and a real incremental
stage (cell 6). Each run reads `readChangeFeed` from `last_version + 1` to the current Delta
version, appends the events, then advances the watermark:

- `update_preimage` rows are filtered out, so one UPDATE produces one event
- `_change_type` is normalized to INSERT / UPDATE / DELETE
- the watermark advances **only after** a successful write, so a failed run retries the range
- the first run after the snapshot initializes the watermark rather than replaying history

The "future enhancement" comment that flagged this as incomplete is gone.

**Verified on 2026-08-06.** First run after the change initialized watermarks; the second
read the feed for real:

```
--- Incremental CDF capture ---
  main.silver.companies:       captured  40 change event(s) v14..v15
  main.silver.price_snapshots: captured  90 change event(s) v14..v15
  main.silver.news_articles:   captured 266 change event(s) v14..v15
Total incremental events captured: 396
```

`main.analytics.cdf_events` now spans `commit_version` 0 through 14 — the original snapshot
sits at version 0, so anything above it came from `readChangeFeed`, and all three watermarks
advanced to 15.

The DELETE events are expected: Silver rebuilds with `mode("overwrite")`, so each run
replaces every row and CDF records that as a delete-then-insert of the whole table (20
companies produce 40 events per run). These are genuine change events describing a table
rewrite, not semantic row-level edits — worth reading `cdf_summary` with that in mind.

### Gold sector rankings — found during verification

Not part of the original review, but surfaced while verifying the fixes above.
`ticker_daily_summary` aliased Polygon's `sic_description` as `sector` (the API returns no
sector field). SIC descriptions are far more granular than the five curated sectors, so once
all 20 tickers were activated `sector_rankings` fragmented into **13 groups** — disagreeing
with the README, with `main.config.ticker_config`, and with the fallback query inside the
App's own `get_sector_rankings`. Sector is now joined from the curated registry, with SIC
kept as a `coalesce` fallback. `sector_rankings` returns 5 rows totalling 20 tickers.

### Data recency phrasing

Both system prompts (`app/app.py` and `agent/07_agent_tools.ipynb`) previously told the model
it had *"access to real-time market data"*, contradicting the documented EOD constraint. They
now state the data is end-of-day and instruct the model to say "as of the latest close".

---

## 3. Not changed, and why

- **RAG chunking / clickable sources.** Suggested as an improvement rather than a deduction.
  Descriptions from the Polygon news endpoint are short enough that chunking would add
  machinery without improving recall at this corpus size (144 indexed rows).
- **Surfacing `cdf_summary` in the app.** The CDF tables are pipeline-monitoring artifacts,
  not user-facing stock research; adding them to the chat UI would muddy the product's purpose.

---

## 4. Verifying this submission

```
lakebase/verify_agent_writes.sql   All 4 write tools, Bronze replay-safety, CDF watermarks
screenshots/                       16 proof-of-execution captures, indexed at the end of SETUP.md
SETUP.md                           Full build log — every error encountered and its fix
```
