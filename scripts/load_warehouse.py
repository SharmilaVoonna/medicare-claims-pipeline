"""Step 3 - Load the star schema into DuckDB.

Builds:
  dim_date      - one row per calendar day
  dim_member    - SCD Type 2: a new version whenever a member's tracked attributes change
  dim_provider  - one row per provider
  fact_claims   - one row per claim line, linked to the member version valid on the claim date

This is a full refresh (tables are rebuilt each run), which keeps the job idempotent.
"""
import sys

import duckdb

from config import STAGED_DIR, WAREHOUSE_DB

BENE = (STAGED_DIR / "beneficiary" / "*.parquet").as_posix()
CLAIMS = (STAGED_DIR / "claims" / "**" / "*.parquet").as_posix()

SQL = f"""
-- ---------- dim_date ----------
CREATE OR REPLACE TABLE dim_date AS
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
    CAST(d AS DATE)                        AS full_date,
    year(d)                                AS year,
    quarter(d)                             AS quarter,
    month(d)                               AS month,
    strftime(d, '%b')                      AS month_name,
    dayofweek(d)                           AS day_of_week
FROM range(DATE '2007-01-01', DATE '2011-12-31', INTERVAL 1 DAY) t(d);

-- ---------- dim_member (SCD Type 2) ----------
-- Each yearly beneficiary file is a snapshot. A new version starts only when
-- a tracked attribute differs from the previous year's snapshot.
CREATE OR REPLACE TABLE dim_member AS
WITH snap AS (
    SELECT *,
        md5(concat_ws('|', state_code, county_code, part_a_months, part_b_months,
                      has_diabetes, has_chf, has_copd)) AS attr_hash
    FROM read_parquet('{BENE}')
),
changes AS (
    SELECT *,
        lag(attr_hash) OVER (PARTITION BY member_id ORDER BY snapshot_year) AS prev_hash
    FROM snap
),
versions AS (
    SELECT * FROM changes
    WHERE prev_hash IS NULL OR prev_hash <> attr_hash
)
SELECT
    row_number() OVER (ORDER BY member_id, snapshot_year)          AS member_sk,
    member_id, birth_date, death_date, sex_code, state_code, county_code,
    part_a_months, part_b_months, has_diabetes, has_chf, has_copd,
    make_date(snapshot_year, 1, 1)                                  AS effective_from,
    coalesce(
        make_date(lead(snapshot_year) OVER w, 1, 1) - INTERVAL 1 DAY,
        DATE '9999-12-31')::DATE                                    AS effective_to,
    lead(snapshot_year) OVER w IS NULL                              AS is_current
FROM versions
WINDOW w AS (PARTITION BY member_id ORDER BY snapshot_year);

-- ---------- dim_provider ----------
CREATE OR REPLACE TABLE dim_provider AS
SELECT
    row_number() OVER (ORDER BY provider_id) AS provider_sk,
    provider_id
FROM (SELECT DISTINCT provider_id FROM read_parquet('{CLAIMS}', hive_partitioning = true)
      WHERE provider_id IS NOT NULL);

-- ---------- fact_claims ----------
-- Claims dated before a member's first snapshot are matched to their earliest version.
CREATE OR REPLACE TABLE fact_claims AS
WITH c AS (SELECT * FROM read_parquet('{CLAIMS}', hive_partitioning = true)),
first_version AS (
    SELECT member_id, min(effective_from) AS first_from FROM dim_member GROUP BY member_id
)
SELECT
    c.claim_id, c.segment, c.claim_type,
    m.member_sk,
    p.provider_sk,
    CAST(strftime(c.claim_from_date, '%Y%m%d') AS INTEGER) AS from_date_key,
    CAST(strftime(c.claim_thru_date, '%Y%m%d') AS INTEGER) AS thru_date_key,
    c.primary_diagnosis,
    c.paid_amount,
    datediff('day', c.claim_from_date, c.claim_thru_date) + 1 AS length_of_service_days
FROM c
LEFT JOIN first_version fv ON fv.member_id = c.member_id
LEFT JOIN dim_member m
       ON m.member_id = c.member_id
      AND greatest(c.claim_from_date, fv.first_from) BETWEEN m.effective_from AND m.effective_to
LEFT JOIN dim_provider p ON p.provider_id = c.provider_id;
"""


def main() -> int:
    con = duckdb.connect(str(WAREHOUSE_DB))
    con.execute(SQL)
    for table in ("dim_date", "dim_member", "dim_provider", "fact_claims"):
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"{table:14s} {n:>12,} rows")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
