"""Step 4 - Data quality checks.

Runs reconciliation and integrity checks against the warehouse, stores every
result in a dq_results table (so you can track quality over time), and exits
with code 1 if any CRITICAL check fails - which makes the Airflow task fail.
"""
import sys
from datetime import datetime, timezone

import duckdb

from config import STAGED_DIR, WAREHOUSE_DB

CLAIMS = (STAGED_DIR / "claims" / "**" / "*.parquet").as_posix()

# (name, severity, SQL that returns the number of BAD rows)
CHECKS = [
    ("row_count_reconciliation", "CRITICAL",
     f"SELECT abs((SELECT count(*) FROM read_parquet('{CLAIMS}', hive_partitioning=true))"
     f" - (SELECT count(*) FROM fact_claims))"),
    ("duplicate_claim_lines", "CRITICAL",
     "SELECT count(*) FROM (SELECT claim_id, segment, claim_type FROM fact_claims "
     "GROUP BY ALL HAVING count(*) > 1)"),
    ("claims_without_member", "CRITICAL",
     "SELECT count(*) FROM fact_claims WHERE member_sk IS NULL"),
    ("member_multiple_current_versions", "CRITICAL",
     "SELECT count(*) FROM (SELECT member_id FROM dim_member WHERE is_current "
     "GROUP BY member_id HAVING count(*) > 1)"),
    ("member_overlapping_versions", "CRITICAL",
     "SELECT count(*) FROM dim_member a JOIN dim_member b "
     "ON a.member_id = b.member_id AND a.member_sk < b.member_sk "
     "AND a.effective_from <= b.effective_to AND b.effective_from <= a.effective_to"),
    ("claims_without_provider", "WARNING",
     "SELECT count(*) FROM fact_claims WHERE provider_sk IS NULL"),
    ("negative_paid_amount", "WARNING",
     "SELECT count(*) FROM fact_claims WHERE paid_amount < 0"),
]


def main() -> int:
    con = duckdb.connect(str(WAREHOUSE_DB))
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_at TIMESTAMP, check_name VARCHAR, severity VARCHAR,
            bad_rows BIGINT, passed BOOLEAN)
    """)
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    critical_failures = 0

    for name, severity, sql in CHECKS:
        bad = con.execute(sql).fetchone()[0]
        passed = bad == 0
        con.execute("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?)",
                    [run_at, name, severity, bad, passed])
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {severity:8s} {name:35s} bad rows: {bad:,}")
        if not passed and severity == "CRITICAL":
            critical_failures += 1

    con.close()
    if critical_failures:
        print(f"\n{critical_failures} critical check(s) failed.")
        return 1
    print("\nAll critical checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
