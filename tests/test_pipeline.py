"""Tests run AFTER the pipeline has run (locally or in CI on sample data)."""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from config import STAGED_DIR, WAREHOUSE_DB  # noqa: E402


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(str(WAREHOUSE_DB), read_only=True)
    yield c
    c.close()


def test_star_schema_tables_exist(con):
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert {"dim_date", "dim_member", "dim_provider", "fact_claims", "dq_results"} <= tables


def test_every_member_has_exactly_one_current_version(con):
    bad = con.execute("""
        SELECT count(*) FROM (SELECT member_id, sum(is_current::INT) AS n
                              FROM dim_member GROUP BY 1) WHERE n <> 1
    """).fetchone()[0]
    assert bad == 0


def test_scd2_creates_history_when_attributes_change(con):
    multi = con.execute("""
        SELECT count(*) FROM (SELECT member_id FROM dim_member
                              GROUP BY 1 HAVING count(*) > 1)
    """).fetchone()[0]
    assert multi > 0, "expected some members to have more than one version"


def test_bad_rows_are_rejected_not_loaded(con):
    rejects = duckdb.sql(
        f"SELECT count(*) FROM read_parquet('{(STAGED_DIR / 'claims_rejects').as_posix()}/*.parquet')"
    ).fetchone()[0]
    leaked = con.execute(
        "SELECT count(*) FROM fact_claims WHERE length_of_service_days <= 0").fetchone()[0]
    assert rejects > 0
    assert leaked == 0


def test_latest_quality_run_has_no_critical_failures(con):
    failed = con.execute("""
        SELECT count(*) FROM dq_results
        WHERE run_at = (SELECT max(run_at) FROM dq_results)
          AND severity = 'CRITICAL' AND NOT passed
    """).fetchone()[0]
    assert failed == 0
