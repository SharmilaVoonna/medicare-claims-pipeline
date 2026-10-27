"""Claims dashboard. Run with:  streamlit run dashboard/app.py"""
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from config import WAREHOUSE_DB  # noqa: E402

st.set_page_config(page_title="Medicare Claims Dashboard", layout="wide")
st.title("Medicare Claims Dashboard")
st.caption("Source: CMS DE-SynPUF (synthetic Medicare claims, 2008-2010)")

if not WAREHOUSE_DB.exists():
    st.error("Warehouse not found. Run the pipeline first.")
    st.stop()

con = duckdb.connect(str(WAREHOUSE_DB), read_only=True)
q = lambda sql: con.execute(sql).df()  # noqa: E731

claim_types = q("SELECT DISTINCT claim_type FROM fact_claims ORDER BY 1")["claim_type"].tolist()
chosen = st.multiselect("Claim type", claim_types, default=claim_types)
if not chosen:
    st.stop()
types_sql = ", ".join(f"'{t}'" for t in chosen)
where = f"WHERE f.claim_type IN ({types_sql})"

kpi = q(f"""
    SELECT count(*) AS claims, count(DISTINCT m.member_id) AS members,
           sum(f.paid_amount) AS paid,
           sum(f.paid_amount) / count(DISTINCT m.member_id) AS paid_per_member
    FROM fact_claims f JOIN dim_member m USING (member_sk) {where}
""").iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Claims", f"{kpi.claims:,.0f}")
c2.metric("Members", f"{kpi.members:,.0f}")
c3.metric("Total paid", f"${kpi.paid:,.0f}")
c4.metric("Paid per member", f"${kpi.paid_per_member:,.0f}")

st.subheader("Paid amount by month")
monthly = q(f"""
    SELECT make_date(d.year, d.month, 1) AS month, f.claim_type, sum(f.paid_amount) AS paid
    FROM fact_claims f JOIN dim_date d ON d.date_key = f.from_date_key {where}
    GROUP BY ALL ORDER BY 1
""")
st.line_chart(monthly, x="month", y="paid", color="claim_type")

left, right = st.columns(2)
with left:
    st.subheader("Top 10 diagnoses by paid amount")
    st.bar_chart(q(f"""
        SELECT f.primary_diagnosis AS diagnosis, sum(f.paid_amount) AS paid
        FROM fact_claims f {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """), x="diagnosis", y="paid", horizontal=True)
with right:
    st.subheader("Paid per member: chronic conditions")
    st.dataframe(q(f"""
        SELECT CASE WHEN m.has_diabetes THEN 'Diabetes' ELSE 'No diabetes' END AS group_name,
               count(DISTINCT m.member_id) AS members,
               round(sum(f.paid_amount) / count(DISTINCT m.member_id), 2) AS paid_per_member
        FROM fact_claims f JOIN dim_member m USING (member_sk) {where} GROUP BY 1
    """), hide_index=True)

st.subheader("Latest data quality results")
st.dataframe(q("""
    SELECT check_name, severity, bad_rows, passed FROM dq_results
    WHERE run_at = (SELECT max(run_at) FROM dq_results) ORDER BY severity, check_name
"""), hide_index=True)
