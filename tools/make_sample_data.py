"""Creates a small FAKE dataset in the same format as the CMS DE-SynPUF files.

Use it to test the pipeline quickly (and in GitHub Actions CI) before you
download the real data. It writes zip files into data/landing.
"""
import csv
import io
import random
import sys
import zipfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
LANDING.mkdir(parents=True, exist_ok=True)
random.seed(42)

N_MEMBERS = 200
MEMBERS = [f"{i:016X}" for i in range(1, N_MEMBERS + 1)]
BENE_COLS = ["DESYNPUF_ID", "BENE_BIRTH_DT", "BENE_DEATH_DT", "BENE_SEX_IDENT_CD",
             "BENE_RACE_CD", "BENE_ESRD_IND", "SP_STATE_CODE", "BENE_COUNTY_CD",
             "BENE_HI_CVRAGE_TOT_MONS", "BENE_SMI_CVRAGE_TOT_MONS",
             "SP_CHF", "SP_DIABETES", "SP_COPD"]
CLAIM_COLS = ["DESYNPUF_ID", "CLM_ID", "SEGMENT", "CLM_FROM_DT", "CLM_THRU_DT",
              "PRVDR_NUM", "CLM_PMT_AMT", "AT_PHYSN_NPI", "ICD9_DGNS_CD_1"]
DIAGNOSES = ["4019", "25000", "4280", "49121", "41401", "V5869", "2724", "53081"]


def ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def write_zip(zip_name: str, csv_name: str, cols: list[str], rows: list[list]) -> None:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    w.writerows(rows)
    with zipfile.ZipFile(LANDING / zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, buf.getvalue())
    print(f"created data/landing/{zip_name} ({len(rows)} rows)")


def beneficiaries(year: int) -> list[list]:
    rows = []
    for i, m in enumerate(MEMBERS):
        rnd = random.Random(i)  # stable per member
        state = rnd.randint(1, 54)
        if i % 10 == 0 and year >= 2009:   # some members move -> SCD2 new version
            state = (state % 54) + 1
        months = 12 if (i + year) % 7 else rnd.randint(1, 11)
        rows.append([m, ymd(date(1925 + rnd.randint(0, 25), rnd.randint(1, 12), 1)), "",
                     rnd.randint(1, 2), rnd.randint(1, 5), "0", state, rnd.randint(1, 999),
                     months, months, rnd.randint(1, 2), 1 if (i + year) % 4 == 0 else 2,
                     rnd.randint(1, 2)])
    return rows


def claims(prefix: str, n: int, max_days: int, amount_range: tuple[int, int]) -> list[list]:
    rows = []
    for k in range(n):
        start = date(2008, 1, 1) + timedelta(days=random.randint(0, 1090))
        end = start + timedelta(days=random.randint(0, max_days))
        rows.append([random.choice(MEMBERS), f"{prefix}{k:09d}", 1, ymd(start), ymd(end),
                     f"{random.randint(1, 60):04d}PR", f"{random.uniform(*amount_range):.2f}",
                     random.randint(1000000000, 9999999999), random.choice(DIAGNOSES)])
    rows.append(rows[0])  # an exact duplicate -> removed by dedup
    bad = list(rows[1]); bad[1] = f"{prefix}BAD000001"; bad[4] = "20000101"
    rows.append(bad)      # thru date before from date -> goes to rejects
    return rows


def main() -> int:
    for year in (2008, 2009, 2010):
        write_zip(f"DE1_0_{year}_Beneficiary_Summary_File_Sample_1.zip",
                  f"DE1_0_{year}_Beneficiary_Summary_File_Sample_1.csv",
                  BENE_COLS, beneficiaries(year))
    write_zip("DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip",
              "DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv",
              CLAIM_COLS, claims("IP", 300, 12, (2000, 25000)))
    write_zip("DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip",
              "DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv",
              CLAIM_COLS, claims("OP", 1500, 1, (20, 900)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
