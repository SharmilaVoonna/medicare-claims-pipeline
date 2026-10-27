"""Shared paths and settings for every pipeline step."""
import os
from pathlib import Path

# PROJECT_ROOT can be overridden (Airflow container mounts the project at /opt/project)
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[1]))

DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing"   # zip files you download from CMS go here
RAW_DIR = DATA_DIR / "raw"           # unzipped CSVs, organised by dataset
STAGED_DIR = DATA_DIR / "staged"     # cleansed Parquet written by PySpark
ARCHIVE_DIR = DATA_DIR / "archive"   # zips are moved here after ingestion

WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
WAREHOUSE_DB = WAREHOUSE_DIR / "claims.duckdb"

# Which raw dataset a CSV belongs to, based on words in the CMS file name
DATASET_PATTERNS = {
    "beneficiary": "Beneficiary_Summary",
    "inpatient": "Inpatient_Claims",
    "outpatient": "Outpatient_Claims",
}

for d in (LANDING_DIR, RAW_DIR, STAGED_DIR, ARCHIVE_DIR, WAREHOUSE_DIR):
    d.mkdir(parents=True, exist_ok=True)
