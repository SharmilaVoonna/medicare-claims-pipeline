"""Step 1 - Ingest.

Unzips every CMS zip file in data/landing into data/raw/<dataset>/,
writes a manifest of what was loaded, and moves the zip to data/archive.
"""
import csv
import json
import shutil
import sys
import zipfile
from datetime import datetime, timezone

from config import ARCHIVE_DIR, DATASET_PATTERNS, LANDING_DIR, RAW_DIR


def dataset_for(filename: str) -> str | None:
    lowered = filename.lower()
    for dataset, pattern in DATASET_PATTERNS.items():
        if pattern.lower() in lowered:
            return dataset
    return None


def count_rows(path) -> int:
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus header


def main() -> int:
    zips = sorted(LANDING_DIR.glob("*.zip"))
    if not zips:
        print("No zip files in data/landing - nothing to ingest.")
        return 0

    manifest = []
    for zpath in zips:
        with zipfile.ZipFile(zpath) as zf:
            for member in zf.namelist():
                if not member.lower().endswith(".csv"):
                    continue
                dataset = dataset_for(member)
                if dataset is None:
                    print(f"SKIP  {member} (not a dataset this project uses)")
                    continue
                target_dir = RAW_DIR / dataset
                target_dir.mkdir(parents=True, exist_ok=True)
                target = target_dir / member.split("/")[-1]
                with zf.open(member) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                rows = count_rows(target)
                manifest.append({
                    "source_zip": zpath.name,
                    "file": target.name,
                    "dataset": dataset,
                    "rows": rows,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                })
                print(f"LOAD  {target.name:70s} -> raw/{dataset}  ({rows:,} rows)")
        shutil.move(str(zpath), ARCHIVE_DIR / zpath.name)

    manifest_path = RAW_DIR / f"_manifest_{datetime.now():%Y%m%d_%H%M%S}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"Manifest written: {manifest_path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
