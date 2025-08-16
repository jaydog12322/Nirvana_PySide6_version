# registry_manager.py
"""
Minimal file-metadata registry for KRX Excel files.

▪ Scans every “*.xlsx” inside folders whose names contain “_Data” or “Perm_Data”.
▪ Records:
    - file_path         absolute path to the Excel file
    - folder            parent directory (used for summary)
    - date              YYYYMMDD parsed from the filename (if found)
    - parquet_exists    True if the matching .parquet file is present
    - rows              number of data rows in the Excel (-1 if unreadable)
    - status            "success" | "empty" | "fail"
▪ Saves / loads the registry to JSON (`file_registry.json` in the project root).
▪ Provides helpers to refresh the registry and compute a folder-level summary.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Dict

import pandas as pd
from openpyxl import load_workbook

# ─────────────────────────────── Paths ────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
REGISTRY_JSON = BASE_DIR / "file_registry.json"

# ────────────────────────────── Internals ──────────────────────────────
_DATE_RE = re.compile(r"(\d{8})")        # grabs first 8-digit block (yyyyMMdd)


def _all_xlsx_files(base_dir: Path) -> List[Path]:
    all_files = list(base_dir.rglob("*.xlsx"))
    print(f"[DEBUG] Found {len(all_files)} .xlsx files under {base_dir}")
    for f in all_files:
        print(" →", f)
    return [
        p for p in all_files
        if not p.name.startswith("~") and "~$" not in p.name
    ]





def _extract_date(fname: str) -> str:
    m = _DATE_RE.search(fname)
    return m.group(1) if m else ""


def _matching_parquet_path(xlsx_path: Path) -> Path:
    """
    Derive where convert_excel_to_parquet() puts the .parquet file.

    📦 Rule used by file_converter.convert_excel_to_parquet():
        • For .../<FOLDER>_Data/<file>.xlsx
          → .../<FOLDER>_Parquet/<file>.parquet
        • For any other path (e.g. inside Perm_Data sub-trees)
          → Same directory, just .parquet extension
    """
    parent = xlsx_path.parent
    if parent.name.endswith("_Data"):
        parquet_folder = parent.parent / f"{parent.name}_Parquet"
        parquet_folder.mkdir(exist_ok=True)
        return parquet_folder / xlsx_path.with_suffix(".parquet").name
    # default: same dir
    return xlsx_path.with_suffix(".parquet")


def _count_rows_fast(xlsx_path: Path) -> int:
    try:
        df = pd.read_excel(xlsx_path, engine="openpyxl")
        df.columns = df.columns.str.strip().str.replace('\xa0', '', regex=False)  # clean up weird headers

        if '거래대금_순매수' not in df.columns:
            return -1  # treat as unusable if key column missing

        df['거래대금_순매수'] = pd.to_numeric(df['거래대금_순매수'], errors='coerce')
        df.dropna(subset=['거래대금_순매수'], inplace=True)
        return len(df)
    except Exception:
        return -1




# ───────────────────────────── Registry API ────────────────────────────
def refresh_registry(base_dir: Path = BASE_DIR) -> pd.DataFrame:
    """
    Scan disk → build a fresh registry DataFrame → save to JSON → return DF.
    """
    from typing import List, Dict
    import pandas as pd
    import json

    registry_path = base_dir / "file_registry.json"
    print(f"[DEBUG] Saving registry to → {registry_path}")

    records: List[Dict] = []

    for xlsx in _all_xlsx_files(base_dir):
        date_str = _extract_date(xlsx.name)
        parquet_path = _matching_parquet_path(xlsx)
        parquet_exists = parquet_path.exists()

        rows = _count_rows_fast(xlsx)
        status = (
            "success" if rows > 0 else
            "empty"   if rows == 0 else
            "fail"
        )

        records.append({
            "file_path": str(xlsx.resolve()),
            "folder": str(xlsx.parent.resolve()),
            "date": date_str,
            "parquet_exists": parquet_exists,
            "rows": rows,
            "status": status,
        })

    df = pd.DataFrame(records)
    df.to_json(registry_path, orient="records", force_ascii=False, indent=2)
    return df


def load_registry(base_dir: Path = BASE_DIR) -> pd.DataFrame:
    registry_path = base_dir / "file_registry.json"
    print(f"[DEBUG] Loading registry from → {registry_path}")
    if registry_path.exists():
        return pd.read_json(registry_path)
    return pd.DataFrame(
        columns=["file_path", "folder", "date", "parquet_exists", "rows", "status"]
    )


def summarize_by_folder(registry_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Quick aggregation useful for the GUI summary panel.
    """
    if registry_df is None:
        registry_df = load_registry()
    if registry_df.empty:
        return pd.DataFrame()

    summary = (
        registry_df
        .groupby("folder", as_index=False)
        .agg(
            total_files=("file_path", "count"),
            success=("status", lambda s: (s == "success").sum()),
            empty=("status", lambda s: (s == "empty").sum()),
            fail=("status", lambda s: (s == "fail").sum()),
            converted=("parquet_exists", "sum"),
        )
    )
    return summary
def update_registry_entry(file_path: str):
    """
    After converting one file, update just that record in the registry JSON
    without doing a full refresh.
    """
    import json
    from pathlib import Path

    registry_path = BASE_DIR / "file_registry.json"
    # If we don’t yet have a registry, rebuild it
    if not registry_path.exists():
        refresh_registry(BASE_DIR)
        return

    # Load the list of records
    with open(registry_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Find and update the matching entry
    updated = False
    for rec in records:
        if rec.get("file_path") == file_path:
            rec["parquet_exists"] = True
            updated = True
            break

    # If it wasn’t already present, do a full refresh
    if not updated:
        refresh_registry(BASE_DIR)
        return

    # Write back the modified list
    with open(registry_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

def is_parquet_done(file_path: str) -> bool:
    """
    Check whether the .parquet version already exists for this Excel file.
    Used by file_converter.py before/after conversion.
    """
    from pathlib import Path

    excel = Path(file_path)
    parquet = _matching_parquet_path(excel)
    return parquet.exists()


# ─────────────────────────── CLI convenience ───────────────────────────
if __name__ == "__main__":
    """
    Allow:  python registry_manager.py
    to run a one-off refresh and print the summary to the console.
    """
    df = refresh_registry()
    print("Registry updated.  First 10 rows:")
    print(df.head(10))
    print("\nFolder summary:")
    print(summarize_by_folder(df))
