from pathlib import Path
import pandas as pd




def ensure_folder_exists(path: Path):
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] Created folder: {path}")


import pandas as pd
from pathlib import Path

def convert_excel_to_parquet(file_path: Path):
    try:
        # Locate top-level folder (e.g., Perm_Data or 외국인_순매수_Data)
        # Step 1: find the first parent folder that ends with _Data or is named Perm_Data
        for ancestor in file_path.parents:
            if ancestor.name.endswith("_Data") or ancestor.name == "Perm_Data":
                root_data_folder = ancestor
                break
        else:
            print(f"[SKIP] Couldn't find root _Data folder for {file_path}")
            return

        relative_path = file_path.relative_to(root_data_folder)

        # Determine _Parquet folder root
        parquet_root = root_data_folder.with_name(root_data_folder.name.replace("_Data", "_Parquet"))
        parquet_path = parquet_root / relative_path.with_suffix(".parquet")

        # Skip if already converted
        if parquet_path.exists():
            print(f"[SKIP] Already converted: {file_path.name}")
            return

        # Ensure destination subfolder exists
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert and save
        df = pd.read_excel(file_path, engine="openpyxl")
        df.to_parquet(parquet_path, index=False)

        print(f"[OK] Converted: {file_path} → {parquet_path}")

    except Exception as e:
        print(f"[❌ FAILED] {file_path.name} → Parquet conversion error: {e}")






def sweep_and_convert_all(base_dir: Path):
    """
    Recursively scans for all .xlsx files under _Data and Perm_Data folders,
    creates sibling _Parquet folders, and converts each .xlsx to .parquet (if not already exists).
    """
    print("[INFO] Starting full sweep for Excel → Parquet conversion...")

    target_files = []
    for subdir in base_dir.rglob("*"):
        if subdir.is_dir() and ("_Data" in subdir.name or "Perm_Data" in str(subdir)):
            target_files.extend(list(subdir.glob("*.xlsx")))

    if not target_files:
        print("[INFO] No Excel files found for conversion.")
        return

    print(f"[INFO] {len(target_files)} Excel files detected for conversion.")

    for file in target_files:
        convert_excel_to_parquet(file)
