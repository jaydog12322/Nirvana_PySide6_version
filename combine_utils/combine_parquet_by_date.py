# combine_parquet_by_date.py

import os
import re
import pandas as pd
from pathlib import Path
from typing import Literal
from datetime import datetime


def combine_parquet_files(
    source_folder: str,
    output_folder: str,
    investor_type: Literal["외국인", "기관합계"],
    progress_callback=None
):
    """
    Combine all .parquet files from a folder into a single file with '일자' column.

    Args:
        source_folder: path containing raw per-stock .parquet files
        output_folder: path to save the final combined file
        investor_type: '외국인' or '기관합계'
    """
    source_path = Path(source_folder)
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    date_pattern = re.compile(r"(\d{8})")  # ✅ Match any 8-digit date
    combined_dfs = []
    skipped_files = 0

    for file in source_path.glob("*.parquet"):
        match = date_pattern.search(file.name)
        if not match:
            print(f"❌ Skipping: {file.name} (no date found)")
            skipped_files += 1
            continue

        date_str = match.group(1)
        try:
            df = pd.read_parquet(file)

            # Force 종목코드 to string with leading zeros
            if "종목코드" in df.columns:
                df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

            df["일자"] = pd.to_datetime(date_str)
            combined_dfs.append(df)

            if progress_callback:
                progress_callback(1)

        except Exception as e:
            print(f"⚠️ Failed to read {file.name}: {e}")

    print(f"📦 Combined {len(combined_dfs)} files, skipped {skipped_files}")

    if not combined_dfs:
        print("⚠️ No valid files to combine.")
        return

    full_df = pd.concat(combined_dfs, ignore_index=True)

    # 👉 Move '일자' column to the front
    cols = full_df.columns.tolist()
    if "일자" in cols:
        cols.insert(0, cols.pop(cols.index("일자")))
        full_df = full_df[cols]

    today_str = datetime.today().strftime("%Y%m%d")
    out_name = f"Combined_{investor_type}순매수_{today_str}.parquet"
    out_path = output_path / out_name
    full_df.to_parquet(out_path, index=False)

    print(f"✅ Combined file saved: {out_path}")
