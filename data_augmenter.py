import os
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
from tqdm import tqdm
from glob import glob

from feature_config import FEATURE_ID_LIST, FEATURE_LABELS


def find_latest_common_stock_file(base_dir="Perm_Data/Tradability"):
    search_path = os.path.join(base_dir, "전종목_우선주제외_List_*.xlsx")
    candidates = glob(search_path)
    if not candidates:
        raise FileNotFoundError("No 전종목_우선주제외_List files found.")
    return max(candidates, key=os.path.getmtime)


# ======== CONFIGURABLE PATHS ========
FOREIGN_PARQUET_FOLDER = "외국인_순매수_Parquet"
INSTITUTION_PARQUET_FOLDER = "기관_순매수_Parquet"
ENHANCED_OUTPUT_BASE = "Enhanced_Data"


# ======== Load 보통주 종목 정보 ========
def load_common_stock_info() -> pd.DataFrame:
    perm_path = find_latest_common_stock_file()
    df = pd.read_excel(perm_path, sheet_name=0)
    df = df[["종목코드", "종목명"]].dropna()
    df["보통주여부"] = True
    return df


# ======== MAIN FUNCTION ========
def generate_enhanced_dataset(
        input_folder: str,
        output_folder: str,
        strategy_id: str,
        selected_features: List[str],
        save_combined: bool = True,
        progress_callback=None
):
    common_df = load_common_stock_info()
    종목명_to_코드 = dict(zip(common_df["종목명"], common_df["종목코드"]))
    보통주_set = set(common_df["종목코드"])

    strategy_folder = os.path.join(output_folder, strategy_id)
    os.makedirs(strategy_folder, exist_ok=True)

    all_dfs = []

    files = sorted([
        f for f in os.listdir(input_folder)
        if f.endswith(".parquet")
    ])

    print(f"Processing {len(files)} files from: {input_folder}")

    for fname in tqdm(files):
        fpath = os.path.join(input_folder, fname)
        try:
            stock_df = pd.read_parquet(fpath)
            stock_code = fname.replace(".parquet", "").split("_")[0]

            print(f"🔍 Processing file: {fname} → 종목코드: {stock_code}")

            if stock_code not in 보통주_set:
                print(f"⚠️ Skipped (not 보통주): {stock_code}")
                continue

            augmented_df = augment_single_file(
                df=stock_df,
                stock_code=stock_code,
                종목명_to_코드=종목명_to_코드,
                selected_features=selected_features
            )

            out_path = os.path.join(strategy_folder, fname)
            augmented_df.to_parquet(out_path, index=False)
            all_dfs.append(augmented_df)

        except Exception as e:
            print(f"❌ Error processing {fname}: {e}")

    if save_combined and all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_df.to_parquet(
            os.path.join(strategy_folder, f"Combined_{strategy_id}.parquet"),
            index=False
        )
        print(f"✅ Combined file saved: {strategy_id}")


# ======== AUGMENTATION ========
def augment_single_file(
        df: pd.DataFrame,
        stock_code: str,
        종목명_to_코드: Dict[str, str],
        selected_features: List[str]
) -> pd.DataFrame:
    label_to_id = dict(zip(FEATURE_LABELS, FEATURE_ID_LIST))
    selected_ids = [label_to_id[label] for label in selected_features if label in label_to_id]

    df = df.copy()

    if "일자" not in df.columns and "날짜" in df.columns:
        df.rename(columns={"날짜": "일자"}, inplace=True)

    if "일자" not in df.columns:
        raise ValueError("⚠️ 날짜/일자 컬럼이 없습니다.")

    df["일자"] = pd.to_datetime(df["일자"])
    df.sort_values("일자", inplace=True)
    df.reset_index(drop=True, inplace=True)

    df["종목코드"] = stock_code
    df["보통주여부"] = True

    # Look up 종목명 from 종목명_to_코드
    code_to_name = {v: k for k, v in 종목명_to_코드.items()}
    df["종목명"] = code_to_name.get(stock_code, "Unknown")

    if "거래대금" in selected_ids:
        df["거래대금"] = ((df["고가"] + df["저가"]) / 2) * df["거래량"]

    # === Precompute MAs if needed for dependent features ===
    required_ma_periods = [5, 10, 20, 60, 120]
    for period in required_ma_periods:
        col = f"MA_{period}"
        if any(sel.startswith(col) or sel.endswith(col) for sel in selected_ids):
            df[col] = df["종가"].rolling(window=period).mean()

    # 🆕 NEW: Calculate Low_MA_5
    if "Low_MA_5" in selected_ids:
        df["Low_MA_5"] = calculate_low_ma_5(df)

    # 🆕 NEW: Calculate False_Entry_Checker (FIXED - NOW IMPLEMENTED)
    if "False_Entry_Checker" in selected_ids:
        # First ensure Low_MA_5 exists (calculate it if needed)
        if "Low_MA_5" not in df.columns:
            df["Low_MA_5"] = calculate_low_ma_5(df)

        df["False_Entry_Checker"] = calculate_false_entry_checker(df)

    if "등락률" in selected_ids:
        df["등락률"] = df["종가"].pct_change().fillna(0) * 100

    for period in required_ma_periods:
        vol_col = f"거래대금_MA_{period}"
        if vol_col in selected_ids and "거래대금" in df.columns:
            df[vol_col] = df["거래대금"].rolling(window=period).mean()

    if "MA5_10_차이율" in selected_ids:
        df["MA5_10_차이율"] = (df["MA_5"] - df["MA_10"]) / df["MA_10"] * 100

    if "MA10_20_차이율" in selected_ids:
        df["MA10_20_차이율"] = (df["MA_10"] - df["MA_20"]) / df["MA_20"] * 100

    if "정배열여부" in selected_ids:
        df["정배열여부"] = (df["MA_5"] > df["MA_10"]) & (df["MA_10"] > df["MA_20"])

    for period in [5, 10, 20]:
        slope_col = f"MA{period}_Slope"
        if slope_col in selected_ids:
            df[slope_col] = df[f"MA_{period}"].diff()

    if "골든크로스_MA5_20" in selected_ids:
        prev_ma5 = df["MA_5"].shift(1)
        prev_ma20 = df["MA_20"].shift(1)
        df["골든크로스_MA5_20"] = (
                (df["MA_5"] > df["MA_20"]) & (prev_ma5 <= prev_ma20)
        )

    if "외국인_순매수" in selected_ids or "기관_순매수" in selected_ids:
        combined_dir = "Combined_순매수_Parquet"

        try:
            latest_foreign = max([
                f for f in os.listdir(combined_dir)
                if f.startswith("Combined_외국인순매수_")
            ])
            foreign_df = pd.read_parquet(os.path.join(combined_dir, latest_foreign))
            foreign_df["일자"] = pd.to_datetime(foreign_df["일자"])
        except Exception as e:
            print(f"⚠️ 외국인 파일 오류: {e}")
            foreign_df = pd.DataFrame()

        try:
            latest_inst = max([
                f for f in os.listdir(combined_dir)
                if f.startswith("Combined_기관합계순매수_")
            ])
            inst_df = pd.read_parquet(os.path.join(combined_dir, latest_inst))
            inst_df["일자"] = pd.to_datetime(inst_df["일자"])
        except Exception as e:
            print(f"⚠️ 기관 파일 오류: {e}")
            inst_df = pd.DataFrame()

        if "외국인_순매수" in selected_ids and not foreign_df.empty:
            df = df.merge(
                foreign_df[["종목코드", "일자", "거래대금_순매수"]]
                .rename(columns={"거래대금_순매수": "외국인_순매수"}),
                on=["종목코드", "일자"], how="left"
            )

        if "기관_순매수" in selected_ids and not inst_df.empty:
            df = df.merge(
                inst_df[["종목코드", "일자", "거래대금_순매수"]]
                .rename(columns={"거래대금_순매수": "기관_순매수"}),
                on=["종목코드", "일자"], how="left"
            )

        # Clean any duplicated columns from earlier merges
        for col in ["외국인_순매수", "기관_순매수"]:
            if f"{col}_x" in df.columns and f"{col}_y" in df.columns:
                df.drop(columns=[f"{col}_x"], inplace=True)
                df.rename(columns={f"{col}_y": col}, inplace=True)

    # === Placeholder Simulation Columns ===
    for col in ["MA5_최대상승률", "MA10_최대상승률", "MDD_진입이후"]:
        if col in selected_ids:
            df[col] = np.nan
    # === D+1 Open to Close % ===
    if "D+1 Open to Close %" in selected_ids:
        df["D+1 Open to Close %"] = (
                                            (df["종가"].shift(-1) - df["시가"].shift(-1)) / df["시가"].shift(-1)
                                    ) * 100

    print(f"✅ Augmented {stock_code} → {df.columns.tolist()}")

    mandatory_columns = [
        "일자",
        "종목명",
        "종목코드",
        "시가",
        "고가",
        "저가",
        "종가",
        "거래량",
        "외국인소진율",
    ]

    final_columns = list(
        dict.fromkeys(
            mandatory_columns
            + [
                col
                for sel in selected_ids
                for col in df.columns
                if col == sel or col.startswith(sel + "_")
            ]
        )
    )

    print(f"[DEBUG] Selected final columns: {final_columns}")
    df = df[final_columns]

    return df


# 🆕 NEW FUNCTION: Calculate Low_MA_5
def calculate_low_ma_5(df: pd.DataFrame) -> pd.Series:
    """
    Calculate Low_MA_5: (Close[D-4] + Close[D-3] + Close[D-2] + Close[D-1] + Low[D]) / 5
    """
    result = pd.Series(np.nan, index=df.index)

    for i in range(4, len(df)):  # Start from index 4 (5th row) to have 4 previous days
        # Get previous 4 days' closing prices
        prev_4_closes = df.loc[i - 4:i - 1, "종가"].values  # [D-4, D-3, D-2, D-1]
        # Get current day's low price
        current_low = df.loc[i, "저가"]

        # Calculate: (sum of prev 4 closes + current low) / 5
        if len(prev_4_closes) == 4 and not np.isnan(current_low):
            low_ma_5 = (prev_4_closes.sum() + current_low) / 5
            result.iloc[i] = low_ma_5

    return result


# 🆕 NEW FUNCTION: Calculate False_Entry_Checker
def calculate_false_entry_checker(df: pd.DataFrame) -> pd.Series:
    """
    Calculate False_Entry_Checker: Compare Low_MA_5 with 저가 (Low price)
    - If Low_MA_5 < 저가: "No_Entry_Made"
    - If Low_MA_5 >= 저가: "Entry_Made"
    """
    result = pd.Series(np.nan, index=df.index)

    for i in range(len(df)):
        low_ma_5 = df.loc[i, "Low_MA_5"]
        low_price = df.loc[i, "저가"]

        # Only calculate if both values are available (not NaN)
        if not pd.isna(low_ma_5) and not pd.isna(low_price):
            if low_ma_5 < low_price:
                result.iloc[i] = "No_Entry_Made"
            else:
                result.iloc[i] = "Entry_Made"

    return result
