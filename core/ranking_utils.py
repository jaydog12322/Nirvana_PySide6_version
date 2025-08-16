# core/ranking_utils.py

import requests
import pandas as pd
from pathlib import Path
import json
from combine_utils.combine_parquet_by_date import combine_parquet_files


def generate_otp(date_str, investor_type_code):
    url = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd?screenId=MDCSTAT024",
        "Origin": "https://data.krx.co.kr",
        "X-Requested-With": "XMLHttpRequest"
    }
    data = {
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT02401",
        "locale": "ko_KR",
        "mktId": "ALL",
        "invstTpCd": investor_type_code,
        "strtDd": date_str,
        "endDd": date_str,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false"
    }
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.text


def download_excel_by_otp(otp_code):
    url = "https://data.krx.co.kr/comm/fileDn/download_excel/download.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://data.krx.co.kr"
    }
    response = requests.post(url, headers=headers, data={"code": otp_code})
    response.raise_for_status()
    return response.content


def save_krx_data(date_str, investor_type_code, folder, investor_name):
    otp = generate_otp(date_str, investor_type_code)
    excel_data = download_excel_by_otp(otp)
    Path(folder).mkdir(parents=True, exist_ok=True)

    filename = f"{investor_name}_순매수_{date_str}.xlsx"
    file_path = Path(folder) / filename

    with open(file_path, "wb") as f:
        f.write(excel_data)

    # Validate and clean up if bad
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        df.columns = df.columns.str.strip().str.replace('\xa0', '', regex=False)
        if df.empty or '거래대금_순매수' not in df.columns:
            file_path.unlink()
            raise ValueError("No valid data - possibly a holiday")
    except Exception as e:
        if file_path.exists():
            file_path.unlink()
        raise e
