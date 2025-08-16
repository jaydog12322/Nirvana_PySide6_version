import os
import requests
import json
import pandas as pd
from time import sleep
from datetime import datetime


def download_daily_candlestick_data(symbols, start_date, end_date, save_dir, logger=print):
    """
    Downloads daily candlestick chart data for a list of stock codes from Naver and saves them as Parquet files.
    """
    base_url = "https://m.stock.naver.com/front-api/external/chart/domestic/info"
    headers = {
        "Referer": "https://m.stock.naver.com/",
        "User-Agent": "Mozilla/5.0"
    }

    os.makedirs(save_dir, exist_ok=True)

    for code in symbols:
        try:
            logger(f"🔍 다운로드 시작: {code} / {start_date} ~ {end_date}")

            params = {
                "symbol": code,
                "requestType": "1",
                "startTime": start_date,
                "endTime": end_date,
                "timeframe": "day"
            }

            response = requests.get(base_url, params=params, headers=headers, timeout=10)

            if response.status_code != 200:
                raise Exception(f"HTTP 오류: {response.status_code}")

            text = response.text.strip()

            if not text.startswith("[[") or not text.endswith("]"):
                raise ValueError("❌ 응답 형식이 올바르지 않습니다.")

            try:
                json_text = text.replace("'", '"')
                cleaned = json.loads(json_text)
            except Exception as parse_error:
                raise ValueError(f"❌ JSON 파싱 실패: {parse_error}")

            if not isinstance(cleaned, list) or len(cleaned) < 2:
                raise ValueError("❌ 유효한 데이터가 없습니다.")

            columns = cleaned[0]
            rows = cleaned[1:]

            df = pd.DataFrame(rows, columns=columns)

            # 날짜를 datetime 형식으로 변환
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y%m%d")
            df.sort_values("날짜", inplace=True)

            # 저장 경로
            filename = f"{code}_{start_date}_{end_date}.parquet"
            full_path = os.path.join(save_dir, filename)
            df.to_parquet(full_path, index=False)

            logger(f"✅ 저장 완료: {filename} (행 수: {len(df)})")

            sleep(1)  # 과도한 요청 방지

        except Exception as e:
            logger(f"❌ {code} 다운로드 실패: {e}")
