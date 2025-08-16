import os
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd
import re

class PermManager:
    def __init__(self, base_dir, status_callback=None):
        self.base_dir = Path(base_dir)
        self.perm_dir = self.base_dir / "Perm_Data"
        self.fundamental_dir = self.perm_dir / "Fundamentals"
        self.tradability_dir = self.perm_dir / "Tradability"
        self.index_trend_dir = self.perm_dir / "Index_Trend"
        self.index_trend_kosdaq_dir = self.perm_dir / "Index_Trend_KOSDAQ"
        self.status_callback = status_callback
        self.status_map = {}

        self._ensure_directories()
        self.load_status()

    def _ensure_directories(self):
        self.perm_dir.mkdir(exist_ok=True)
        self.fundamental_dir.mkdir(exist_ok=True)
        self.tradability_dir.mkdir(exist_ok=True)
        self.index_trend_dir.mkdir(exist_ok=True)
        self.index_trend_kosdaq_dir.mkdir(exist_ok=True)

    def load_status(self):
        status_file = self.perm_dir / "perm_status.json"
        if status_file.exists():
            with open(status_file, "r", encoding="utf-8") as f:
                self.status_map = json.load(f)

    def save_status(self):
        status_file = self.perm_dir / "perm_status.json"
        with open(status_file, "w", encoding="utf-8") as f:
            json.dump(self.status_map, f, indent=2, ensure_ascii=False)

    def _update_status(self, key, status):
        self.status_map[key] = {
            "last_updated": datetime.today().strftime("%Y-%m-%d"),
            "status": status
        }
        self.save_status()

    def check_updates(self):
        self._check_mock_fundamentals()
        self._check_tradability_file()
        self._check_index_trend_file()
        self._check_index_trend_kosdaq_file()

    def _check_mock_fundamentals(self):
        key = "mock_fundamentals"
        status = self.status_map.get(key, {})
        last_updated = status.get("last_updated")

        if not last_updated or (datetime.today() - datetime.strptime(last_updated, "%Y-%m-%d")).days > 7:
            self._download_mock_file()
            self._update_status(key, "updated")
            if self.status_callback:
                self.status_callback("✔ Fundamentals updated")
        else:
            if self.status_callback:
                self.status_callback("✔ Fundamentals up-to-date")

    def _download_mock_file(self):
        sample_data = {
            "005930": {"name": "삼성전자", "PER": 11.2, "PBR": 1.4},
            "000660": {"name": "SK하이닉스", "PER": 14.7, "PBR": 1.6}
        }
        file_path = self.fundamental_dir / "mock_fundamentals.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(sample_data, f, indent=2, ensure_ascii=False)
        return file_path

    def _check_tradability_file(self):
        key = "tradability_file"
        status = self.status_map.get(key, {})
        last_updated = status.get("last_updated")

        today_str = datetime.today().strftime("%Y%m%d")
        filtered_filename = f"전종목_우선주제외_List_{today_str}.xlsx"
        filtered_path = self.tradability_dir / filtered_filename

        if last_updated == datetime.today().strftime("%Y-%m-%d"):
            if self.status_callback:
                self.status_callback("✔ Tradability file up-to-date")

            # ✅ Even if no new file, try to generate filtered version if missing
            if not filtered_path.exists():
                latest = self._get_latest_tradability_file()
                if latest:
                    try:
                        self.create_filtered_tradability_copy(latest)
                    except Exception as e:
                        if self.status_callback:
                            self.status_callback(f"❌ Failed to generate filtered file: {e}")
            return

        try:
            file_path = self.download_and_save_tradability_file()
            self._update_status(key, "updated")
            if self.status_callback:
                self.status_callback(f"✔ Tradability file saved to {file_path.name}")
        except Exception as e:
            if self.status_callback:
                self.status_callback(f"❌ Tradability file failed: {e}")

    def download_and_save_tradability_file(self):
        otp = self.generate_otp_for_tradability()
        content = self.download_excel_by_otp(otp)

        today_str = datetime.today().strftime("%Y%m%d")
        file_path = self.tradability_dir / f"전종목_지정내역_{today_str}.xlsx"
        with open(file_path, "wb") as f:
            f.write(content)

        try:
            df = pd.read_excel(file_path, engine="openpyxl")
            if df.empty:
                raise ValueError("Downloaded Tradability file is empty.")
            self.create_filtered_tradability_copy(file_path)
        except Exception as e:
            if self.status_callback:
                self.status_callback(f"❌ Skipping filtering: {e}")

        return file_path

    def create_filtered_tradability_copy(self, source_path):
        df = pd.read_excel(source_path, engine="openpyxl")
        if df.shape[1] < 2:
            raise ValueError("Unexpected file format: less than 2 columns found.")

        # Identify stock code and name columns
        stock_code_col = df.columns[0]  # Usually column A (종목코드)
        stock_name_col = df.columns[1]  # Usually column B (종목명)

        # Convert to string and zero-pad stock codes
        names = df[stock_name_col].astype(str)
        codes = df[stock_code_col].astype(str).str.zfill(6)

        # ✅ Updated pattern: 우, (전환), 우B, 우C, 스팩, 00호 ~ 99호
        pattern = re.compile(r"(우$|\(전환\)$|우[BC]$|스팩$|[0-9]{1,2}호$)")

        # ✅ Exempt these tickers even if their name matches pattern
        exempt_tickers = {"458650", "159910", "294090"}

        # Apply regex to name
        is_non_common = names.str.contains(pattern)
        is_exempt = codes.isin(exempt_tickers)

        # Final mask: non-보통주 AND not exempt
        final_mask = is_non_common & ~is_exempt

        # Sheet 2: non-보통주
        filtered_sheet2 = df[final_mask]

        # Sheet 1: 보통주
        filtered_sheet1 = df[~final_mask]

        # Save both to Excel
        today_str = datetime.today().strftime("%Y%m%d")
        output_file = self.tradability_dir / f"전종목_우선주제외_List_{today_str}.xlsx"

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            filtered_sheet1.to_excel(writer, sheet_name="보통주", index=False)
            filtered_sheet2.to_excel(writer, sheet_name="우선주+스팩주", index=False)

        if self.status_callback:
            self.status_callback(f"✔ Filtered Tradability list saved to {output_file.name}")

    def generate_otp_for_tradability(self):
        url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020202",
            "Origin": "http://data.krx.co.kr",
            "X-Requested-With": "XMLHttpRequest"
        }
        data = {
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT02001",
            "locale": "ko_KR",
            "mktId": "ALL",
            "csvxls_isNo": "false"
        }
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.text

    def download_excel_by_otp(self, otp_code):
        url = "http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "http://data.krx.co.kr"
        }
        response = requests.post(url, headers=headers, data={"code": otp_code})
        response.raise_for_status()
        return response.content

    def _check_index_trend_file(self):
        self._check_index_trend_generic(
            market_name="KOSPI",
            save_dir=self.index_trend_dir,
            filename_prefix="종합(KOSPI)",
            otp_params={
                "indIdx": "1",
                "indIdx2": "001",
                "codeNm": "코스피"
            }
        )

    def _check_index_trend_kosdaq_file(self):
        self._check_index_trend_generic(
            market_name="KOSDAQ",
            save_dir=self.index_trend_kosdaq_dir,
            filename_prefix="종합(KOSDAQ)",
            otp_params={
                "indIdx": "2",
                "indIdx2": "001",
                "codeNm": "코스닥"
            }
        )

    def _check_index_trend_generic(self, market_name, save_dir, filename_prefix, otp_params):
        now = datetime.now()
        print(f"[DEBUG] Checking index trend for {market_name}...")

        latest_file = self._get_latest_index_file(save_dir, filename_prefix)
        last_date = None
        if latest_file:
            try:
                last_date_str = latest_file.stem.split("_")[-1].replace(")", "")
                last_date = datetime.strptime(last_date_str, "%Y%m%d").date()
            except:
                pass

        start_date = (last_date + timedelta(days=1)) if last_date else now.date() - timedelta(days=30)
        end_date = now.date() - timedelta(days=1)

        missing_dates = pd.date_range(start=start_date, end=end_date)
        missing_dates = [d for d in missing_dates if d.weekday() < 5]

        if not missing_dates:
            if self.status_callback:
                self.status_callback(f"✔ Index trend ({market_name}) up-to-date")
            return

        try:
            new_df = self.download_index_trend_data(missing_dates[0], missing_dates[-1], otp_params)
            if new_df.empty:
                raise ValueError("No data rows in new download")

            new_df['일자'] = pd.to_datetime(new_df['일자'], errors='coerce').dt.strftime('%Y/%m/%d')
            new_df = new_df.sort_values(by='일자', ascending=False)

            if latest_file:
                existing_df = pd.read_excel(latest_file, engine="openpyxl")
                combined = pd.concat([new_df, existing_df], ignore_index=True)
                latest_file.unlink()
            else:
                combined = new_df

            latest_str = new_df['일자'].str.replace("/", "").max()
            new_path = save_dir / f"{filename_prefix}_{latest_str}.xlsx"
            combined.to_excel(new_path, index=False, engine="openpyxl")

            self._update_status(f"index_trend_{market_name.lower()}", "updated")
            if self.status_callback:
                self.status_callback(f"✔ Index trend file ({market_name}) updated")

        except Exception as e:
            if self.status_callback:
                self.status_callback(f"❌ Index trend file ({market_name}) failed: {e}")

    def _get_latest_index_file(self, folder, prefix):
        files = sorted(folder.glob(f"{prefix}_*.xlsx"), reverse=True)
        return files[0] if files else None

    def download_index_trend_data(self, start_date, end_date, otp_params):
        date_str_start = start_date.strftime("%Y%m%d")
        date_str_end = end_date.strftime("%Y%m%d")
        otp = self.generate_otp_for_index_trend(date_str_start, date_str_end, otp_params)
        content = self.download_excel_by_otp(otp)

        temp_path = self.perm_dir / f"temp_{otp_params['codeNm']}_{date_str_start}_{date_str_end}.xlsx"
        with open(temp_path, "wb") as f:
            f.write(content)

        df = pd.read_excel(temp_path, engine="openpyxl")
        temp_path.unlink()
        return df

    def generate_otp_for_index_trend(self, start_date, end_date, otp_params):
        url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020202",
            "Origin": "http://data.krx.co.kr",
            "X-Requested-With": "XMLHttpRequest"
        }
        data = {
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT00301",
            "locale": "ko_KR",
            "tboxindIdx_finder_equidx0_12": otp_params['codeNm'],
            "indIdx": otp_params['indIdx'],
            "indIdx2": otp_params['indIdx2'],
            "codeNmindIdx_finder_equidx0_12": otp_params['codeNm'],
            "param1indIdx_finder_equidx0_12": "",
            "strtDd": start_date,
            "endDd": end_date,
            "share": "2",
            "money": "3",
            "csvxls_isNo": "false"
        }
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.text

    def _get_latest_tradability_file(self):
        files = sorted(self.tradability_dir.glob("전종목_지정내역_*.xlsx"), reverse=True)
        return files[0] if files else None
