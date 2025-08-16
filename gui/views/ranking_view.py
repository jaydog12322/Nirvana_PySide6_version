# gui/views/ranking_view.py
from core.file_converter import convert_excel_to_parquet
from combine_utils.combine_parquet_by_date import combine_parquet_files
from PySide6.QtWidgets import QProgressDialog, QApplication
from PySide6.QtCore import Qt

import os
from datetime import timedelta
from pathlib import Path
import pandas as pd
from core.ranking_utils import save_krx_data



from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QHBoxLayout, QComboBox, QDateEdit,
    QPushButton, QTableWidget, QTableWidgetItem, QTextEdit, QGroupBox, QSizePolicy, QHeaderView
)
from PySide6.QtCore import QDate


class SingleRankingPanel(QWidget):
    def __init__(self, label: str, show_controls=True, fixed_investor=None, tab_refs=None):
        self.tab_refs = tab_refs or {}
        self.fixed_investor = fixed_investor
        super().__init__()
        self.label = label
        self.show_controls = show_controls
        self.data_by_date = {}
        self.current_date = None
        self.current_view = None

        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Optional: Header and log box
        if self.show_controls:
            header = QGroupBox(f"{self.label} - 제어판")
            header_layout = QHBoxLayout()

            self.start_date = QDateEdit()
            self.start_date.setCalendarPopup(True)
            self.start_date.setDate(QDate.currentDate())

            self.end_date = QDateEdit()
            self.end_date.setCalendarPopup(True)
            self.end_date.setDate(QDate.currentDate())

            self.investor_combo = QComboBox()
            self.investor_combo.addItems(["외국인", "기관합계"])

            self.download_btn = QPushButton("Download")
            self.auto_btn = QPushButton("Auto Download")
            self.convert_btn = QPushButton("Convert to Parquet")
            self.combine_btn = QPushButton("Combine Parquet")

            self.download_btn.clicked.connect(self.on_download)
            self.auto_btn.clicked.connect(self.on_auto_download)
            self.convert_btn.clicked.connect(self.on_convert_parquet)
            self.combine_btn.clicked.connect(self.on_combine_parquet)

            header_layout.addWidget(QLabel("시작일:"))
            header_layout.addWidget(self.start_date)
            header_layout.addWidget(QLabel("종료일:"))
            header_layout.addWidget(self.end_date)
            header_layout.addWidget(QLabel("투자자:"))
            header_layout.addWidget(self.investor_combo)
            header_layout.addWidget(self.download_btn)
            header_layout.addWidget(self.auto_btn)
            header_layout.addWidget(self.convert_btn)
            header_layout.addWidget(self.combine_btn)

            header.setLayout(header_layout)
            layout.addWidget(header)

            self.log_box = QTextEdit()
            self.log_box.setReadOnly(True)
            self.log_box.setFixedHeight(100)
            layout.addWidget(self.log_box)

        # Button row
        button_row = QHBoxLayout()
        self.load_btn = QPushButton("Load")
        self.daily_btn = QPushButton("Load Daily 순위")
        self.five_day_btn = QPushButton("Load 5-Day 순위")
        self.ten_day_btn = QPushButton("Load 10-Day 순위")
        self.prev_btn = QPushButton("←")
        self.next_btn = QPushButton("→")
        self.date_label = QLabel("현재 선택된 날짜: 없음")
        self.load_btn.clicked.connect(self.load_parquet_files)
        self.daily_btn.clicked.connect(self.show_daily)
        self.five_day_btn.clicked.connect(self.show_5day)
        self.ten_day_btn.clicked.connect(self.show_10day)
        self.prev_btn.clicked.connect(self.prev_date)
        self.next_btn.clicked.connect(self.next_date)

        for btn in [self.load_btn, self.daily_btn, self.five_day_btn, self.ten_day_btn, self.prev_btn, self.next_btn]:
            button_row.addWidget(btn)
        button_row.addWidget(self.date_label)
        layout.addLayout(button_row)

        # Table
        self.table = QTableWidget(20, 4)
        self.table.setHorizontalHeaderLabels(["순위", "종목코드", "종목명", "거래대금_순매수"])
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)

        layout.addWidget(QLabel(self.label))
    def on_download(self):
        start = self.start_date.date().toPython()
        end = self.end_date.date().toPython()
        investor = self.investor_combo.currentText()

        code_map = {
            "외국인": "9000",
            "기관합계": "7050"
        }
        folder = Path.cwd() / (f"{investor}_순매수_Data")
        code = code_map[investor]
        dates = pd.date_range(start=start, end=end)

        for d in dates:
            date_str = d.strftime("%Y%m%d")
            try:
                save_krx_data(date_str, code, folder, investor)
                self.append_log(f"{investor} - {date_str}: ✅ Successfully updated")
            except Exception:
                self.append_log(f"{investor} - {date_str}: ❌ Download failed")

    def on_auto_download(self):
        start = self.start_date.date().toPython()
        end = self.end_date.date().toPython()
        investor = self.investor_combo.currentText()

        code_map = {
            "외국인": "9000",
            "기관합계": "7050"
        }
        folder = Path.cwd() / (f"{investor}_순매수_Data")
        code = code_map[investor]
        dates = pd.date_range(start=start, end=end)
        dates = [d for d in dates if d.weekday() < 5]

        if len(dates) > 200:
            self.append_log("❌ Cannot request more than 200 dates.")
            return

        for d in dates:
            date_str = d.strftime("%Y%m%d")
            file_path = folder / f"{investor}_순매수_{date_str}.xlsx"
            if file_path.exists():
                self.append_log(f"{date_str}: Already exists")
                continue
            try:
                save_krx_data(date_str, code, folder, investor)
                self.append_log(f"{date_str}: ✅ Downloaded")
            except Exception as e:
                self.append_log(f"{date_str}: ❌ Failed: {e}")

    def append_log(self, msg: str):
        self.log_box.append(msg)

    def on_convert_parquet(self):
        investor = self.investor_combo.currentText()
        folder = Path.cwd() / (f"{investor}_순매수_Data")

        if not folder.exists():
            self.append_log(f"❌ Folder does not exist: {folder.name}")
            return

        files = list(folder.rglob("*.xlsx"))
        if not files:
            self.append_log(f"ℹ️ No Excel files found in {folder.name}")
            return

        self.append_log(f"🔄 Starting Parquet conversion: {len(files)} files")

        for i, file in enumerate(files, 1):
            convert_excel_to_parquet(file)
            self.append_log(f"[{i}/{len(files)}] ✅ Converted: {file.name}")

        self.append_log("✅ All files converted to Parquet.")

    def load_parquet_files(self):
        # Special logic for 통합 순매수
        if self.label == "통합 순매수":
            foreign_panel = self.tab_refs.get("외국인")
            institution_panel = self.tab_refs.get("기관")

            if not foreign_panel or not institution_panel:
                self.append_log("❌ Missing references to panels.")
                return

            self.data_by_date.clear()

            all_dates = set(foreign_panel.data_by_date.keys()) | set(institution_panel.data_by_date.keys())

            for date in sorted(all_dates):
                df_f = foreign_panel.data_by_date.get(date)
                df_i = institution_panel.data_by_date.get(date)
                frames = []

                if df_f is not None:
                    frames.append(df_f)
                if df_i is not None:
                    frames.append(df_i)

                if frames:
                    combined = pd.concat(frames)
                    combined = combined.groupby(["종목코드", "종목명"], as_index=False)["거래대금_순매수"].sum()
                    self.data_by_date[date] = combined

            if self.data_by_date:
                self.current_date = max(self.data_by_date.keys())
                self.date_label.setText(f"현재 선택된 날짜: {self.current_date}")
                self.show_daily()
            else:
                self.append_log("❌ No combined data found.")
            return  # ⬅️ Exit early

        # Normal case for 외국인 / 기관
        investor = self.fixed_investor or self.investor_combo.currentText()
        folder = Path.cwd() / f"{investor}_순매수_Parquet"
        if not folder.exists():
            self.append_log(f"❌ Folder does not exist: {folder}")
            return

        files = sorted(folder.glob("*.parquet"))
        if not files:
            self.append_log("ℹ️ No Parquet files found.")
            return

        self.data_by_date.clear()

        for file in files:
            try:
                date_part = file.stem.split("_")[-1]
                date_obj = pd.to_datetime(date_part).date()
                df = pd.read_parquet(file)
                if '거래대금_순매수' not in df.columns:
                    continue
                df['거래대금_순매수'] = pd.to_numeric(df['거래대금_순매수'], errors='coerce')
                df.dropna(subset=['거래대금_순매수'], inplace=True)
                self.data_by_date[date_obj] = df
            except Exception as e:
                self.append_log(f"❌ Failed to load {file.name}: {e}")

        if self.data_by_date:
            self.current_date = max(self.data_by_date.keys())
            self.date_label.setText(f"현재 선택된 날짜: {self.current_date}")
            self.show_daily()
        else:
            self.append_log("❌ No usable Parquet data found.")

    def display_top_20(self, df: pd.DataFrame):
        self.table.setRowCount(0)
        df_sorted = df.sort_values(by="거래대금_순매수", ascending=False).head(20).copy()
        df_sorted.reset_index(drop=True, inplace=True)

        for idx, row in df_sorted.iterrows():
            self.table.insertRow(idx)
            self.table.setItem(idx, 0, QTableWidgetItem(str(idx + 1)))
            self.table.setItem(idx, 1, QTableWidgetItem(str(row["종목코드"])))
            self.table.setItem(idx, 2, QTableWidgetItem(str(row["종목명"])))
            self.table.setItem(idx, 3, QTableWidgetItem(f"{int(row['거래대금_순매수']):,}"))

    def show_daily(self):
        if not self.current_date:
            return
        df = self.data_by_date.get(self.current_date)
        if df is not None:
            self.current_view = "daily"
            self.display_top_20(df)

    def show_5day(self):
        if not self.current_date:
            return
        dates = sorted([d for d in self.data_by_date if d <= self.current_date], reverse=True)[:5]
        combined = pd.concat([self.data_by_date[d] for d in dates])
        grouped = combined.groupby(["종목코드", "종목명"], as_index=False)["거래대금_순매수"].sum()
        self.current_view = "5day"
        self.display_top_20(grouped)

    def show_10day(self):
        if not self.current_date:
            return
        dates = sorted([d for d in self.data_by_date if d <= self.current_date], reverse=True)[:10]
        combined = pd.concat([self.data_by_date[d] for d in dates])
        grouped = combined.groupby(["종목코드", "종목명"], as_index=False)["거래대금_순매수"].sum()
        self.current_view = "10day"
        self.display_top_20(grouped)

    def prev_date(self):
        if not self.current_date:
            return
        dates = sorted(self.data_by_date.keys())
        idx = dates.index(self.current_date)
        if idx > 0:
            self.current_date = dates[idx - 1]
            self.date_label.setText(f"현재 선택된 날짜: {self.current_date}")
            self.refresh_current_view()

    def next_date(self):
        if not self.current_date:
            return
        dates = sorted(self.data_by_date.keys())
        idx = dates.index(self.current_date)
        if idx < len(dates) - 1:
            self.current_date = dates[idx + 1]
            self.date_label.setText(f"현재 선택된 날짜: {self.current_date}")
            self.refresh_current_view()

    def refresh_current_view(self):
        if self.current_view == "daily":
            self.show_daily()
        elif self.current_view == "5day":
            self.show_5day()
        elif self.current_view == "10day":
            self.show_10day()

    def on_combine_parquet(self):
        investor = self.investor_combo.currentText()
        input_folder = Path.cwd() / f"{investor}_순매수_Parquet"
        output_folder = Path.cwd() / "Combined_순매수_Parquet"

        if not input_folder.exists():
            self.append_log(f"❌ Input folder not found: {input_folder}")
            return

        files = list(input_folder.glob("*.parquet"))
        total = len(files)
        if total == 0:
            self.append_log(f"❌ No parquet files found to combine.")
            return

        # ✅ Create progress dialog
        progress = QProgressDialog("Combining files...", "Cancel", 0, total, self)
        progress.setWindowTitle("Combining Parquet Files")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.setValue(0)

        def update_progress(step):
            progress.setValue(progress.value() + step)
            QApplication.processEvents()

        try:
            combine_parquet_files(
                source_folder=str(input_folder),
                output_folder=str(output_folder),
                investor_type=investor,
                progress_callback=update_progress  # ✅ Hooked
            )
            self.append_log(f"✅ Combined Parquet files saved to {output_folder.name}")
        except Exception as e:
            self.append_log(f"❌ Combination failed: {e}")
        finally:
            progress.close()


class RankingView(QWidget):
    def __init__(self):
        super().__init__()
        layout = QHBoxLayout()

        self.foreign_panel = SingleRankingPanel("외국인 순매수", show_controls=True)
        self.institution_panel = SingleRankingPanel("기관 순매수", show_controls=False, fixed_investor="기관합계")
        self.combined_panel = SingleRankingPanel(
            "통합 순매수",
            show_controls=False,
            tab_refs={
                "외국인": self.foreign_panel,
                "기관": self.institution_panel
            }
        )

        layout.addWidget(self.foreign_panel)
        layout.addWidget(self.institution_panel)
        layout.addWidget(self.combined_panel)

        self.setLayout(layout)
