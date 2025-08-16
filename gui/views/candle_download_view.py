from PySide6.QtWidgets import (
    QWidget, QLabel, QVBoxLayout, QHBoxLayout,
    QPushButton, QLineEdit, QDateEdit, QTextEdit,
    QListWidget, QListWidgetItem, QSplitter, QScrollArea
)
from PySide6.QtCore import QDate, Qt
from datetime import datetime
import traceback
import os
import pandas as pd

from core.chart_downloader import download_daily_candlestick_data


class CandleDownloadView(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("종목 일봉차트 다운로드")

        main_layout = QVBoxLayout()

        # --- Controls ---
        control_layout = QHBoxLayout()

        control_layout.addWidget(QLabel("종목코드 (띄어쓰기로 구분):"))
        self.symbol_input = QLineEdit()
        self.symbol_input.setPlaceholderText("예: 005930 034020")
        control_layout.addWidget(self.symbol_input)

        control_layout.addWidget(QLabel("시작일:"))
        self.start_date = QDateEdit()
        self.start_date.setCalendarPopup(True)
        self.start_date.setDate(QDate.currentDate().addMonths(-6))
        control_layout.addWidget(self.start_date)

        control_layout.addWidget(QLabel("종료일:"))
        self.end_date = QDateEdit()
        self.end_date.setCalendarPopup(True)
        self.end_date.setDate(QDate.currentDate())
        control_layout.addWidget(self.end_date)

        self.download_button = QPushButton("📥 일봉차트 다운로드")
        self.download_button.clicked.connect(self.download_data)
        control_layout.addWidget(self.download_button)

        main_layout.addLayout(control_layout)

        # --- Split view ---
        splitter = QSplitter(Qt.Horizontal)

        # File list (left)
        left_panel = QVBoxLayout()

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("🔍 파일 검색 (예: 005930)")
        self.search_input.textChanged.connect(self.filter_file_list)
        left_panel.addWidget(self.search_input)

        self.file_count_label = QLabel("📦 파일 수: 0")
        left_panel.addWidget(self.file_count_label)

        self.file_list = QListWidget()
        self.file_list.itemClicked.connect(self.preview_file)
        self.file_list.setSelectionMode(QListWidget.SingleSelection)
        left_panel.addWidget(self.file_list)

        left_widget = QWidget()
        left_widget.setLayout(left_panel)
        splitter.addWidget(left_widget)

        # File preview (right)
        self.file_preview = QTextEdit()
        self.file_preview.setReadOnly(True)
        splitter.addWidget(self.file_preview)
        splitter.setSizes([200, 600])

        main_layout.addWidget(splitter)

        # --- Log output ---
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        main_layout.addWidget(self.log_output)

        self.setLayout(main_layout)

        # Load existing files
        self.refresh_file_list()

    def log(self, message: str):
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        self.log_output.append(f"{timestamp} {message}")

    def download_data(self):
        symbol_text = self.symbol_input.text().strip()
        if not symbol_text:
            self.log("❌ 종목코드를 입력하세요.")
            return

        symbols = [s.strip() for s in symbol_text.split() if s.strip()]
        start = self.start_date.date().toString("yyyyMMdd")
        end = self.end_date.date().toString("yyyyMMdd")

        self.log(f"🔍 다운로드 시작: {', '.join(symbols)} / {start} ~ {end}")

        try:
            save_dir = os.path.join("Chart_Data", "DailyCandle")
            os.makedirs(save_dir, exist_ok=True)
            download_daily_candlestick_data(symbols, start, end, save_dir, logger=self.log)
            self.log("✅ 다운로드 완료.")
            self.refresh_file_list()
        except Exception as e:
            self.log(f"❌ 에러 발생: {str(e)}")
            self.log(traceback.format_exc())

    def refresh_file_list(self):
        self.file_list.clear()
        self.all_parquet_files = []
        folder = os.path.join("Chart_Data", "DailyCandle")
        if os.path.exists(folder):
            self.all_parquet_files = sorted(f for f in os.listdir(folder) if f.endswith(".parquet"))
        self.filter_file_list()  # Apply current search filter

    def preview_file(self, item):
        filename = item.text()
        full_path = os.path.join("Chart_Data", "DailyCandle", filename)
        try:
            df = pd.read_parquet(full_path)
            preview_text = df.to_string(index=False)
            self.file_preview.setPlainText(preview_text)
        except Exception as e:
            self.file_preview.setPlainText(f"파일 로딩 실패: {str(e)}")

    def filter_file_list(self):
        query = self.search_input.text().strip().lower()
        self.file_list.clear()
        filtered = [f for f in self.all_parquet_files if query in f.lower()]
        for f in filtered:
            self.file_list.addItem(QListWidgetItem(f))
        self.file_count_label.setText(f"📦 파일 수: {len(filtered)}")
