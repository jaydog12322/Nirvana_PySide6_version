# gui/views/registry_view.py

from pathlib import Path
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QPushButton, QTableWidget, QTableWidgetItem, QLabel, QHBoxLayout, QMessageBox
)
from PySide6.QtCore import Qt
import pandas as pd
from core.registry_manager import refresh_registry, load_registry


class RegistryView(QWidget):
    def __init__(self, base_dir: Path):
        super().__init__()
        self.base_dir = base_dir
        self.table = QTableWidget()
        self.init_ui()
        self.load_registry_data()

    def init_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        refresh_btn = QPushButton("🔄 Refresh Registry")
        refresh_btn.clicked.connect(self.refresh_clicked)
        header.addWidget(QLabel("📁 파일 레지스트리 상태"))
        header.addStretch()
        header.addWidget(refresh_btn)

        layout.addLayout(header)
        layout.addWidget(self.table)

    def load_registry_data(self):
        try:
            df = load_registry(self.base_dir)
            if df.empty:
                self.show_message("No registry data found.")
                return

            self.table.setColumnCount(len(df.columns))
            self.table.setRowCount(len(df))
            self.table.setHorizontalHeaderLabels(df.columns)

            for row in range(len(df)):
                for col, column_name in enumerate(df.columns):
                    value = str(df.iloc[row][column_name])
                    item = QTableWidgetItem(value)
                    if column_name == "status":
                        if value == "success":
                            item.setBackground(Qt.green)
                        elif value == "fail":
                            item.setBackground(Qt.red)
                        elif value == "empty":
                            item.setBackground(Qt.yellow)
                    self.table.setItem(row, col, item)

            self.table.resizeColumnsToContents()

        except Exception as e:
            self.show_message(f"Error loading registry: {e}")

    def refresh_clicked(self):
        refresh_registry(self.base_dir)
        self.load_registry_data()

    def show_message(self, msg):
        QMessageBox.information(self, "Registry Info", msg)
