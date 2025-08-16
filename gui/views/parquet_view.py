# gui/views/parquet_view.py

import os
import pandas as pd
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QListWidget, QTableView,
    QFileDialog, QPushButton, QHBoxLayout, QAbstractItemView
)
from PySide6.QtCore import Qt, QAbstractTableModel


class PandasModel(QAbstractTableModel):
    def __init__(self, df=pd.DataFrame()):
        super().__init__()
        self._df = df

    def rowCount(self, parent=None):
        return len(self._df)

    def columnCount(self, parent=None):
        return self._df.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            value = self._df.iloc[index.row(), index.column()]
            return str(value)
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return self._df.columns[section]
            else:
                return str(section + 1)
        return None


class ParquetViewer(QWidget):
    def __init__(self):
        super().__init__()

        self.folder = None
        self.df = pd.DataFrame()

        layout = QVBoxLayout()

        # Top Bar
        top_bar = QHBoxLayout()
        self.folder_label = QLabel("📁 No folder selected")
        self.browse_button = QPushButton("📂 Select Folder")
        self.browse_button.clicked.connect(self.select_folder)
        top_bar.addWidget(self.folder_label)
        top_bar.addStretch()
        top_bar.addWidget(self.browse_button)
        layout.addLayout(top_bar)

        # File list
        self.file_list = QListWidget()
        self.file_list.itemSelectionChanged.connect(self.load_selected_file)
        layout.addWidget(self.file_list)

        self.status_label = QLabel("📂 No file loaded.")
        layout.addWidget(self.status_label)

        # Table View
        self.table_view = QTableView()
        self.table_view.setSortingEnabled(True)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.table_view.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        layout.addWidget(self.table_view, stretch=1)

        self.setLayout(layout)

    def select_folder(self):
        selected = QFileDialog.getExistingDirectory(self, "Select Folder")
        if selected:
            self.folder = selected
            self.folder_label.setText(f"📁 {selected}")
            self.refresh_file_list()

    def refresh_file_list(self):
        self.file_list.clear()
        if not self.folder or not os.path.exists(self.folder):
            return
        for file in sorted(os.listdir(self.folder)):
            if file.endswith(".parquet"):
                self.file_list.addItem(file)

    def load_selected_file(self):
        selected_items = self.file_list.selectedItems()
        if not selected_items:
            return
        file_name = selected_items[0].text()
        full_path = os.path.join(self.folder, file_name)

        try:
            self.df = pd.read_parquet(full_path)
            model = PandasModel(self.df)
            self.table_view.setModel(model)
            self.status_label.setText(
                f"✅ Loaded: {file_name} ({len(self.df):,} rows)"
            )
        except Exception as e:
            self.status_label.setText(f"❌ Failed to load: {e}")
