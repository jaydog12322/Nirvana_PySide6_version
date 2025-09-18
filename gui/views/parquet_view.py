# gui/views/parquet_view.py

import os
import pandas as pd
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QListWidget, QTableView,
    QFileDialog, QPushButton, QHBoxLayout, QAbstractItemView,
    QLineEdit, QComboBox
)
from PySide6.QtCore import Qt, QAbstractTableModel, QSortFilterProxyModel, QRegularExpression


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
        self.column_selector = QComboBox()
        self.column_selector.addItem("All Columns", -1)
        self.column_selector.currentIndexChanged.connect(self.update_filter_column)
        self.filter_input = QLineEdit()
        self.filter_input.setPlaceholderText("Type to filter…")
        self.filter_button = QPushButton("Start Filter")
        self.filter_button.clicked.connect(self.apply_filter)
        self.browse_button = QPushButton("📂 Select Folder")
        self.browse_button.clicked.connect(self.select_folder)
        top_bar.addWidget(self.folder_label)
        top_bar.addWidget(self.column_selector)
        top_bar.addWidget(self.filter_input)
        top_bar.addWidget(self.filter_button)
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

        self.model = PandasModel(self.df)
        self.proxy_model = QSortFilterProxyModel(self)
        self.proxy_model.setSourceModel(self.model)
        self.proxy_model.setDynamicSortFilter(True)
        self.table_view.setModel(self.proxy_model)

        self.populate_column_selector()

        self.setLayout(layout)

    def update_filter_column(self, _index=None):
        if not hasattr(self, "proxy_model"):
            return
        column = self.column_selector.currentData()
        column = -1 if column is None else column
        self.proxy_model.setFilterKeyColumn(column)

    def apply_filter(self):
        if not hasattr(self, "proxy_model"):
            return
        pattern = self.filter_input.text()
        if not pattern:
            self.proxy_model.setFilterRegularExpression(QRegularExpression())
            return
        regex = QRegularExpression(pattern)
        if not regex.isValid():
            regex = QRegularExpression(QRegularExpression.escape(pattern))
        regex.setPatternOptions(QRegularExpression.CaseInsensitiveOption)
        self.proxy_model.setFilterRegularExpression(regex)

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
            self.model = PandasModel(self.df)
            self.proxy_model.setSourceModel(self.model)
            self.populate_column_selector()
            self.filter_input.blockSignals(True)
            self.filter_input.clear()
            self.filter_input.blockSignals(False)
            self.proxy_model.setFilterRegularExpression(QRegularExpression())
            self.status_label.setText(
                f"✅ Loaded: {file_name} ({len(self.df):,} rows)"
            )
        except Exception as e:
            self.status_label.setText(f"❌ Failed to load: {e}")

    def populate_column_selector(self):
        self.column_selector.blockSignals(True)
        self.column_selector.clear()
        self.column_selector.addItem("All Columns", -1)
        for idx, column_name in enumerate(self.df.columns):
            self.column_selector.addItem(column_name, idx)
        self.column_selector.setCurrentIndex(0)
        self.column_selector.blockSignals(False)
        self.proxy_model.setFilterKeyColumn(-1)
