# gui/views/augmenter_view.py
from PySide6.QtWidgets import QProgressDialog
from PySide6.QtCore import Qt

import os
from pathlib import Path

import pandas as pd
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QPushButton,
    QHBoxLayout, QLineEdit, QCheckBox, QFileDialog,
    QListWidget, QSplitter, QTableView, QMessageBox, QScrollArea
)
from PySide6.QtCore import Qt, QAbstractTableModel

from feature_config import FEATURE_ID_LIST, FEATURE_LABELS
from data_augmenter import generate_enhanced_dataset  # will pass progress_callback later

from PySide6.QtWidgets import QGridLayout


class PandasModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df

    def rowCount(self, _):
        return len(self._df)

    def columnCount(self, _):
        return len(self._df.columns)

    def data(self, index, role):
        if role == Qt.DisplayRole:
            return str(self._df.iloc[index.row(), index.column()])
        return None

    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return self._df.columns[section]
            else:
                return str(section)
        return None


class AugmenterView(QWidget):
    def __init__(self):
        super().__init__()

        self.default_folder = Path("Chart_Data/DailyCandle")
        self.selected_folder = self.default_folder

        self.init_ui()
        self.refresh_file_list()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Folder + strategy bar
        folder_row = QHBoxLayout()
        self.folder_input = QLineEdit(str(self.default_folder))
        self.folder_browse_btn = QPushButton("📂 Browse")
        self.folder_browse_btn.clicked.connect(self.browse_folder)
        folder_row.addWidget(QLabel("Input Folder:"))
        folder_row.addWidget(self.folder_input)
        folder_row.addWidget(self.folder_browse_btn)

        # Strategy ID
        strategy_row = QHBoxLayout()
        self.strategy_input = QLineEdit()
        strategy_row.addWidget(QLabel("Strategy ID:"))
        strategy_row.addWidget(self.strategy_input)

        # 🆕 SCROLLABLE Feature checkboxes
        feature_scroll = QScrollArea()
        feature_scroll.setWidgetResizable(True)
        feature_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        feature_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        feature_scroll.setMaximumHeight(300)  # Limit height to make it scrollable

        feature_widget = QWidget()
        feature_layout = QGridLayout(feature_widget)
        self.feature_checks = {}

        for i, fid in enumerate(FEATURE_ID_LIST):
            cb = QCheckBox(FEATURE_LABELS[fid])
            self.feature_checks[fid] = cb
            row = i // 3  # 3 checkboxes per row
            col = i % 3
            feature_layout.addWidget(cb, row, col)

        feature_scroll.setWidget(feature_widget)

        # Start button
        self.start_btn = QPushButton("🚀 Start Augmentation")
        self.start_btn.clicked.connect(self.start_augmentation)

        # File list and preview pane
        aug_row = QHBoxLayout()
        self.aug_folder_input = QLineEdit("Enhanced_Data")
        self.aug_folder_button = QPushButton("📂 Browse")
        self.aug_folder_button.clicked.connect(self.browse_aug_folder)
        aug_row.addWidget(QLabel("📦 Augmented Folder:"))
        aug_row.addWidget(self.aug_folder_input)
        aug_row.addWidget(self.aug_folder_button)

        self.file_list = QListWidget()
        self.file_list.itemClicked.connect(self.load_preview)

        self.preview = QTableView()
        self.preview.setMinimumHeight(300)

        file_splitter = QSplitter(Qt.Vertical)
        file_splitter.addWidget(self.file_list)
        file_splitter.addWidget(self.preview)
        file_splitter.setSizes([200, 400])

        # Add all components to main layout
        layout.addLayout(aug_row)
        layout.addLayout(folder_row)
        layout.addLayout(strategy_row)
        layout.addWidget(QLabel("📋 Select Features (scroll to see all):"))
        layout.addWidget(feature_scroll)  # 🆕 Now scrollable!
        layout.addWidget(self.start_btn)
        layout.addWidget(QLabel("📄 Files in Folder:"))
        layout.addWidget(file_splitter)

    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder", str(self.default_folder))
        if folder:
            self.selected_folder = Path(folder)
            self.folder_input.setText(str(folder))
            self.refresh_file_list()

    def browse_aug_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder", str(self.aug_folder_input.text()))
        if folder:
            self.aug_folder_input.setText(folder)
            self.refresh_file_list()

    def refresh_file_list(self):
        self.file_list.clear()
        folder = Path(self.aug_folder_input.text())
        if folder.exists():
            for file in sorted(folder.glob("*.parquet")):
                self.file_list.addItem(file.name)

    def load_preview(self, item):
        folder = Path(self.folder_input.text())
        fpath = folder / item.text()
        if not fpath.exists():
            return
        try:
            df = pd.read_parquet(fpath)
            self.preview.setModel(PandasModel(df.head(200)))
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load preview: {e}")

    def start_augmentation(self):
        strategy_id = self.strategy_input.text().strip()
        if not strategy_id:
            QMessageBox.warning(self, "Missing", "Please enter a strategy ID")
            return

        selected_features = [
            fid for fid, cb in self.feature_checks.items()
            if cb.isChecked()
        ]

        if not selected_features:
            QMessageBox.warning(self, "Missing", "Please select at least one feature")
            return

        # Collect list of files to process
        input_folder = Path(self.folder_input.text())
        parquet_files = sorted([f for f in input_folder.glob("*.parquet")])
        total = len(parquet_files)

        if total == 0:
            QMessageBox.warning(self, "No files", "No .parquet files found in the input folder.")
            return

        progress = QProgressDialog("Running augmentation...", "Cancel", 0, total, self)
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.setValue(0)

        output_folder = Path("Enhanced_Data") / strategy_id
        output_folder.mkdir(parents=True, exist_ok=True)

        from data_augmenter import augment_single_file, load_common_stock_info

        try:
            common_df = load_common_stock_info()
            종목명_to_코드 = dict(zip(common_df["종목명"], common_df["종목코드"]))
            보통주_set = set(common_df["종목코드"])
            all_dfs = []

            for i, fpath in enumerate(parquet_files):
                progress.setValue(i)
                if progress.wasCanceled():
                    QMessageBox.information(self, "Cancelled", "Operation cancelled by user.")
                    return

                fname = fpath.name
                stock_code = fname.replace(".parquet", "").split("_")[0]
                if stock_code not in 보통주_set:
                    continue

                df = pd.read_parquet(fpath)
                enhanced_df = augment_single_file(
                    df=df,
                    stock_code=stock_code,
                    종목명_to_코드=종목명_to_코드,
                    selected_features=selected_features
                )

                enhanced_df.to_parquet(output_folder / fname, index=False)
                all_dfs.append(enhanced_df)

            if all_dfs:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                combined_df.to_parquet(output_folder / f"Combined_{strategy_id}.parquet", index=False)

            self.aug_folder_input.setText(str(output_folder))
            QMessageBox.information(self, "Success", "✅ Augmentation completed")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"❌ Failed: {e}")

        progress.setValue(total)
        self.refresh_file_list()