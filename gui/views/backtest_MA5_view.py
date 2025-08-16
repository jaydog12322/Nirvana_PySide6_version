# backtest_MA5_view.py
from PySide6.QtCore import Qt

from PySide6.QtWidgets import (
    QWidget, QLabel, QPushButton, QLineEdit, QVBoxLayout, QHBoxLayout,
    QGridLayout, QTextEdit, QFileDialog, QGroupBox, QCheckBox
)

import os
import subprocess

class BacktestMA5View(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("MA5 전략 테스트")
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # === Strategy ID Display ===
        strategy_layout = QHBoxLayout()
        strategy_label = QLabel("Strategy ID:")
        self.strategy_id_field = QLineEdit("ma5_support_v1.20.7")

        strategy_layout.addWidget(strategy_label)
        strategy_layout.addWidget(self.strategy_id_field)
        layout.addLayout(strategy_layout)

        # === File Selection ===
        file_layout = QHBoxLayout()
        self.file_path_field = QLineEdit()
        self.browse_button = QPushButton("Browse Parquet File")
        self.browse_button.clicked.connect(self.browse_file)
        file_layout.addWidget(self.file_path_field)
        file_layout.addWidget(self.browse_button)
        layout.addLayout(file_layout)

        # === Manual Trigger File Selection ===
        manual_layout = QHBoxLayout()
        self.manual_trigger_field = QLineEdit()
        self.manual_trigger_browse = QPushButton("Optional: Browse Manual Trigger File")
        self.manual_trigger_browse.clicked.connect(self.browse_manual_trigger)
        manual_layout.addWidget(self.manual_trigger_field)
        manual_layout.addWidget(self.manual_trigger_browse)
        layout.addLayout(manual_layout)


        # === Trigger Parameter Controls ===
        # === Trigger Parameter Controls ===
        param_group = QGroupBox("Trigger Parameter Settings")
        param_vbox = QVBoxLayout()

        def add_param_row(label_text, input_field, toggle):
            row_layout = QHBoxLayout()
            label = QLabel(label_text)
            input_field.setFixedWidth(100)
            toggle.setChecked(True)
            toggle.setToolTip("활성화 / 비활성화")

            row_layout.addWidget(label)
            row_layout.addWidget(input_field)
            row_layout.addWidget(toggle)
            row_layout.addStretch()  # Push everything to the left
            param_vbox.addLayout(row_layout)

        # Input fields and toggles
        self.input_pct_change = QLineEdit("10")
        self.toggle_pct_change = QCheckBox()
        add_param_row("등락률 (%) ≥", self.input_pct_change, self.toggle_pct_change)
        self.input_pct_change_max = QLineEdit("")
        self.toggle_pct_change_max = QCheckBox()
        add_param_row("등락률 (%) ≤", self.input_pct_change_max, self.toggle_pct_change_max)

        self.input_trading_value = QLineEdit("10000000000")
        self.toggle_trading_value = QCheckBox()
        add_param_row("거래대금 (원) ≥", self.input_trading_value, self.toggle_trading_value)
        self.input_trading_value_max = QLineEdit("")
        self.toggle_trading_value_max = QCheckBox()
        add_param_row("거래대금 (원) ≤", self.input_trading_value_max, self.toggle_trading_value_max)

        self.input_foreign = QLineEdit("0")
        self.toggle_foreign = QCheckBox()
        add_param_row("외국인 순매수 ≥", self.input_foreign, self.toggle_foreign)

        self.input_institution = QLineEdit("0")
        self.toggle_institution = QCheckBox()
        add_param_row("기관 순매수 ≥", self.input_institution, self.toggle_institution)

        param_group.setLayout(param_vbox)
        layout.addWidget(param_group)

        # === Fixed Rule Info ===
        rule_box = QGroupBox("Fixed Trigger Conditions")
        rule_layout = QVBoxLayout()

        fixed_rules = [
            "- Previous day must not have 등락률 < -10%",
            "- 종가 must be at least 5% above MA5",
            "- Last 5 days must not be flat (0% 등락률)",
            "- Must dip below MA20 before next trigger allowed",
            "- If MA5 or MA10 touched before valid entry: trigger is canceled"
            "- Trigger day: MA20 must not be more than 4% above MA10"
            "- Trigger day: MA10 must not be more than 5% above MA5"
        ]
        for rule in fixed_rules:
            rule_layout.addWidget(QLabel(rule))

        rule_box.setLayout(rule_layout)
        layout.addWidget(rule_box)

        # === Start Button & Log Output ===
        self.start_button = QPushButton("▶ Run MA5 Backtest")
        self.start_button.clicked.connect(self.run_backtest)

        layout.addWidget(self.start_button)
        layout.addWidget(QLabel("Backtest Log:"))

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        layout.addWidget(self.log_output)

        self.setLayout(layout)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Parquet File", "", "Parquet Files (*.parquet)"
        )
        if file_path:
            self.file_path_field.setText(file_path)
    def browse_manual_trigger(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Manual Trigger Excel File", "", "Excel Files (*.xlsx *.xls)"
        )
        if file_path:
            self.manual_trigger_field.setText(file_path)

    def run_backtest(self):
        parquet_path = self.file_path_field.text().strip()
        if not os.path.exists(parquet_path):
            self.log_output.append("❌ Invalid file path.")
            return

        # Prepare command-line arguments including user parameters
        strategy_id = self.strategy_id_field.text().strip()

        args = [
            "python", "core/backtest_runner_MA5.py",
            parquet_path,
            strategy_id,
            self.input_pct_change.text().strip() if self.toggle_pct_change.isChecked() else "",
            self.input_pct_change_max.text().strip() if self.toggle_pct_change_max.isChecked() else "",
            self.input_trading_value.text().strip() if self.toggle_trading_value.isChecked() else "",
            self.input_trading_value_max.text().strip() if self.toggle_trading_value_max.isChecked() else "",
            self.input_foreign.text().strip() if self.toggle_foreign.isChecked() else "",
            self.input_institution.text().strip() if self.toggle_institution.isChecked() else "",
        ]

        manual_trigger_path = self.manual_trigger_field.text().strip()
        if manual_trigger_path:
            args.append("--manual-trigger")
            args.append(manual_trigger_path)

        self.log_output.append(f"▶ Running backtest on: {parquet_path}")
        self.log_output.append(f"Parameters: {args[2:]}")

        try:
            result = subprocess.run(
                args,
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=True
            )
            self.log_output.append(result.stdout)
        except subprocess.CalledProcessError as e:
            self.log_output.append("❌ Error running backtest:")
            self.log_output.append(e.stderr)
