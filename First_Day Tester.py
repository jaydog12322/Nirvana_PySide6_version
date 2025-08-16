# summary_gui.py

import sys
import os
import pandas as pd
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QPushButton, QFileDialog, QMessageBox
)
from PySide6.QtCore import Qt

from PySide6.QtWidgets import QSpinBox

# -------------------------------
# Summary generation functions
# -------------------------------

def generate_daily_summary(df_results: pd.DataFrame) -> pd.DataFrame:
    df = df_results.copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['exit_date'] = pd.to_datetime(df['exit_date'])

    # Generate full calendar date range
    full_dates = pd.date_range(df['entry_date'].min(), df['exit_date'].max(), freq='D')

    # Calculate stocks held at end of each day
    held_counts = pd.Series([
        df[(df['entry_date'] <= date) & (df['exit_date'] >= date)].shape[0]
        for date in full_dates
    ], index=full_dates)

    # Group by entry_date for daily trade stats
    grouped = df.groupby('entry_date')
    stats_df = pd.DataFrame({
        'return_pct (sum)': grouped['return_pct'].sum(),
        '# of Trades (sum)': grouped.size(),
        'max_gain_pct (average)': grouped['max_gain_pct'].mean(),
        'max_drawdown (average)': grouped['max_drawdown'].mean(),
        'spread_ma5_10 (average)': grouped['spread_ma5_10'].mean(),
        'risk_to_reward (sum)': grouped['risk_to_reward'].sum(),
        'risk_to_reward (average)': grouped['risk_to_reward'].mean(),
        '# of stocks bought (sum)': grouped.size(),
    })

    # Count how many stocks exited on each day
    exit_counts = df['exit_date'].value_counts().rename('# of stocks sold (sum)')

    # Reindex to full calendar range for alignment
    summary = stats_df.reindex(full_dates).fillna(0)
    summary['# of stocks sold (sum)'] = full_dates.map(exit_counts).fillna(0).astype(int)
    summary['# of stocks held (EOD)'] = held_counts

    summary = summary.round(2)

    # Total and average rows
    total_row = summary.sum(numeric_only=True)
    total_row.name = 'Total'
    avg_row = summary.mean(numeric_only=True)
    avg_row.name = 'Average'

    summary = pd.concat([summary, pd.DataFrame([total_row, avg_row])])
    return summary


def generate_monthly_summary(df_results: pd.DataFrame) -> pd.DataFrame:
    df = df_results.copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['month'] = df['entry_date'].dt.to_period('M').astype(str)

    grouped = df.groupby('month')
    return_sum = grouped['return_pct'].sum()
    risk_avg = grouped['risk_to_reward'].mean()
    trade_counts = grouped.size()
    win_counts = grouped.apply(lambda g: (g['return_pct'] > 0).sum())
    lose_counts = grouped.apply(lambda g: (g['return_pct'] <= 0).sum())
    win_ratios = (win_counts / trade_counts).round(2)

    monthly = pd.DataFrame({
        'return_pct (sum)': return_sum,
        'cumulative_return_pct': return_sum.cumsum(),
        'Win (#)': win_counts,
        'Lose (#)': lose_counts,
        'Win Ratio (Win/Total # Trade)': win_ratios,
        'risk_to_reward (average)': risk_avg,
        '# stocks traded (sum)': trade_counts
    })

    monthly = monthly.round(2)
    monthly = monthly.T
    monthly['Total'] = monthly.sum(axis=1)
    monthly['Average'] = monthly.mean(axis=1)
    return monthly


def generate_weekly_summary(df_results: pd.DataFrame) -> pd.DataFrame:
    df = df_results.copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['week'] = df['entry_date'].dt.strftime('%G-W%V')

    grouped = df.groupby('week')
    return_sum = grouped['return_pct'].sum()
    win_counts = grouped.apply(lambda g: (g['return_pct'] > 0).sum())
    lose_counts = grouped.apply(lambda g: (g['return_pct'] <= 0).sum())
    total_counts = grouped.size()
    win_ratios = (win_counts / total_counts).round(2)

    summary = pd.DataFrame({
        'return_pct (sum)': return_sum,
        'Win (#)': win_counts,
        'Lose (#)': lose_counts,
        'Win Ratio (Win#/# of Trades)': win_ratios,
    })

    summary = summary.round(2)
    return summary


def calculate_max_streaks(df: pd.DataFrame) -> tuple:
    df = df.copy()
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    daily_returns = df.groupby('entry_date')['return_pct'].sum().sort_index()

    win_streak = max_win = 0
    for val in daily_returns:
        if val > 0:
            win_streak += 1
            max_win = max(max_win, win_streak)
        else:
            win_streak = 0

    loss_streak = max_loss = 0
    for val in daily_returns:
        if val < 0:
            loss_streak += 1
            max_loss = max(max_loss, loss_streak)
        else:
            loss_streak = 0

    return max_win, max_loss

class SummaryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("First Day Trade Summary Generator")
        self.setMinimumWidth(500)

        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.file_label = QLabel("No file selected")
        self.file_label.setWordWrap(True)
        self.layout.addWidget(self.file_label)

        self.select_button = QPushButton("📁 Select Excel File")
        self.select_button.clicked.connect(self.select_file)
        self.layout.addWidget(self.select_button)

        self.day_label = QLabel("📌 Days_took_to_Entry =")
        self.layout.addWidget(self.day_label)

        self.day_spinbox = QSpinBox()
        self.day_spinbox.setMinimum(1)
        self.day_spinbox.setMaximum(30)  # or adjust higher if needed
        self.day_spinbox.setValue(1)  # default is 1-day entry
        self.layout.addWidget(self.day_spinbox)

        self.run_button = QPushButton("▶️ Generate Summary")
        self.run_button.clicked.connect(self.generate_summary)
        self.run_button.setEnabled(False)
        self.layout.addWidget(self.run_button)

        self.streak_label = QLabel("")
        self.layout.addWidget(self.streak_label)

        self.peak_label = QLabel("📌 Min pre_entry_peak_return_pct =")
        self.layout.addWidget(self.peak_label)

        self.peak_spinbox = QSpinBox()
        self.peak_spinbox.setMinimum(-100)
        self.peak_spinbox.setMaximum(100)
        self.peak_spinbox.setValue(0)
        self.layout.addWidget(self.peak_spinbox)

        self.peak_button = QPushButton("▶️ Generate Pre-Entry Peak Summary")
        self.peak_button.clicked.connect(self.generate_peak_summary)
        self.peak_button.setEnabled(False)
        self.layout.addWidget(self.peak_button)

        self.status_label = QLabel("")
        self.layout.addWidget(self.status_label)

        self.selected_path = None

    def select_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select Excel File", "", "Excel Files (*.xlsx *.xls)")
        if path:
            self.selected_path = path
            self.file_label.setText(f"Selected: {os.path.basename(path)}")
            self.run_button.setEnabled(True)
            self.peak_button.setEnabled(True)
            self.status_label.setText("")
            self.streak_label.setText("")

    def generate_summary(self):
        try:
            df = pd.read_excel(self.selected_path)
            df.columns = df.columns.map(str).str.strip()

            print("Columns in input file:", df.columns.tolist())

            # --- Filter based on Days_took_to_Entry ---
            df['entry_date'] = pd.to_datetime(df['entry_date'])
            selected_day = self.day_spinbox.value()
            first_day_df = df[df['Days_took_to_Entry'] == selected_day]

            if first_day_df.empty:
                QMessageBox.warning(self, "No Data", "No First Day trades found.")
                return

            # --- Generate summaries ---
            daily = generate_daily_summary(first_day_df)
            weekly = generate_weekly_summary(first_day_df)
            monthly = generate_monthly_summary(first_day_df)
            win_streak, loss_streak = calculate_max_streaks(first_day_df)

            # --- Save output ---
            folder = os.path.dirname(self.selected_path)
            base = os.path.splitext(os.path.basename(self.selected_path))[0]
            output_path = os.path.join(folder, f"{base}_Summary.xlsx")

            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                daily.to_excel(writer, sheet_name='Daily Summary')
                weekly.to_excel(writer, sheet_name='Weekly Summary')
                monthly.to_excel(writer, sheet_name='Monthly Summary')

            self.streak_label.setText(f"✅ Max Win Streak: {win_streak} | Max Loss Streak: {loss_streak}")
            self.status_label.setText(f"📄 Summary saved to: {output_path}")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred:\n{str(e)}")

    def generate_peak_summary(self):
        try:
            df = pd.read_excel(self.selected_path)
            df.columns = df.columns.map(str).str.strip()

            df['entry_date'] = pd.to_datetime(df['entry_date'])
            if 'exit_date' in df.columns:
                df['exit_date'] = pd.to_datetime(df['exit_date'])

            min_value = self.peak_spinbox.value()
            filtered_df = df[df['pre_entry_peak_return_pct'] >= min_value]

            if filtered_df.empty:
                QMessageBox.warning(self, "No Data", "No trades found meeting the pre-entry peak threshold.")
                return

            daily = generate_daily_summary(filtered_df)
            weekly = generate_weekly_summary(filtered_df)
            monthly = generate_monthly_summary(filtered_df)
            win_streak, loss_streak = calculate_max_streaks(filtered_df)

            folder = os.path.dirname(self.selected_path)
            base = os.path.splitext(os.path.basename(self.selected_path))[0]
            output_path = os.path.join(folder, f"{base}_PreEntryPeakSummary.xlsx")

            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                daily.to_excel(writer, sheet_name='Daily Summary')
                weekly.to_excel(writer, sheet_name='Weekly Summary')
                monthly.to_excel(writer, sheet_name='Monthly Summary')

            self.streak_label.setText(f"✅ Max Win Streak: {win_streak} | Max Loss Streak: {loss_streak}")
            self.status_label.setText(f"📄 Summary saved to: {output_path}")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred:\n{str(e)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SummaryApp()
    window.show()
    sys.exit(app.exec())
