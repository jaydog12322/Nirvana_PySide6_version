import matplotlib
matplotlib.use('TkAgg')  # <- Add this first
import pandas as pd
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import filedialog, messagebox
import os
import numpy as np

def process_file(file_path):
    try:
        # Load Excel
        df = pd.read_excel(file_path, header=0)

        # Basic validation
        if df.shape[1] < 2:
            messagebox.showerror("Error", "File must have at least two columns.")
            return

        # Use the first two columns
        x_col = df.columns[0]
        y_col = df.columns[1]

        df = df[[x_col, y_col]].copy()
        df = df.dropna()
        df[x_col] = pd.to_numeric(df[x_col], errors='coerce')
        df[y_col] = pd.to_numeric(df[y_col], errors='coerce')
        df = df.dropna()

        # Bin X-axis by 1-unit ranges
        df['bin'] = df[x_col].astype(int)
        grouped = df.groupby('bin')[y_col].mean().reset_index()
        grouped['bin_mid'] = grouped['bin'] + 0.5

        # Save output CSV to same folder
        output_dir = os.path.dirname(file_path)
        output_file = os.path.join(output_dir, "binned_output.csv")
        grouped.to_csv(output_file, index=False)

        # Group values per bin (for boxplot)
        grouped_data = df.groupby('bin')[y_col].apply(list)
        bin_midpoints = [b + 0.5 for b in grouped_data.index]

        # Group stats: mean and std for each bin
        stats = df.groupby('bin')[y_col].agg(['mean', 'std']).reset_index()
        stats['bin_mid'] = stats['bin'] + 0.5

        # Plot custom box: mean ± 1 std
        plt.figure(figsize=(12, 6))

        # Draw box as vertical bar from (mean - std) to (mean + std)
        for i, row in stats.iterrows():
            x = row['bin_mid']
            mean = row['mean']
            std = row['std']

            # Draw the std box
            plt.fill_between([x - 0.3, x + 0.3], mean - std, mean + std, color='lightblue', alpha=0.6)

            # Draw the mean marker
            plt.plot(x, mean, 'o', color='red')

        plt.title(f"{y_col} by {x_col} Bin (Mean ± 1 Std)")
        plt.xlabel(f"{x_col} Bin Midpoint")
        plt.ylabel(y_col)
        plt.xticks(stats['bin_mid'], rotation=45)
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()

        messagebox.showinfo("Done", f"Binned data saved to:\n{output_file}")

    except Exception as e:
        messagebox.showerror("Error", f"Failed to process file:\n{str(e)}")

def select_file():
    file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    if file_path:
        process_file(file_path)

# GUI
root = tk.Tk()
root.title("Risk-Reward Binner")

frame = tk.Frame(root, padx=20, pady=20)
frame.pack()

label = tk.Label(frame, text="Select Excel file with two columns", font=("Arial", 12))
label.pack(pady=10)

button = tk.Button(frame, text="Browse and Run", command=select_file, font=("Arial", 12), width=25)
button.pack(pady=10)

root.mainloop()
