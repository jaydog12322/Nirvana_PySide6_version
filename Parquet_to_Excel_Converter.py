#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parquet → CSV / XLSX Converter (Microseconds-safe for Excel)
- Minimal GUI (Tkinter)
- Pick a .parquet file
- Choose output format (CSV or Excel)
- Preserves microseconds by converting the chosen timestamp column to text
  (ISO 8601 "%Y-%m-%d %H:%M:%S.%f").
- Optional Excel-safe mode: prefix text with a leading apostrophe so Excel won't re-parse.
Dependencies:
  - One of: pyarrow OR fastparquet  (required to read parquet)
  - Optional: xlsxwriter or openpyxl (only if saving as .xlsx)
"""
import os
import sys
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox

import pandas as pd

DEFAULT_TS_COL = "timestamp"

def _read_parquet(path):
    """
    Try pyarrow first, then fastparquet.
    Raise a helpful error if neither is available.
    """
    try:
        import pyarrow.parquet as pq  # type: ignore
        table = pq.read_table(path)
        df = table.to_pandas()
        return df, "pyarrow"
    except Exception as e1:
        try:
            import fastparquet as fp  # type: ignore
            df = fp.ParquetFile(path).to_pandas()
            return df, "fastparquet"
        except Exception as e2:
            pyarrow_missing = "No module named 'pyarrow'" in str(e1)
            fastparquet_missing = "No module named 'fastparquet'" in str(e2)
            if pyarrow_missing and fastparquet_missing:
                raise RuntimeError(
                    "Neither 'pyarrow' nor 'fastparquet' is installed.\n\n"
                    "Please install one of them and try again:\n"
                    "  pip install pyarrow\n"
                    "     - or -\n"
                    "  pip install fastparquet\n"
                )
            combined = (
                "Failed to read Parquet with both engines.\n\n"
                f"pyarrow error:\n{e1}\n\n"
                f"fastparquet error:\n{e2}\n"
            )
            raise RuntimeError(combined) from None

def _ensure_ts_text(df: pd.DataFrame, ts_col: str, excel_safe: bool) -> pd.DataFrame:
    """
    Convert a timestamp column to ISO-8601 with microseconds as TEXT to prevent Excel re-parsing.
    If excel_safe=True, prefix with a leading apostrophe so Excel keeps it as text.
    """
    if ts_col not in df.columns:
        return df  # silently ignore if column not found

    s = df[ts_col]

    # If it looks like datetime, use dt.strftime; otherwise try to parse.
    if pd.api.types.is_datetime64_any_dtype(s) or pd.api.types.is_datetime64tz_dtype(s):
        ts_str = s.dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        # Try parsing; handle numbers (epoch) or strings.
        parsed = pd.to_datetime(s, errors="coerce", utc=False, infer_datetime_format=True)
        # If parsing succeeded for at least one row, use it; else leave original as string.
        if parsed.notna().any():
            ts_str = parsed.dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            # As a last resort, cast to string (assume already formatted correctly in parquet)
            ts_str = s.astype(str)

    if excel_safe:
        # Leading apostrophe forces Excel to treat as text (apostrophe is not displayed).
        ts_str = "'" + ts_str

    df = df.copy()
    df[ts_col] = ts_str
    return df

def save_csv(df: pd.DataFrame, out_path: str):
    # Save with UTF-8 BOM for Windows Excel friendliness.
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

def save_xlsx(df: pd.DataFrame, out_path: str):
    # Try xlsxwriter first for speed/compat; fallback to openpyxl.
    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
    except Exception:
        try:
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Sheet1")
        except Exception as e:
            raise RuntimeError(
                "Saving as .xlsx requires 'xlsxwriter' or 'openpyxl'.\n"
                "Install one and try again:\n"
                "  pip install xlsxwriter\n"
                "     - or -\n"
                "  pip install openpyxl\n"
                f"\n\nOriginal error: {e}"
            )

def convert_file(parquet_path: str, out_format: str, ts_col: str, excel_safe: bool):
    if not parquet_path or not os.path.isfile(parquet_path):
        raise FileNotFoundError("Please select a valid .parquet file first.")

    df, engine = _read_parquet(parquet_path)

    # Microseconds-safe handling for the timestamp column
    if ts_col:
        df = _ensure_ts_text(df, ts_col, excel_safe=excel_safe)

    base, _ = os.path.splitext(parquet_path)
    if out_format == "csv":
        out_path = base + ".csv"
        save_csv(df, out_path)
    else:
        out_path = base + ".xlsx"
        save_xlsx(df, out_path)

    return out_path, engine, len(df)

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Parquet → CSV/XLSX (Microseconds-safe)")
        self.geometry("560x260")
        self.resizable(False, False)

        self.selected_path = tk.StringVar(value="")
        self.status_text = tk.StringVar(value="Pick a .parquet file to begin.")
        self.engine_text = tk.StringVar(value="Engine: (auto)")
        self.ts_col = tk.StringVar(value=DEFAULT_TS_COL)
        self.out_format = tk.StringVar(value="csv")  # "csv" or "xlsx"
        self.excel_safe = tk.BooleanVar(value=True)  # prefix apostrophe

        # --- File row ---
        frm = tk.Frame(self, padx=10, pady=10)
        frm.pack(fill="x")

        tk.Label(frm, text="Selected file:").grid(row=0, column=0, sticky="w")
        self.path_entry = tk.Entry(frm, textvariable=self.selected_path, width=58, state="readonly")
        self.path_entry.grid(row=1, column=0, columnspan=2, sticky="we", pady=(2, 8))

        browse_btn = tk.Button(frm, text="Browse…", command=self.on_browse)
        browse_btn.grid(row=1, column=2, padx=(8, 0))

        # --- Options ---
        opt = tk.Frame(self, padx=10)
        opt.pack(fill="x", pady=(0, 6))

        tk.Label(opt, text="Timestamp column:").grid(row=0, column=0, sticky="w")
        tk.Entry(opt, textvariable=self.ts_col, width=20).grid(row=0, column=1, sticky="w", padx=(6, 20))

        tk.Label(opt, text="Output:").grid(row=0, column=2, sticky="w")
        tk.Radiobutton(opt, text="CSV", variable=self.out_format, value="csv").grid(row=0, column=3, sticky="w")
        tk.Radiobutton(opt, text="Excel (.xlsx)", variable=self.out_format, value="xlsx").grid(row=0, column=4, sticky="w")

        tk.Checkbutton(opt, text="Excel-safe microseconds (keep as text)",
                       variable=self.excel_safe).grid(row=1, column=0, columnspan=5, sticky="w", pady=(6,0))

        # --- Convert button ---
        self.convert_btn = tk.Button(self, text="Convert (same folder)", command=self.on_convert, state="disabled")
        self.convert_btn.pack(pady=(2, 6))

        # --- Status ---
        engine_lbl = tk.Label(self, textvariable=self.engine_text, fg="#555")
        engine_lbl.pack()

        status_lbl = tk.Label(self, textvariable=self.status_text, wraplength=520, justify="left")
        status_lbl.pack(pady=(6, 0))

    def on_browse(self):
        path = filedialog.askopenfilename(
            title="Select a Parquet file",
            filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")],
        )
        if path:
            self.selected_path.set(path)
            self.status_text.set("Ready to convert.")
            self.engine_text.set("Engine: (auto)")
            self.convert_btn.config(state="normal")

    def on_convert(self):
        parquet_path = self.selected_path.get().strip()
        try:
            self.convert_btn.config(state="disabled")
            self.status_text.set("Converting… please wait.")
            self.update_idletasks()

            out_path, engine, n_rows = convert_file(
                parquet_path,
                out_format=self.out_format.get(),
                ts_col=self.ts_col.get().strip(),
                excel_safe=self.excel_safe.get(),
            )

            self.engine_text.set(f"Engine: {engine}")
            self.status_text.set(f"✅ Done. Saved: {out_path}\nRows: {n_rows}")
            if messagebox.askyesno("Open Folder?", "Open the output folder now?"):
                self.open_folder(os.path.dirname(out_path))
        except Exception as e:
            traceback.print_exc()
            self.status_text.set(str(e))
            messagebox.showerror("Conversion failed", str(e))
        finally:
            self.convert_btn.config(state="normal")

    @staticmethod
    def open_folder(path):
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore
        elif sys.platform == "darwin":
            os.system(f'open "{path}"')
        else:
            os.system(f'xdg-open "{path}"')

if __name__ == "__main__":
    App().mainloop()
