import os
import re
import pandas as pd
import sqlite3
import streamlit as st
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

DB_TOKEN=st.secrets["chronect"]["token"]
DB_PATH = st.secrets["database"]["path"]

# Folder to watch for new Excel files:
INPUT_DIR = st.secrets["chronect"]["input_dir"]

# ------------------ DB Helpers ------------------

def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    """Run once (or at import) to create your three tables if they don't exist."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS chronect_data (
      Barcode TEXT PRIMARY KEY,
      Tray TEXT, Vial TEXT, VialPosition TEXT,
      SampleID TEXT, UserID TEXT, SubstanceName TEXT,
      Head TEXT, LotID TEXT,
      TargetWeight REAL, ActualWeight REAL,
      Outcome TEXT, DeviationPercent REAL,
      Date TEXT, Time TEXT, DispenseDuration INTEGER,
      ErrorMessage TEXT, StableWeight INTEGER,
      Timestamp TEXT, SourceFile TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS hamilton_data (
      Barcode TEXT PRIMARY KEY,
      RackID INTEGER, Row TEXT, Column INTEGER,
      SourceFile TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS inventory_fact (
      Barcode TEXT PRIMARY KEY,
      Status TEXT DEFAULT 'Ready',
      Source TEXT,
      FOREIGN KEY(Barcode) REFERENCES chronect_data(Barcode)
    )""")
    conn.commit()
    conn.close()

# ------------------ CHRONECT Loader ------------------

def find_chronect_files():
    """List all .xlsx files in INPUT_DIR matching your timestamp pattern."""
    return [
      os.path.join(INPUT_DIR, f)
      for f in os.listdir(INPUT_DIR)
      if re.match(r".*_\d{8}_\d{6}\.xlsx$", f)
    ]

def normalize_columns(df, source_file):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
      "Vial.1":"VialPosition",
      "Substance Name":"SubstanceName",
      "Lot ID":"LotID",
      "Target Weight (mg)":"TargetWeight",
      "Actual Weight (mg)":"ActualWeight",
      "Deviation (%)":"DeviationPercent",
      "Dispense Duration (s)":"DispenseDuration",
      "Stable Weight?":"StableWeight"
    })
    df["Timestamp"] = pd.to_datetime(
      df["Date"].astype(str)+" "+df["Time"].astype(str),
      errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["StableWeight"] = df["StableWeight"].map({True:1,False:0})
    df["SourceFile"] = os.path.basename(source_file)
    return df

def insert_into_database(df):
    conn = get_connection()
    c = conn.cursor()
    chronect_cols = [
      "Barcode","Tray","Vial","VialPosition","SampleID","UserID",
      "SubstanceName","Head","LotID","TargetWeight","ActualWeight",
      "Outcome","DeviationPercent","Date","Time","DispenseDuration",
      "ErrorMessage","StableWeight","Timestamp","SourceFile"
    ]
    for _, row in df.iterrows():
        try:
            # chronect_data
            placeholders = ",".join("?"*len(chronect_cols))
            c.execute(f"""
              INSERT OR IGNORE INTO chronect_data ({','.join(chronect_cols)})
              VALUES ({placeholders})
            """, [row.get(cn) for cn in chronect_cols])
            # inventory_fact
            c.execute("""
              INSERT OR IGNORE INTO inventory_fact (Barcode,Status,Source)
              VALUES (?, 'Ready', 'CHRONECT')
            """, (row["Barcode"],))
        except Exception as e:
            print("❌", row["Barcode"], e)
    conn.commit()
    conn.close()

def load_all_chronect_files():
    files = find_chronect_files()
    for fn in sorted(files):
        print("📥", fn)
        df = pd.read_excel(fn, engine="openpyxl")
        df = normalize_columns(df, fn)
        insert_into_database(df)
    print("✅ All CHRONECT files loaded.")

def load_one_chronect_file(path):
    print("🔔 Detected new file:", path)
    try:
        df = pd.read_excel(path, engine="openpyxl")
        df = normalize_columns(df, path)
        insert_into_database(df)
        print("✅", os.path.basename(path), "ingested.")
    except Exception as e:
        print("❌ Failed to ingest", path, e)

# ------------------ Watchdog ------------------

class ChronectHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".xlsx"):
            load_one_chronect_file(event.src_path)

def start_chronect_watcher():
    observer = Observer()
    observer.schedule(ChronectHandler(), INPUT_DIR, recursive=False)
    observer.daemon = True
    observer.start()
    print("🔍 Watching", INPUT_DIR, "for new CHRONECT files…")