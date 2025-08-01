import os
import re
import pandas as pd
import sqlite3
import streamlit as st
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import dropbox
from dropbox.exceptions import ApiError
import io

DB_PATH    = st.secrets["database"]["STREAMLIT_DB"]
DBX_TOKEN  = st.secrets["dropbox"]["DBX_TOKEN"]
INPUT_DIR  = st.secrets["dropbox"]["INPUT_DIR"]  # e.g. "/ChronectOutputs"

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

def insert_into_database(df, conn=None):
    """Insert CHRONECT dataframe rows into the database.

    If *conn* is provided the existing connection is used, otherwise a new
    connection is created for the duration of this call.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

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
    dbx = dropbox.Dropbox(DBX_TOKEN)
    try:
        # list all .xlsx in that Dropbox folder
        res = dbx.files_list_folder(INPUT_DIR)

    except ApiError as e:
        # give a helpful message when the folder does not exist
        if isinstance(e.error, dropbox.files.ListFolderError):
            lf_err = e.error
            if lf_err.is_path() and lf_err.get_path().is_not_found():
                print(f"❌ Dropbox folder {DBX_INPUT} not found. Check INPUT_DIR in Streamlit secrets.")
                return
        print("❌ Dropbox API error:", e)

    for entry in res.entries:
        if re.match(r".*_\d{8}_\d{6}\.xlsx$", entry.name):
            print("📥 Loading", entry.name)
            md, resp = dbx.files_download(entry.path_lower)
            df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
            df = normalize_columns(df, entry.name)
            insert_into_database(df)

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
    """Start a watchdog observer on INPUT_DIR if the folder exists."""
    if not os.path.isdir(INPUT_DIR):
        print(f"❌ Local folder {INPUT_DIR} does not exist. Watcher disabled.")
        return

    observer = Observer()
    observer.schedule(ChronectHandler(), INPUT_DIR, recursive=False)
    observer.daemon = True
    observer.start()
    print("🔍 Watching", INPUT_DIR, "for new CHRONECT files…")
    try:
        observer.schedule(ChronectHandler(), INPUT_DIR, recursive=False)
        observer.daemon = True
        observer.start()
        print("🔍 Watching", INPUT_DIR, "for new CHRONECT files…")
    except (FileNotFoundError, OSError) as e:
        print("❌ Failed to start watcher:", e)