# load_chronect.py

import os
import re
import pandas as pd
import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

# ─── CONFIG ───────────────────────────
# Point this at your Dropbox‐synced ChronectOutputs folder:
CHRONECT_DIR = os.getenv("CHRONECT_INPUT_DIR",
                         os.path.expanduser("~/Dropbox/ChronectOutputs"))
DB_PATH = os.getenv("LAB_DB_PATH", "lab_inventory.db")

# ─── DB ────────────────────────────────
def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

# ─── NORMALIZE & INSERT ─────────────────
def normalize_columns(df, source_file):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={
        "Tray": "Tray",
        "Vial": "Vial",
        "Vial.1": "VialPosition",
        "Barcode": "Barcode",
        "SampleID": "SampleID",
        "UserID": "UserID",
        "Substance Name": "SubstanceName",
        "Head": "Head",
        "Lot ID": "LotID",
        "Target Weight (mg)": "TargetWeight",
        "Actual Weight (mg)": "ActualWeight",
        "Outcome": "Outcome",
        "Deviation (%)": "DeviationPercent",
        "Date": "Date",
        "Time": "Time",
        "Dispense Duration (s)": "DispenseDuration",
        "Error Message": "ErrorMessage",
        "Stable Weight?": "StableWeight"
    })
    df["Timestamp"] = pd.to_datetime(df["Date"].astype(str) + df["Time"].astype(str), errors="coerce")
    df["Timestamp"] = df["Timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')
    df["StableWeight"] = df["StableWeight"].map({True:1, False:0})
    df["SourceFile"]   = os.path.basename(source_file)
    return df

def insert_into_database(df, conn):
    cursor = conn.cursor()
    chronect_cols = [
        "Barcode","Tray","Vial","VialPosition","SampleID","UserID",
        "SubstanceName","Head","LotID","TargetWeight","ActualWeight",
        "Outcome","DeviationPercent","Date","Time","DispenseDuration",
        "ErrorMessage","StableWeight","Timestamp","SourceFile"
    ]
    for _, row in df.iterrows():
        try:
            # Insert into chronect_data
            cursor.execute(f"""
                INSERT OR IGNORE INTO chronect_data ({','.join(chronect_cols)})
                VALUES ({','.join(['?'] * len(chronect_cols))})
            """, [row.get(col, None) for col in chronect_cols])

            # Insert into inventory_fact
            cursor.execute("""
                INSERT OR IGNORE INTO inventory_fact (Barcode, Status, Source)
                VALUES (?, 'Ready', 'CHRONECT')
            """, (row["Barcode"],))

        except Exception as e:
            print(f"❌ Error inserting row for Barcode {row.get('Barcode')}: {e}")

    conn.commit()

# ─── SINGLE FILE LOADER ─────────────────
def load_one_chronect_file(path, conn=None):
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        df = pd.read_excel(path, engine="openpyxl")
        df = normalize_columns(df, path)
        insert_into_database(df, conn)
        print(f"✅ Ingested {os.path.basename(path)}")
    except Exception as e:
        print(f"❌ Failed {os.path.basename(path)}: {e}")
    finally:
        if close_conn:
            conn.close()

# ─── BULK STARTUP LOADER ─────────────────
def load_all_chronect_files_to_db():
    conn = get_connection()
    pattern = re.compile(r"_\d{8}_\d{6}\.xlsx$")
    for root, _, files in os.walk(CHRONECT_DIR):
        for fn in sorted(files):
            if pattern.search(fn):
                load_one_chronect_file(os.path.join(root, fn), conn)
    conn.close()

# if __name__ == "__main__":
#     conn=get_connection()
#     load_all_chronect_files_to_db()
#     conn.close()

# ─── WATCHDOG SETUP ─────────────────────
class ChronectHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".xlsx"):
            # small delay to allow file to finish writing
            time.sleep(1)
            load_one_chronect_file(event.src_path)

def start_chronect_watcher():
    os.makedirs(CHRONECT_DIR, exist_ok=True)
    obs = Observer()
    obs.schedule(ChronectHandler(), CHRONECT_DIR, recursive=True)
    obs.daemon = True
    obs.start()
    print(f"🔍 Watching {CHRONECT_DIR} for new files…")