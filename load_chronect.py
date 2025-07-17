import os
import re
import pandas as pd
import sqlite3

# ---------- CONFIG ----------
FOLDER_PATH = "/Users/amd/PycharmProjects/Sarthak Cellario Scripting"
DB_PATH = "lab_inventory.db"

# ---------- HELPERS ----------
def get_connection():
    return sqlite3.connect("lab_inventory.db", check_same_thread=False)

def find_chronect_files(folder_path):
    pattern = re.compile(r"_\d{8}_\d{6}\.xlsx$")
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".xlsx")
    ]

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

    # Format fields
    df["Timestamp"] = pd.to_datetime(df["Date"].astype(str) + df["Time"].astype(str), errors="coerce")
    df["Timestamp"] = df["Timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')
    df["StableWeight"] = df["StableWeight"].apply(lambda x: 1 if x is True else 0 if x is False else None)
    df["SourceFile"] = os.path.basename(source_file)

    return df

def insert_into_database(df,conn):
    cursor = conn.cursor()

    chronect_cols = [
        "Barcode", "Tray", "Vial", "VialPosition", "SampleID", "UserID",
        "SubstanceName", "Head", "LotID", "TargetWeight", "ActualWeight",
        "Outcome", "DeviationPercent", "Date", "Time", "DispenseDuration",
        "ErrorMessage", "StableWeight", "Timestamp", "SourceFile"
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

# ---------- MAIN ----------
def load_all_chronect_files_to_db(conn):
    file_list = find_chronect_files(FOLDER_PATH)
    if not file_list:
        print("⚠️ No CHRONECT files found.")
        return

    for file in sorted(file_list):
        try:
            print(f"📥 Loading: {os.path.basename(file)}")
            df = pd.read_excel(file, engine="openpyxl")
            df = normalize_columns(df, file)
            insert_into_database(df,conn)
        except Exception as e:
            print(f"❌ Failed to load {file}: {e}")

    print("✅ All CHRONECT data loaded into database.")

# ---------- RUN ----------
if __name__ == "__main__":
    conn=get_connection()
    load_all_chronect_files_to_db(conn)
    conn.close()