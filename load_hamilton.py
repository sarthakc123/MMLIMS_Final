import sqlite3
import pandas as pd
import os
import re

DB_PATH = "lab_inventory.db"
LAYOUT_FOLDER = "/Users/amd/PycharmProjects/Sarthak Cellario Scripting/HamiltonLayouts"

def find_hamilton_files(folder_path):
    pattern = re.compile(r"\.csv$")  # Match all CSV files
    return [
        os.path.join(folder_path, f)
        for f in sorted(os.listdir(folder_path))
        if pattern.search(f)
    ]

def create_hamilton_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hamilton_layout (
            ChronectBarcode TEXT PRIMARY KEY,
            RackID INTEGER,
            Row TEXT,
            Col INTEGER,
            SourceFile TEXT
        )
    """)

    conn.commit()
    conn.close()

def load_hamilton_files():
    create_hamilton_table()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    files = find_hamilton_files(LAYOUT_FOLDER)
    rack_id = 1

    for file in files:
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip()

            print(f"📥 Loading: {os.path.basename(file)}")

            for _, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO hamilton_layout (ChronectBarcode, RackID, Row, Col, SourceFile)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        row["Chronect Barcode"],
                        rack_id,
                        row["Row"],
                        int(row["Column"]),
                        os.path.basename(file)
                    ))

                    # Also update chronect_data.Status to "In Fridge"
                    cursor.execute("""
                        UPDATE chronect_data SET Status = 'In Fridge' WHERE Barcode = ?
                    """, (row["Chronect Barcode"],))

                except Exception as e:
                    print(f"❌ Failed to insert row {row.to_dict()}: {e}")

            rack_id += 1

        except Exception as e:
            print(f"❌ Failed to load {file}: {e}")

    conn.commit()
    conn.close()
    print("✅ All Hamilton layout data loaded into database.")

if __name__ == "__main__":
    load_hamilton_files()