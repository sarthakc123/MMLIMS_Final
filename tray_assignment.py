import sqlite3
import pandas as pd

def assign_rack_to_ready_vials(db_path="lab_inventory.db"):
    ROWS = "ABCDEFGH"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get next available rack ID
    cur.execute("SELECT MAX(RackID) FROM hamilton_data")
    max_rack = cur.fetchone()[0] or 0
    rack_id = max_rack + 1

    # Get up to 96 vials that are Ready and not yet in hamilton_data
    df = pd.read_sql("""
        SELECT cd.Barcode, cd.Timestamp
        FROM chronect_data cd
        JOIN inventory_fact inv ON cd.Barcode = inv.Barcode
        LEFT JOIN hamilton_data hd ON cd.Barcode = hd.Barcode
        WHERE inv.Status = 'Ready' AND hd.Barcode is NULL
        ORDER BY cd.Timestamp
        LIMIT 96
    """, conn)

    if df.empty:
        print("No unassigned ready vials found.")
        conn.close()
        return

    # Assign positions and insert
    insert_rows = []
    for idx, row in df.iterrows():
        row_letter = ROWS[idx // 12]
        col_number = (idx % 12) + 1
        insert_rows.append((row["Barcode"], rack_id, row_letter, col_number, None))  # SourceFile optional

    cur.executemany("""
        INSERT OR REPLACE INTO hamilton_data (Barcode, RackID, Row, Column, SourceFile)
        VALUES (?, ?, ?, ?, ?)
    """, insert_rows)

    print("🧾 RACK ASSIGNMENTS TO INSERT:")
    print(insert_rows) #It is empty

    conn.commit()
    conn.close()
    print(f"✅ Assigned Rack {rack_id} to {len(insert_rows)} vials.")