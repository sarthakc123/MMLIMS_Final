# MMLIMS_ver_final.py
from tray_assignment import *
from load_chronect import *

import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = "lab_inventory.db"

def get_connection():
    return sqlite3.connect("lab_inventory.db", check_same_thread=False)

def get_master_df():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT 
            cd.Barcode,
            cd.Tray,
            cd.Vial,
            cd.VialPosition,
            cd.SampleID,
            cd.UserID,
            cd.SubstanceName,
            cd.Head,
            cd.LotID,
            cd.TargetWeight,
            cd.ActualWeight,
            cd.Outcome,
            cd.DeviationPercent,
            cd.Date,
            cd.Time,
            cd.DispenseDuration,
            cd.ErrorMessage,
            cd.StableWeight,
            cd.Timestamp,
            cd.SourceFile,
            inv.Status AS Status,
            inv.Source AS FactSource,
            hd.RackID,
            hd.Row,
            hd.Column
        FROM inventory_fact inv
        LEFT JOIN chronect_data cd ON inv.Barcode = cd.Barcode
        LEFT JOIN hamilton_data hd ON cd.Barcode = hd.Barcode
    """, conn)
    conn.close()
    return df

def update_status(barcodes, new_status):
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany("""
        UPDATE inventory_fact SET Status = ? WHERE Barcode = ?
    """, [(new_status, barcode) for barcode in barcodes])
    conn.commit()
    conn.close()

# ----------------- Streamlit UI -------------------
conn=get_connection()
load_all_chronect_files_to_db(conn)
if "last_downloaded_barcodes" not in st.session_state:
    st.session_state.last_downloaded_barcodes = []

st.set_page_config("MML Lab Inventory", layout="wide")
st.title("🧪 MML Lab Inventory Management System")

master_df = get_master_df()
#master_df["Status"]=master_df["Status"].replace("In Fridge","Ready")

# View full table
st.subheader("📋 Master Inventory Table")
st.dataframe(master_df, use_container_width=True)

# ---------- Update Status in Master Table ----------
st.markdown("### 🧊 Add All 'Ready' Vials to Fridge")

if st.button("➕ Add Vials to Fridge"):
    assign_rack_to_ready_vials()  # assign rack first
    conn=get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE inventory_fact
        SET Status = 'In Fridge'
        WHERE Status = 'Ready'
    """)
    conn.commit()
    conn.close()
    st.success("✅ All 'Ready' vials have been marked as 'In Fridge' and assigned to a rack.")

# Retrieve by Rack
st.subheader("📦 Retrieve by Rack ID")
rack_options = sorted(master_df["RackID"].dropna().unique().tolist())
selected_rack = st.selectbox("Select Rack", rack_options)

# For Rack
if st.button("📤 Download Rack Barcode List"):
    rack_df = master_df[master_df["RackID"] == selected_rack]
    csv = rack_df[["Barcode"]].to_csv(index=False, header=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", csv, f"rack_{selected_rack}_barcodes.csv", "text/csv")

    # Save to session for status update
    st.session_state.last_downloaded_barcodes = rack_df["Barcode"].tolist()

# Retrieve by Substance & Vial Count
st.subheader("🔬 Retrieve by Substance and Vial Count (FIFO)")
substance_options = sorted(master_df["SubstanceName"].dropna().unique())
selected_substance = st.selectbox("Select Substance", substance_options)
vial_count = st.number_input("Vials to Retrieve", min_value=1, max_value=96, value=8)

# For FIFO Substance
if st.button("📥 Get FIFO Vials"):
    fifo_df = master_df[
        (master_df["SubstanceName"] == selected_substance) &
        (master_df["Status"] == "In Fridge")
    ].sort_values("Timestamp").head(vial_count)

    if fifo_df.empty:
        st.warning("No matching vials found.")
    else:
        st.dataframe(fifo_df)
        csv = fifo_df[["Barcode"]].to_csv(index=False,header=False).encode("utf-8")
        st.download_button("⬇️ Download FIFO List", csv, f"{selected_substance}_vials.csv", "text/csv")

        # Save barcodes to session
        st.session_state.last_downloaded_barcodes = fifo_df["Barcode"].tolist()

# ✅ Marking Downloaded Vials as Completed (for both rack/substance)
if st.session_state.last_downloaded_barcodes:
    st.markdown("### ✅ Mark Downloaded Vials as Completed")
    if st.button("✅ Mark as Completed"):
        update_status(st.session_state.last_downloaded_barcodes, "Completed")
        st.success(f"{len(st.session_state.last_downloaded_barcodes)} vials marked as Completed.")
        st.session_state.last_downloaded_barcodes = []
        st.rerun()