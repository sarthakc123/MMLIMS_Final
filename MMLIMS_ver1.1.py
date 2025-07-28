# app.py
import os
import sqlite3
import pandas as pd
import streamlit as st
import altair as alt


from load_chronect import start_chronect_watcher
from tray_assignment  import assign_rack_to_ready_vials
from init_db import init_db, get_connection

# config
DB_PATH = st.secrets["database"]["STREAMLIT_DB"]

# Folder to watch for new Excel files:
INPUT_DIR = st.secrets["dropbox"]["INPUT_DIR"]

# cached single connection
@st.cache_resource
def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

import load_chronect
load_chronect.DB_PATH = DB_PATH
load_chronect.INPUT_DIR = INPUT_DIR
load_chronect.init_db()
# do an initial bulk load
load_chronect.load_all_chronect_files()
# start the background watcher thread
load_chronect.start_chronect_watcher()

def get_master_df():
    sql = """
    SELECT
      cd.Barcode, cd.Tray, cd.Vial, cd.VialPosition, cd.SampleID, cd.UserID,
      cd.SubstanceName, cd.Head, cd.LotID, cd.TargetWeight, cd.ActualWeight,
      cd.Outcome, cd.DeviationPercent, cd.Date, cd.Time, cd.DispenseDuration,
      cd.ErrorMessage, cd.StableWeight, cd.Timestamp, cd.SourceFile,
      inv.Status, inv.Source AS FactSource,
      hd.RackID, hd.Row, hd.Column
    FROM inventory_fact inv
    LEFT JOIN chronect_data cd ON inv.Barcode = cd.Barcode
    LEFT JOIN hamilton_data hd ON cd.Barcode = hd.Barcode
    """
    conn = get_connection()
    return pd.read_sql(sql, conn)

def update_status(barcodes, new_status):
    conn = get_connection()
    cur  = conn.cursor()
    cur.executemany(
        "UPDATE inventory_fact SET Status = ? WHERE Barcode = ?",
        [(new_status, bc) for bc in barcodes]
    )
    conn.commit()

# 2️⃣ start the background folder watcher
start_chronect_watcher()

# 2) Build UI
st.set_page_config("MML Lab Inventory", layout="wide")
st.title("🧪 MML Lab Inventory Management System")

master_df = get_master_df()

st.subheader("📋 Master Inventory Table")
st.dataframe(master_df, use_container_width=True)

# 3) Add Ready → In Fridge
st.markdown("### 🧊 Add All 'Ready' Vials to Fridge")
if st.button("➕ Add All Ready Vials"):
    assign_rack_to_ready_vials(DB_PATH)
    update_status(
        master_df[master_df.Status == "Ready"]["Barcode"].tolist(),
        "In Fridge"
    )

# 4) In-Fridge Chart
st.subheader("📊 Vials In-Fridge by Substance")
fridge = master_df[master_df.Status == "In Fridge"]
if fridge.empty:
    st.write("No vials currently “In Fridge.”")
else:
    counts = (
        fridge.SubstanceName.value_counts()
        .rename_axis("SubstanceName")
        .reset_index(name="Count")
    )
    chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(
            x="Count:Q",
            y=alt.Y("SubstanceName:N", sort="-x")
        )
    )
    st.altair_chart(chart, use_container_width=True)

# 5) Retrieve by Rack
st.subheader("📦 Retrieve by Rack ID")
rack_opts = sorted(master_df.RackID.dropna().unique().tolist())
sel_rack = st.selectbox("Select Rack", rack_opts)
if st.button("📤 Download Rack CSV"):
    rack_df = master_df[master_df.RackID == sel_rack]
    csv = rack_df[["Barcode"]].to_csv(index=False, header=False).encode()
    st.download_button(
        "⬇️ Download Rack Barcodes",
        data=csv,
        file_name=f"rack_{sel_rack}.csv",
        mime="text/csv",
        key="dl_rack"
    )
    st.session_state["last_downloaded"] = rack_df.Barcode.tolist()

# 6) Retrieve by Substance (FIFO)
st.subheader("🔬 Retrieve by Substance & Count (FIFO)")
subs_opts = sorted(master_df.SubstanceName.dropna().unique())
sel_sub  = st.selectbox("Select Substance", subs_opts, key="substance")
count    = st.slider("Vials to retrieve", 1, 96, 8)
if st.button("📥 Get FIFO List"):
    fifo = (
        master_df[
            (master_df.SubstanceName == sel_sub) &
            (master_df.Status == "In Fridge")
        ]
        .sort_values("Timestamp")
        .head(count)
    )
    if fifo.empty:
        st.warning("No matching vials.")
    else:
        st.dataframe(fifo)
        csv = fifo[["Barcode"]].to_csv(index=False, header=False).encode()
        st.download_button(
            "⬇️ Download FIFO Barcodes",
            data=csv,
            file_name=f"{sel_sub}_fifo.csv",
            mime="text/csv",
            key="dl_fifo"
        )
        st.session_state["last_downloaded"] = fifo.Barcode.tolist()

# 7) Mark completed
if st.session_state.get("last_downloaded"):
    st.markdown("### ✅ Mark Downloaded Vials as Completed")
    if st.button("✅ Mark Completed"):
        update_status(st.session_state["last_downloaded"], "Completed")
        count = len(st.session_state["last_downloaded"])
        st.success(f"{count} vials completed")
        del st.session_state["last_downloaded"]
