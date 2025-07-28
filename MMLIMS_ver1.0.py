import streamlit as st
import pandas as pd
import os
import re

# ---------- CONFIG ----------
FOLDER_PATH = "/Users/amd/PycharmProjects/Sarthak Cellario Scripting"
MAX_VIALS = 96


# ---------- FUNCTIONS ----------

def find_chronect_files(folder_path):
    pattern = re.compile(r"_\d{8}_\d{6}\.xlsx$")
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if pattern.search(f) and f.endswith(".xlsx")
    ]

def load_all_chronect_files(folder_path, start_rack_id=1):
    file_list = find_chronect_files(folder_path)
    if not file_list:
        st.warning("⚠️ No CHRONECT files found.")
        return pd.DataFrame()

    dfs = []
    rack_id = start_rack_id

    for file in sorted(file_list):  # Sorting ensures consistent order
        try:
            df = pd.read_excel(file, engine='openpyxl')
            df.columns = df.columns.str.strip()
            df["Timestamp"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
            df["Status"] = "Ready"
            df["Rack ID"] = rack_id  # Assign rack here
            dfs.append(df)
            rack_id += 1  # Next file gets next rack
        except Exception as e:
            st.error(f"❌ Failed to load {file}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def generate_mapped_putlist_df(df):
    rows = "ABCDEFGH"
    barcodes = df.sort_values("Timestamp", ascending=False)["Barcode"].dropna().unique()

    if len(barcodes) > MAX_VIALS:
        st.error(f"❌ {len(barcodes)} vials found. Only {MAX_VIALS} vials allowed per rack.")
        return pd.DataFrame()

    mapped_rows = []
    for i, barcode in enumerate(barcodes[:MAX_VIALS]):
        row = rows[i // 12]
        col = (i % 12) + 1
        mapped_rows.append({
            "Chronect Barcode": barcode,
            "Row": row,
            "Column": col
        })

    return pd.DataFrame(mapped_rows)

def update_status(df, barcodes, new_status):
    df.loc[df["Barcode"].isin(barcodes), "Status"] = new_status
    return df

def build_master_df(chronect_df, putlist_df):
    chronect_df = chronect_df.rename(columns={"Barcode": "Chronect Barcode"})
    return pd.merge(chronect_df, putlist_df, on="Chronect Barcode", how="inner")

# ---------- STREAMLIT UI ----------

st.set_page_config(page_title="MML Lab Inventory Management System", layout="wide")
st.title("MML Lab Inventory Management System")

# Load data
df = load_all_chronect_files(FOLDER_PATH)
if df.empty:
    st.stop()

st.success(f"📊 Loaded {len(df)} records from CHRONECT files.")
st.dataframe(df, use_container_width=True)

# Session state to persist master_df
if "master_df" not in st.session_state:
    st.session_state.master_df = pd.DataFrame()

# Generate putlist and update status
if st.button("Press if Trays are Added to Fridges"):
    putlist_df = generate_mapped_putlist_df(df)
    if not putlist_df.empty:
        df = update_status(df, putlist_df["Chronect Barcode"].tolist(), "In Fridge")
        master_df = build_master_df(df, putlist_df)
        st.session_state.master_df = master_df

        st.subheader("📦 Mapped Hamilton Putlist (Max 96 Vials)")
        st.dataframe(putlist_df)

        csv = putlist_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Putlist CSV", csv, "hamilton_putlist.csv", "text/csv")

# Show + interact with master_df
if not st.session_state.master_df.empty:
    master_df = st.session_state.master_df

    st.subheader("📋 Combined Master DataFrame with Status")
    st.dataframe(master_df, use_container_width=True)

    selectable = master_df["Chronect Barcode"].tolist()
    selectable_rack_id=master_df["Rack ID"].unique().tolist()
    select_substance_name=master_df["Substance Name"].unique().tolist()
    selected = st.multiselect("Select vials to mark as Completed", selectable)
    selected_rack=st.selectbox("Select Rack", selectable_rack_id)

    #Selecting & Downloading Barcodes Through Rack
    if st.button("Select Rack Number"):
        master_df.loc[master_df["Rack ID"]==selected_rack, "Status"]= "Completed"
        st.session_state.master_df = master_df
        master_df = st.session_state.master_df
        st.success(f"All vials in Rack {selected_rack} marked as Completed.")

    #Selecting & Downloading Barcodes through Substance Name in FIFO Manner

    if not st.session_state.master_df.empty:
        master_df = st.session_state.master_df

        st.subheader("🧪 Step 1: Select Substance and Vial Count (FIFO Order)")

        substance_options = master_df["Substance Name"].dropna().unique().tolist()
        selected_substance = st.selectbox("Select Substance", substance_options)
        vial_count = st.number_input("Number of vials", min_value=1, max_value=96, step=1)

        if st.button("➡️ Generate FIFO Vial List"):
            fifo_vials = master_df[
                (master_df["Substance Name"] == selected_substance) &
                (master_df["Status"] == "In Fridge")
                ].sort_values("Timestamp").head(vial_count)

            if fifo_vials.empty:
                st.warning("⚠️ No matching 'Ready' vials found.")
            else:
                st.dataframe(fifo_vials[["Chronect Barcode", "Substance Name", "Timestamp"]])
                barcode_csv = fifo_vials[["Chronect Barcode"]].to_csv(index=False).encode("utf-8")
                st.download_button('⬇️ Download Barcode List', barcode_csv, f'{selected_substance}.csv', 'text/csv')
    csv_master = master_df.to_csv(index=False).encode("utf-8")
    st.download_button("\N{DOWNWARDS BLACK ARROW} Download Master CSV", csv_master, "master_data.csv", "text/csv")
