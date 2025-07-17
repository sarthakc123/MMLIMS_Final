import pandas as pd
import os
import re

def find_chronect_files(folder_path):
    """Find all Excel files in the folder that match CHRONECT's filename pattern."""
    pattern = re.compile(r"_\d{8}_\d{6}\.xlsx$")
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if pattern.search(f) and f.endswith(".xlsx")
    ]

def load_all_chronect_files(folder_path):
    """Load and combine all CHRONECT Excel files in the folder."""
    file_list = find_chronect_files(folder_path)

    if not file_list:
        print("⚠️ No CHRONECT files found in folder.")
        return pd.DataFrame()

    dfs = []
    for file in file_list:
        try:
            df = pd.read_excel(file, engine='openpyxl')
            df.columns = df.columns.str.strip()
            df["Timestamp"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
            dfs.append(df)
            print(f"✅ Loaded: {os.path.basename(file)}")
        except Exception as e:
            print(f"❌ Failed to load {file}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def generate_mapped_putlist_df(df, rack_id=1):
    """
    Generate a Hamilton put list DataFrame for all vials sorted by Timestamp (latest first).
    Caps at 96 vials per rack. Raises error if too many vials.
    """
    rows = "ABCDEFGH"
    max_capacity = 96

    df_sorted = df.sort_values("Timestamp", ascending=False)
    barcodes = df_sorted["Barcode"].dropna().unique()

    if len(barcodes) > max_capacity:
        raise ValueError(f"❌ {len(barcodes)} vials found. Only 96 vials allowed per rack.")

    mapped_rows = []
    for i, barcode in enumerate(barcodes[:max_capacity]):
        row = rows[i // 12]
        col = (i % 12) + 1
        mapped_rows.append({
            "Chronect Barcode": barcode,
            "Rack ID": rack_id,
            "Row": row,
            "Column": col
        })

    return pd.DataFrame(mapped_rows)

def save_putlist_to_file(putlist, output_path):
    """Save the put list to a file."""
    putlist.to_csv(output_path, index=False)


def build_master_df(chronect_df, putlist_df):
    """
    Merge CHRONECT vial data with Hamilton put list layout using Chronect Barcode.
    Returns a master DataFrame with full metadata + storage mapping.
    """
    # Ensure key column is aligned
    chronect_df = chronect_df.rename(columns={"Barcode": "Chronect Barcode"})

    # Merge on Chronect Barcode
    master_df = pd.merge(
        chronect_df,
        putlist_df,
        on="Chronect Barcode",
        how="inner"  # keep only vials present in both
    )

    return master_df

# ------------------ RUN ------------------

if __name__ == "__main__":
    folder = "/Users/amd/PycharmProjects/Sarthak Cellario Scripting"
    output_file = "/Users/amd/PycharmProjects/Sarthak Cellario Scripting/hamilton_putlist.csv"
    df = load_all_chronect_files(folder)
    pd.set_option('display.max_columns', None)
    #print(df.head(10))
    print(f"\n📊 Total records loaded: {len(df)}")
    putlist=generate_mapped_putlist_df(df)
   # print(putlist)
    save_putlist_to_file(putlist, output_file)
    a=build_master_df(df, putlist)
    print(a)
