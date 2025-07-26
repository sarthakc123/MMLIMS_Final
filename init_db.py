import streamlit as st
import pandas as pd
import os
import re
from sqlalchemy import create_engine,text

import sqlite3

# Connect or create the SQLite database
DB_PATH='lab_inventory.db'

def get_connection():
    conn = sqlite3.connect(DB_PATH,check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db(conn):
    # Table 1: CHRONECT data
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS chronect_data (
    Barcode TEXT PRIMARY KEY,
    Tray TEXT,
    Vial TEXT,
    VialPosition TEXT,
    SampleID TEXT,
    UserID TEXT,
    SubstanceName TEXT,
    Head TEXT,
    LotID TEXT,
    TargetWeight REAL,
    ActualWeight REAL,
    Outcome TEXT,
    DeviationPercent REAL,
    Date TEXT,
    Time TEXT,
    DispenseDuration INTEGER,
    ErrorMessage TEXT,
    StableWeight INTEGER,
    Timestamp TEXT,
    SourceFile TEXT
    )
    """)
    # Table 2: Hamilton layout data (optional)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hamilton_data (
    Barcode TEXT PRIMARY KEY,
    RackID INTEGER,
    Row TEXT,
    Column INTEGER,
    SourceFile TEXT
    )
    """)

    # Table 3: Fact Table (master status + link)
    # --- Drop existing inventory_fact table ---
    cursor.execute("DROP TABLE IF EXISTS inventory_fact")
    # Recreate inventory_fact table with correct structure
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS inventory_fact (
    Barcode TEXT PRIMARY KEY,
    SubstanceName TEXT,
    Status TEXT DEFAULT 'Ready on Chronect',
    Source TEXT,
    FOREIGN KEY (Barcode) REFERENCES chronect_data(Barcode),
    FOREIGN KEY (Barcode) REFERENCES SubstanceName
    )
    """)
    print("✅ Database and all 3 tables created.")
    conn.commit()

if __name__ == '__main__':
    conn = get_connection()
    init_db(conn)
    conn.close()
