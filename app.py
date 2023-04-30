import pandas as pd
import sqlite3
import streamlit as st
import base64
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    DateTime,
)


# Infer column types from CSV
def infer_column_types(df):
    type_map = {
        "int64": Integer,
        "float64": Float,
        "datetime64": DateTime,
        "object": String,
    }
    return [type_map[str(df[col].dtype)] for col in df.columns]


# Import CSV data into the database
def csv_to_sqlite(df, db_file, table_name):
    # Create a connection to the SQLite database
    conn = sqlite3.connect(db_file)

    # Write the contents of the DataFrame to the SQLite database
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()


def get_binary_file_downloader_html(bin_file, file_label="File"):
    with open(bin_file, "rb") as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{bin_file}">{file_label}</a>'  # noqa: E501
    return href


st.set_page_config(
    page_title="CSV to SQLite",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("CSV to SQLite Converter")

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file:
    # Read CSV file using pandas
    df = pd.read_csv(uploaded_file)

    # Get input file name and replace the extension with .sqlite
    db_file = uploaded_file.name.replace(".csv", ".sqlite")
    table_name = "input_table"

    # Create database schema
    engine = create_engine(f"sqlite:///{db_file}")
    metadata_obj = MetaData()

    column_types = infer_column_types(df)

    wallet_stats_table = Table(
        table_name,
        metadata_obj,
        *[
            Column(column_name, column_type)
            for column_name, column_type in zip(df.columns, column_types)
        ],
    )

    metadata_obj.create_all(engine)

    csv_to_sqlite(df, db_file, table_name)

    st.success(f"CSV data has been imported into {db_file}.")
    st.markdown(
        get_binary_file_downloader_html(db_file, "Download SQLite Database"),
        unsafe_allow_html=True,
    )
