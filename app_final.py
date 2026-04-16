# ============================================================
# PHONEPE PULSE PROJECT - BEGINNER FRIENDLY VERSION
# This app does 2 main jobs:
# 1) Read PhonePe Pulse JSON files and load them into MySQL
# 2) Show a Streamlit dashboard and analytics
# ============================================================

# -------------------------
# IMPORT LIBRARIES
# -------------------------
import os
import json
import warnings
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import requests
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt

import plotly.graph_objects as go
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore", category=UserWarning)

# -------------------------
# STREAMLIT PAGE SETTINGS
# -------------------------
st.set_page_config(
    page_title="PhonePe Pulse",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------------------
# SIMPLE APP STYLING
# -------------------------
st.markdown("""
<style>
    .stApp { background-color: #1a0b2e; color: white; }
    .pulse-number { font-size: 2.1rem; font-weight: 700; color: #25d366; margin-bottom: 0px; }
    .pulse-title { font-size: 1.0rem; color: #d1d5db; margin-bottom: 15px; }
    .category-row {
        display: flex; justify-content: space-between;
        margin-bottom: 10px; border-bottom: 1px solid #3b2a54; padding-bottom: 5px;
    }
    .category-name { color: white; font-weight: 500; }
    .category-value { color: #25d366; font-weight: bold; }
    .top-row {
        display: flex; justify-content: space-between;
        margin-bottom: 8px; border-bottom: 1px solid #3b2a54; padding-bottom: 4px;
    }
    .rank { color: #a3a3a3; font-weight: 500; margin-right: 10px; }
    .name { color: white; font-weight: 500; flex-grow: 1; }
    .value { color: #25d366; font-weight: bold; }
    .section-header {
        font-size: 1.3rem; font-weight: 700; color: #25d366;
        margin-top: 20px; margin-bottom: 15px;
        border-bottom: 2px solid #25d366; padding-bottom: 5px;
    }
    .chart-title {
        font-size: 1.2rem; font-weight: 700; color: #25d366;
        text-align: center; margin-top: 20px; margin-bottom: 10px;
    }
    header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# -------------------------
# SEABORN STYLE
# -------------------------
sns.set_theme(style="darkgrid", palette="viridis")
plt.style.use("dark_background")

# ============================================================
# BASIC CONFIGURATION
# ============================================================

# MySQL details
DB_USER = "YOUR_USERNAME"
DB_PASSWORD = "YOUR_PASSWORD"
DB_HOST = "localhost"

# latest database name will be saved here
LATEST_DB_FILE = "latest_db.txt"

# csv export folder
BASE_EXPORT_DIR = r"D:\Data\Project\data\export"

# India map geojson
INDIA_GEOJSON_URL = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

# all folder paths we want to process
TASKS = [
    {"path": r"Project/data/aggregated/transaction/country/india/state", "category": "aggr_transaction"},
    {"path": r"Project/data/aggregated/insurance/country/india/state", "category": "aggr_insurance"},
    {"path": r"Project/data/aggregated/user/country/india/state", "category": "aggr_user"},
    {"path": r"Project/data/map/insurance/hover/country/india/state", "category": "map_insurance_hover"},
    {"path": r"Project/data/map/transaction/hover/country/india/state", "category": "map_transaction_hover"},
    {"path": r"Project/data/map/user/hover/country/india/state", "category": "map_user_hover"},
    {"path": r"Project/data/top/insurance/country/india/state", "category": "top_insurance"},
    {"path": r"Project/data/top/transaction/country/india/state", "category": "top_transaction"},
    {"path": r"Project/data/top/user/country/india/state", "category": "top_user"}
]

# category to mysql table mapping
TABLE_NAMES = {
    "aggr_transaction": "Aggregated_transaction",
    "aggr_insurance": "Aggregated_insurance",
    "aggr_user": "Aggregated_user",
    "map_insurance_hover": "Map_insurance",
    "map_transaction_hover": "Map_transaction",
    "map_user_hover": "Map_user",
    "top_insurance": "top_insurance",
    "top_transaction": "top_map",
    "top_user": "Top_user"
}

# create table queries
CREATE_TABLES = [
    """CREATE TABLE IF NOT EXISTS Aggregated_transaction (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50),
        Year INT,
        Quarter INT,
        Transaction_type VARCHAR(100),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18,2)
    )""",

    """CREATE TABLE IF NOT EXISTS Aggregated_insurance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50),
        Year INT,
        Quarter INT,
        Transaction_type VARCHAR(50),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18,2)
    )""",

    """CREATE TABLE IF NOT EXISTS Aggregated_user (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50),
        Year INT,
        Quarter INT,
        Transaction_count BIGINT,
        Transaction_user BIGINT,
        Transaction_brand VARCHAR(50),
        Transaction_percentage DECIMAL(10,4)
    )""",

    """CREATE TABLE IF NOT EXISTS Map_insurance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50),
        Year INT,
        Quarter INT,
        District VARCHAR(100),
        Metric_type VARCHAR(20),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18,2)
    )""",

    """CREATE TABLE IF NOT EXISTS Map_transaction (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50),
        Year INT,
        Quarter INT,
        District VARCHAR(100),
        Metric_type VARCHAR(20),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(18,2)
    )""",

    """CREATE TABLE IF NOT EXISTS Map_user (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50),
        Year INT,
        Quarter INT,
        District VARCHAR(100),
        Registered_users BIGINT,
        App_opens BIGINT
    )""",

    """CREATE TABLE IF NOT EXISTS top_insurance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(100),
        Year INT,
        Quarter INT,
        `Level` VARCHAR(20),
        EntityName VARCHAR(100),
        Metric_type VARCHAR(50),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(20,4)
    )""",

    """CREATE TABLE IF NOT EXISTS top_map (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(100),
        Year INT,
        Quarter INT,
        `Level` VARCHAR(20),
        EntityName VARCHAR(100),
        Metric_type VARCHAR(50),
        Transaction_count BIGINT,
        Transaction_amount DECIMAL(20,2)
    )""",

    """CREATE TABLE IF NOT EXISTS Top_user (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50),
        Year INT,
        Quarter INT,
        `Level` VARCHAR(20),
        Name VARCHAR(50),
        Registered_users BIGINT
    )"""
]

# state latitude and longitude for bubble map
STATE_COORDS = {
    "andaman-&-nicobar-islands": [11.7401, 92.6586],
    "andhra-pradesh": [15.9129, 79.7400],
    "arunachal-pradesh": [28.2180, 94.7278],
    "assam": [26.2006, 92.9376],
    "bihar": [25.0961, 85.3131],
    "chandigarh": [30.7333, 76.7794],
    "chhattisgarh": [21.2787, 81.8661],
    "dadra-and-nagar-haveli-and-daman-and-diu": [20.1809, 73.0169],
    "delhi": [28.7041, 77.1025],
    "goa": [15.2993, 74.1240],
    "gujarat": [22.2587, 71.1924],
    "haryana": [29.0588, 76.0856],
    "himachal-pradesh": [31.1048, 77.1734],
    "jammu-&-kashmir": [33.5000, 75.3000],
    "jharkhand": [23.6102, 85.2799],
    "karnataka": [15.3173, 75.7139],
    "kerala": [10.8505, 76.2711],
    "ladakh": [34.1526, 77.5771],
    "lakshadweep": [10.5667, 72.6417],
    "madhya-pradesh": [22.9734, 78.6569],
    "maharashtra": [19.7515, 75.7139],
    "manipur": [24.6637, 93.9063],
    "meghalaya": [25.4670, 91.3662],
    "mizoram": [23.1645, 92.9376],
    "nagaland": [26.1584, 94.5624],
    "odisha": [20.9517, 85.0985],
    "puducherry": [11.9416, 79.8083],
    "punjab": [31.1471, 75.3412],
    "rajasthan": [27.0238, 74.2179],
    "sikkim": [27.5330, 88.5122],
    "tamil-nadu": [11.1271, 78.6569],
    "telangana": [18.1124, 79.0193],
    "tripura": [23.9408, 91.9882],
    "uttar-pradesh": [26.8467, 80.9462],
    "uttarakhand": [30.0668, 79.0193],
    "west-bengal": [22.9868, 87.8550]
}

# proper state names
STATE_NAMES = {
    "andaman-&-nicobar-islands": "Andaman & Nicobar Islands",
    "andhra-pradesh": "Andhra Pradesh",
    "arunachal-pradesh": "Arunachal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra-and-nagar-haveli-and-daman-and-diu": "Dadra and Nagar Haveli and Daman and Diu",
    "delhi": "Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal-pradesh": "Himachal Pradesh",
    "jammu-&-kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Ladakh",
    "lakshadweep": "Lakshadweep",
    "madhya-pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamil-nadu": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "uttar-pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west-bengal": "West Bengal"
}

# ============================================================
# DATABASE FUNCTIONS
# ============================================================

def get_latest_db_name():
    # this reads the last created database name from a text file
    if os.path.exists(LATEST_DB_FILE):
        with open(LATEST_DB_FILE, "r", encoding="utf-8") as file:
            return file.read().strip()
    return None


def save_latest_db_name(db_name):
    # save latest database name
    with open(LATEST_DB_FILE, "w", encoding="utf-8") as file:
        file.write(db_name)


def make_engine(db_name=None):
    # make mysql connection
    password = quote_plus(DB_PASSWORD)

    if db_name is None:
        db_name = get_latest_db_name()

    if not db_name:
        return None

    connection_url = f"mysql+mysqlconnector://{DB_USER}:{password}@{DB_HOST}/{db_name}"
    return create_engine(connection_url)


def run_sql(query):
    # run sql query and return dataframe
    try:
        engine = make_engine()
        if engine is None:
            st.error("No database found. Please run ETL first.")
            return pd.DataFrame()

        df = pd.read_sql(query, engine)
        engine.dispose()
        return df

    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()


# ============================================================
# ETL HELPERS
# ============================================================

def empty_data_structure(category):
    # based on category, create empty columns
    if category in ["aggr_transaction", "aggr_insurance"]:
        return {
            "State": [],
            "Year": [],
            "Quarter": [],
            "Transaction_type": [],
            "Transaction_count": [],
            "Transaction_amount": []
        }

    if category in ["map_insurance_hover", "map_transaction_hover"]:
        return {
            "State": [],
            "Year": [],
            "Quarter": [],
            "District": [],
            "Metric_type": [],
            "Transaction_count": [],
            "Transaction_amount": []
        }

    if category == "map_user_hover":
        return {
            "State": [],
            "Year": [],
            "Quarter": [],
            "District": [],
            "Registered_users": [],
            "App_opens": []
        }

    if category in ["top_transaction", "top_insurance"]:
        return {
            "State": [],
            "Year": [],
            "Quarter": [],
            "Level": [],
            "EntityName": [],
            "Metric_type": [],
            "Transaction_count": [],
            "Transaction_amount": []
        }

    if category == "top_user":
        return {
            "State": [],
            "Year": [],
            "Quarter": [],
            "Level": [],
            "Name": [],
            "Registered_users": []
        }

    # default structure for aggregated user
    return {
        "State": [],
        "Year": [],
        "Quarter": [],
        "Transaction_count": [],
        "Transaction_user": [],
        "Transaction_brand": [],
        "Transaction_percentage": []
    }


def read_one_category(folder_path, category):
    # this function reads all json files inside one category folder
    full_path = os.path.abspath(folder_path)
    data_dict = empty_data_structure(category)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Path not found: {full_path}")

    # loop through states
    for state in os.listdir(full_path):
        state_path = os.path.join(full_path, state)

        if not os.path.isdir(state_path):
            continue

        # loop through years
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)

            if not os.path.isdir(year_path):
                continue

            # loop through quarter files
            for filename in os.listdir(year_path):
                if not filename.endswith(".json"):
                    continue

                file_path = os.path.join(year_path, filename)

                with open(file_path, "r", encoding="utf-8") as f:
                    json_data = json.load(f)

                data = json_data.get("data", {})
                if data is None:
                    continue

                quarter = int(os.path.splitext(filename)[0])

                # aggregated transaction / insurance
                if category in ["aggr_transaction", "aggr_insurance"]:
                    for item in data.get("transactionData", []) or []:
                        payment_info = (item.get("paymentInstruments") or [{}])[0]
                        #payment_list = item.get("paymentInstruments")
                        #if payment_list is not None and len(payment_list) > 0:
                            #payment_info = payment_list[0]
                        #else:
                            #payment_info = {}

                        data_dict["State"].append(state)
                        data_dict["Year"].append(int(year))
                        data_dict["Quarter"].append(quarter)
                        data_dict["Transaction_type"].append(item.get("name"))
                        data_dict["Transaction_count"].append(payment_info.get("count", 0))
                        data_dict["Transaction_amount"].append(payment_info.get("amount", 0))

                # aggregated user
                elif category == "aggr_user":
                    total_users = data.get("aggregated", {}).get("registeredUsers", 0)
                    #total_users = data["aggregated"]["registeredUsers"] -- to avoid keyError

                    for device in data.get("usersByDevice") or []:
                        data_dict["State"].append(state)
                        data_dict["Year"].append(int(year))
                        data_dict["Quarter"].append(quarter)
                        data_dict["Transaction_count"].append(device.get("count", 0))
                        data_dict["Transaction_user"].append(total_users)
                        data_dict["Transaction_brand"].append(device.get("brand"))
                        data_dict["Transaction_percentage"].append(device.get("percentage", 0))

                # map insurance or map transaction
                elif category in ["map_insurance_hover", "map_transaction_hover"]:
                    for item in data.get("hoverDataList") or []:
                        for metric in item.get("metric") or []:
                            data_dict["State"].append(state)
                            data_dict["Year"].append(int(year))
                            data_dict["Quarter"].append(quarter)
                            data_dict["District"].append(item.get("name"))
                            data_dict["Metric_type"].append(metric.get("type"))
                            data_dict["Transaction_count"].append(metric.get("count", 0))
                            data_dict["Transaction_amount"].append(metric.get("amount", 0))

                # map user
                elif category == "map_user_hover":
                    #payment_info = item["paymentInstruments"][0]
                    ## for district, metrics in data["hoverData"].items():
                    for district, metrics in (data.get("hoverData") or {}).items():
                        data_dict["State"].append(state)
                        data_dict["Year"].append(int(year))
                        data_dict["Quarter"].append(quarter)
                        data_dict["District"].append(district)
                        data_dict["Registered_users"].append(metrics.get("registeredUsers", 0))
                        data_dict["App_opens"].append(metrics.get("appOpens", 0))

                # top transaction / top insurance
                elif category in ["top_transaction", "top_insurance"]:
                    for item in data.get("districts") or []:
                        metric = item.get("metric") or {}
                        data_dict["State"].append(state)
                        data_dict["Year"].append(int(year))
                        data_dict["Quarter"].append(quarter)
                        data_dict["Level"].append("district")
                        data_dict["EntityName"].append(item.get("entityName"))
                        data_dict["Metric_type"].append(metric.get("type"))
                        data_dict["Transaction_count"].append(metric.get("count", 0))
                        data_dict["Transaction_amount"].append(metric.get("amount", 0))

                    for item in data.get("pincodes") or []:
                        metric = item.get("metric") or {}
                        data_dict["State"].append(state)
                        data_dict["Year"].append(int(year))
                        data_dict["Quarter"].append(quarter)
                        data_dict["Level"].append("pincode")
                        data_dict["EntityName"].append(item.get("entityName"))
                        data_dict["Metric_type"].append(metric.get("type"))
                        data_dict["Transaction_count"].append(metric.get("count", 0))
                        data_dict["Transaction_amount"].append(metric.get("amount", 0))

                # top user
                elif category == "top_user":
                    for item in data.get("districts") or []:
                        data_dict["State"].append(state)
                        data_dict["Year"].append(int(year))
                        data_dict["Quarter"].append(quarter)
                        data_dict["Level"].append("district")
                        data_dict["Name"].append(item.get("name"))
                        data_dict["Registered_users"].append(item.get("registeredUsers", 0))

                    for item in data.get("pincodes") or []:
                        data_dict["State"].append(state)
                        data_dict["Year"].append(int(year))
                        data_dict["Quarter"].append(quarter)
                        data_dict["Level"].append("pincode")
                        data_dict["Name"].append(item.get("name"))
                        data_dict["Registered_users"].append(item.get("registeredUsers", 0))

    return pd.DataFrame(data_dict)


def save_csv(df, category_name):
    # save each dataframe as csv
    folder = os.path.join(BASE_EXPORT_DIR, category_name)
    os.makedirs(folder, exist_ok=True)

    time_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(folder, f"{category_name}_{time_now}.csv")

    df.to_csv(file_path, index=False)
    return file_path


def insert_dataframe_to_mysql(df, table_name, engine):
    # insert dataframe rows into mysql table
    if df.empty:
        return 0
    #df.to_sql(table_name, con=engine, if_exists='append', index=False)
    #return len(df)
    
    columns = ", ".join(f"`{col}`" for col in df.columns)
    placeholders = ", ".join(f":{col}" for col in df.columns)
    query = text(f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})")

    with engine.connect() as conn:
        for _, row in df.iterrows():
            row_data = {}
            for col in df.columns:
                value = row[col]
                row_data[col] = None if pd.isna(value) else value

            conn.execute(query, row_data)

        conn.commit()

    return len(df)


def run_full_etl(status_box):
    # this function runs the full ETL process
    new_db_name = f"phone_pe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    password = quote_plus(DB_PASSWORD)

    # step 1 - create database
    status_box.info("Creating new MySQL database...")
    admin_engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{password}@{DB_HOST}")

    with admin_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE `{new_db_name}`"))
        conn.commit()

    admin_engine.dispose()

    # step 2 - connect to new database and create tables
    db_engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{password}@{DB_HOST}/{new_db_name}")

    with db_engine.connect() as conn:
        for query in CREATE_TABLES:
            conn.execute(text(query))
        conn.commit()

    status_box.success(f"Database created: {new_db_name}")

    # step 3 - read each category and load into mysql
    for task in TASKS:
        category = task["category"]
        folder_path = task["path"]
        table_name = TABLE_NAMES[category]

        try:
            status_box.info(f"Reading {category} data...")

            df = read_one_category(folder_path, category)

            save_csv(df, category)

            rows = insert_dataframe_to_mysql(df, table_name, db_engine)

            status_box.success(f"Loaded {rows} rows into {table_name}")

        except Exception as e:
            status_box.error(f"{category} failed: {e}")

    db_engine.dispose()

    # step 4 - save latest db name
    save_latest_db_name(new_db_name)

    return new_db_name


# ============================================================
# MAP FUNCTION
# ============================================================

@st.cache_data(ttl=3600)
def load_india_geojson():
    # load india geojson one time and cache it
    try:
        response = requests.get(INDIA_GEOJSON_URL, timeout=15)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Could not load India geojson. Status code: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error loading India geojson: {e}")
        return None


def make_india_map(df):
    # make india state map with bubbles
    if df.empty:
        return go.Figure()

    if "State" not in df.columns or "Value" not in df.columns or "Count" not in df.columns:
        return go.Figure()

    geojson_data = load_india_geojson()
    if geojson_data is None:
        return go.Figure()

    df = df.copy()
    df["lat"] = df["State"].map(lambda x: STATE_COORDS.get(x, [None, None])[0])
    df["lon"] = df["State"].map(lambda x: STATE_COORDS.get(x, [None, None])[1])
    df["State_Name"] = df["State"].map(lambda x: STATE_NAMES.get(x, x))
    df = df.dropna(subset=["lat", "lon"])

    if df.empty:
        return go.Figure()

    max_value = df["Value"].max()
    min_value = df["Value"].min()

    if max_value == min_value:
        df["bubble_size"] = 12
    else:
        df["bubble_size"] = 6 + 18 * (df["Value"] - min_value) / (max_value - min_value)

    fig = go.Figure()

    # state background
    fig.add_trace(go.Choropleth(
        geojson=geojson_data,
        featureidkey="properties.ST_NM",
        locations=df["State_Name"],
        z=[1] * len(df),
        showscale=False,
        marker_line_color="white",
        marker_line_width=0.8,
        colorscale=[[0, "#2b144d"], [1, "#2b144d"]],
        hoverinfo="skip"
    ))

    # bubble points
    fig.add_trace(go.Scattergeo(
        lat=df["lat"],
        lon=df["lon"],
        mode="markers",
        text=df.apply(
            lambda row: (
                f"<b>{row['State_Name']}</b><br>"
                f"Value: {row['Value']:,.0f}<br>"
                f"Count: {row['Count']:,.0f}"
            ),
            axis=1
        ),
        hoverinfo="text",
        marker=dict(
            size=df["bubble_size"],
            color=df["Value"],
            colorscale="Viridis",
            showscale=False,
            opacity=0.88
        )
    ))

    fig.update_geos(
        fitbounds="locations",
        visible=False,
        projection_type="mercator",
        showland=False,
        showcountries=False,
        showcoastlines=False,
        showsubunits=False,
        showframe=False,
        bgcolor="rgba(0,0,0,0)"
    )

    fig.update_layout(
        paper_bgcolor="#1a0b2e",
        plot_bgcolor="#1a0b2e",
        geo_bgcolor="#1a0b2e",
        font={"color": "white"},
        height=620,
        margin=dict(l=0, r=0, t=0, b=0)
    )

    return fig


# ============================================================
# SEABORN CHART FUNCTIONS
# ============================================================

def style_chart(ax, title):
    # apply same style to all matplotlib charts
    ax.set_title(title, color="#25d366", fontsize=14, fontweight="bold")
    ax.set_facecolor("#1a0b2e")
    ax.figure.set_facecolor("#1a0b2e")
    ax.tick_params(colors="white")
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")

    for spine in ax.spines.values():
        spine.set_color("#3b2a54")

    ax.grid(color="#3b2a54", alpha=0.4)


def make_bar_chart(df, x_col, y_col, title):
    # horizontal bar chart
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=df, x=y_col, y=x_col, ax=ax, palette="viridis")
    style_chart(ax, title)
    ax.set_xlabel(y_col)
    ax.set_ylabel(x_col)

    for container in ax.containers:
        ax.bar_label(container, fmt="%.0f", color="white", padding=3)

    plt.tight_layout()
    return fig


def make_pie_chart(df, label_col, value_col, title):
    # pie chart using matplotlib
    fig, ax = plt.subplots(figsize=(8, 8))
    colors = sns.color_palette("viridis", len(df))

    ax.pie(
        df[value_col],
        labels=df[label_col],
        autopct="%1.1f%%",
        startangle=90,
        colors=colors,
        textprops={"color": "white"}
    )

    ax.set_title(title, color="#25d366", fontsize=14, fontweight="bold")
    fig.patch.set_facecolor("#1a0b2e")
    ax.set_facecolor("#1a0b2e")

    plt.tight_layout()
    return fig


def make_line_chart(df, x_col, y_col, title):
    # line chart
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.lineplot(data=df, x=x_col, y=y_col, marker="o", ax=ax, color="#25d366", linewidth=2.5)
    style_chart(ax, title)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    plt.xticks(rotation=45)
    plt.tight_layout()
    return fig


def make_year_chart(df, title):
    # simple year comparison bar chart
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(data=df, x="Year", y="Value", ax=ax, palette="viridis")
    style_chart(ax, title)
    ax.set_xlabel("Year")
    ax.set_ylabel("Value")

    for container in ax.containers:
        ax.bar_label(container, fmt="%.0f", color="white", padding=3)

    plt.tight_layout()
    return fig


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.markdown("## 🟣 PhonePe Pulse")
st.sidebar.markdown("---")

menu = st.sidebar.radio(
    "Choose Page",
    ["🏠 Dashboard", "📈 Analytics", "🔄 Run ETL Pipeline", "📊 Data Explorer"]
)

latest_db = get_latest_db_name()

if latest_db:
    st.sidebar.success(f"Active DB: {latest_db}")
else:
    st.sidebar.warning("No database found. Run ETL first.")


# ============================================================
# DASHBOARD PAGE
# ============================================================

if menu == "🏠 Dashboard":

    st.markdown(
        "<h1 style='text-align: center; color: white;'>PhonePe Pulse Dashboard</h1>",
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    if not latest_db:
        st.warning("No database found. Please run ETL first.")
    else:
        left_col, middle_col, right_col = st.columns([1, 3.2, 1.3], gap="medium")

        # -------------------------
        # FILTERS
        # -------------------------
        with left_col:
            st.markdown("### Filters")

            selected_type = st.selectbox("Data Type", ["Transactions", "Users", "Insurance"])
            selected_year = st.selectbox("Select Year", [2024, 2023, 2022, 2021, 2020, 2019, 2018])
            selected_quarter_name = st.selectbox(
                "Select Quarter",
                ["Q1 (Jan-Mar)", "Q2 (Apr-Jun)", "Q3 (Jul-Sep)", "Q4 (Oct-Dec)"]
            )
            selected_quarter = int(selected_quarter_name[1])

        # -------------------------
        # SQL QUERIES BASED ON TYPE
        # -------------------------
        if selected_type == "Transactions":
            map_query = f"""
                SELECT State,
                       SUM(Transaction_amount) as Value,
                       SUM(Transaction_count) as Count
                FROM Aggregated_transaction
                WHERE Year={selected_year} AND Quarter={selected_quarter}
                GROUP BY State
            """

            category_query = f"""
                SELECT Transaction_type as Category,
                       SUM(Transaction_count) as Value
                FROM Aggregated_transaction
                WHERE Year={selected_year} AND Quarter={selected_quarter}
                GROUP BY Transaction_type
            """

        elif selected_type == "Users":
            map_query = f"""
                SELECT State,
                       SUM(Registered_users) as Value,
                       SUM(App_opens) as Count
                FROM Map_user
                WHERE Year={selected_year} AND Quarter={selected_quarter}
                GROUP BY State
            """

            category_query = f"""
                SELECT Transaction_brand as Category,
                       SUM(Transaction_count) as Value
                FROM Aggregated_user
                WHERE Year={selected_year} AND Quarter={selected_quarter}
                GROUP BY Transaction_brand
            """

        else:
            map_query = f"""
                SELECT State,
                       SUM(Transaction_amount) as Value,
                       SUM(Transaction_count) as Count
                FROM Aggregated_insurance
                WHERE Year={selected_year} AND Quarter={selected_quarter}
                GROUP BY State
            """

            category_query = f"""
                SELECT Transaction_type as Category,
                       SUM(Transaction_count) as Value
                FROM Aggregated_insurance
                WHERE Year={selected_year} AND Quarter={selected_quarter}
                GROUP BY Transaction_type
            """

        df_map = run_sql(map_query)
        df_category = run_sql(category_query)

        # -------------------------
        # MAP
        # -------------------------
        with middle_col:
            if not df_map.empty:
                india_map = make_india_map(df_map)
                st.plotly_chart(india_map, use_container_width=True, config={"displayModeBar": False})
            else:
                st.warning("No data available for selected filters.")

        # -------------------------
        # METRICS
        # -------------------------
        with right_col:
            if not df_map.empty:
                st.markdown(f"### {selected_type}")
                st.markdown(
                    f"<p style='color:#a3a3a3;'>All India {selected_type} ({selected_year} Q{selected_quarter})</p>",
                    unsafe_allow_html=True
                )

                total_count = df_map["Count"].sum()
                total_value = df_map["Value"].sum()

                if selected_type == "Transactions":
                    st.markdown(f"<p class='pulse-number'>{total_count:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>Total Transactions</p>", unsafe_allow_html=True)

                    st.markdown(f"<p class='pulse-number'>₹{total_value:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>Total Payment Value</p>", unsafe_allow_html=True)

                    if total_count > 0:
                        avg_value = total_value / total_count
                        st.markdown(f"<p class='pulse-number'>₹{avg_value:,.0f}</p>", unsafe_allow_html=True)
                        st.markdown("<p class='pulse-title'>Avg. Transaction Value</p>", unsafe_allow_html=True)

                elif selected_type == "Users":
                    st.markdown(f"<p class='pulse-number'>{total_value:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>Total Registered Users</p>", unsafe_allow_html=True)

                    st.markdown(f"<p class='pulse-number'>{total_count:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>App Opens</p>", unsafe_allow_html=True)

                else:
                    st.markdown(f"<p class='pulse-number'>{total_count:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>Total Policies</p>", unsafe_allow_html=True)

                    st.markdown(f"<p class='pulse-number'>₹{total_value:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>Total Premium Value</p>", unsafe_allow_html=True)

                st.markdown("<div class='section-header'>Categories</div>", unsafe_allow_html=True)

                if not df_category.empty:
                    for _, row in df_category.iterrows():
                        category_name = row["Category"] if pd.notna(row["Category"]) and row["Category"] else "Unknown"
                        st.markdown(f"""
                            <div class='category-row'>
                                <span class='category-name'>{category_name}</span>
                                <span class='category-value'>{row['Value']:,.0f}</span>
                            </div>
                        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # -------------------------
        # TOP STATES / DISTRICTS / PINCODES
        # -------------------------
        if not df_map.empty:
            c1, c2, c3 = st.columns(3, gap="large")

            # top states
            with c1:
                st.markdown("<div class='section-header'>Top 10 States</div>", unsafe_allow_html=True)

                top_states = df_map.nlargest(10, "Value").reset_index(drop=True)
                top_states["State_Name"] = top_states["State"].map(lambda x: STATE_NAMES.get(x, x))

                for i, row in top_states.iterrows():
                    st.markdown(f"""
                        <div class='top-row'>
                            <span class='rank'>{i+1}.</span>
                            <span class='name'>{row['State_Name']}</span>
                            <span class='value'>{row['Value']:,.0f}</span>
                        </div>
                    """, unsafe_allow_html=True)

            # top districts
            with c2:
                st.markdown("<div class='section-header'>Top 10 Districts</div>", unsafe_allow_html=True)

                if selected_type == "Transactions":
                    district_query = f"""
                        SELECT District, SUM(Transaction_count) as Value
                        FROM Map_transaction
                        WHERE Year={selected_year} AND Quarter={selected_quarter}
                        GROUP BY District
                        ORDER BY Value DESC
                        LIMIT 10
                    """
                elif selected_type == "Users":
                    district_query = f"""
                        SELECT District, SUM(Registered_users) as Value
                        FROM Map_user
                        WHERE Year={selected_year} AND Quarter={selected_quarter}
                        GROUP BY District
                        ORDER BY Value DESC
                        LIMIT 10
                    """
                else:
                    district_query = f"""
                        SELECT District, SUM(Transaction_count) as Value
                        FROM Map_insurance
                        WHERE Year={selected_year} AND Quarter={selected_quarter}
                        GROUP BY District
                        ORDER BY Value DESC
                        LIMIT 10
                    """

                df_district = run_sql(district_query)

                if not df_district.empty:
                    for i, row in df_district.reset_index(drop=True).iterrows():
                        district_name = row["District"] if pd.notna(row["District"]) and row["District"] else "Unknown"
                        st.markdown(f"""
                            <div class='top-row'>
                                <span class='rank'>{i+1}.</span>
                                <span class='name'>{district_name}</span>
                                <span class='value'>{row['Value']:,.0f}</span>
                            </div>
                        """, unsafe_allow_html=True)

            # top pincodes
            with c3:
                st.markdown("<div class='section-header'>Top 10 Pincodes</div>", unsafe_allow_html=True)

                if selected_type == "Transactions":
                    pincode_query = f"""
                        SELECT EntityName, SUM(Transaction_count) as Value
                        FROM top_map
                        WHERE Year={selected_year} AND Quarter={selected_quarter} AND Level='pincode'
                        GROUP BY EntityName
                        ORDER BY Value DESC
                        LIMIT 10
                    """
                elif selected_type == "Users":
                    pincode_query = f"""
                        SELECT Name as EntityName, SUM(Registered_users) as Value
                        FROM Top_user
                        WHERE Year={selected_year} AND Quarter={selected_quarter} AND Level='pincode'
                        GROUP BY Name
                        ORDER BY Value DESC
                        LIMIT 10
                    """
                else:
                    pincode_query = f"""
                        SELECT EntityName, SUM(Transaction_count) as Value
                        FROM top_insurance
                        WHERE Year={selected_year} AND Quarter={selected_quarter} AND Level='pincode'
                        GROUP BY EntityName
                        ORDER BY Value DESC
                        LIMIT 10
                    """

                df_pincode = run_sql(pincode_query)

                if not df_pincode.empty:
                    for i, row in df_pincode.reset_index(drop=True).iterrows():
                        pincode_name = row["EntityName"] if pd.notna(row["EntityName"]) and row["EntityName"] else "Unknown"
                        st.markdown(f"""
                            <div class='top-row'>
                                <span class='rank'>{i+1}.</span>
                                <span class='name'>{pincode_name}</span>
                                <span class='value'>{row['Value']:,.0f}</span>
                            </div>
                        """, unsafe_allow_html=True)


# ============================================================
# ANALYTICS PAGE
# ============================================================

elif menu == "📈 Analytics":

    st.markdown(
        "<h1 style='text-align: center; color: white;'>Advanced Analytics</h1>",
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    if not latest_db:
        st.warning("No database found. Please run ETL first.")
    else:
        a1, a2, a3 = st.columns(3)

        with a1:
            analysis_type = st.selectbox("Analysis Type", ["Transactions", "Users", "Insurance"])
        with a2:
            analysis_year = st.selectbox("Year", [2024, 2023, 2022, 2021, 2020, 2019, 2018])
        with a3:
            analysis_quarter = st.selectbox("Quarter", [1, 2, 3, 4])

        st.markdown("---")

        # -------------------------
        # row 1 charts
        # -------------------------
        left_chart, right_chart = st.columns(2, gap="large")

        with left_chart:
            st.markdown("<div class='chart-title'>Top 10 States</div>", unsafe_allow_html=True)

            if analysis_type == "Transactions":
                bar_query = f"""
                    SELECT State, SUM(Transaction_amount) as Value
                    FROM Aggregated_transaction
                    WHERE Year={analysis_year} AND Quarter={analysis_quarter}
                    GROUP BY State
                    ORDER BY Value DESC
                    LIMIT 10
                """
            elif analysis_type == "Users":
                bar_query = f"""
                    SELECT State, SUM(Registered_users) as Value
                    FROM Map_user
                    WHERE Year={analysis_year} AND Quarter={analysis_quarter}
                    GROUP BY State
                    ORDER BY Value DESC
                    LIMIT 10
                """
            else:
                bar_query = f"""
                    SELECT State, SUM(Transaction_amount) as Value
                    FROM Aggregated_insurance
                    WHERE Year={analysis_year} AND Quarter={analysis_quarter}
                    GROUP BY State
                    ORDER BY Value DESC
                    LIMIT 10
                """

            df_bar = run_sql(bar_query)

            if not df_bar.empty:
                df_bar["State_Name"] = df_bar["State"].map(lambda x: STATE_NAMES.get(x, x))
                fig_bar = make_bar_chart(df_bar, "State_Name", "Value", f"Top 10 States - {analysis_type}")
                st.pyplot(fig_bar)
                plt.close(fig_bar)

        with right_chart:
            st.markdown("<div class='chart-title'>Category Distribution</div>", unsafe_allow_html=True)

            if analysis_type == "Transactions":
                pie_query = f"""
                    SELECT Transaction_type as Category, SUM(Transaction_count) as Value
                    FROM Aggregated_transaction
                    WHERE Year={analysis_year} AND Quarter={analysis_quarter}
                    GROUP BY Transaction_type
                """
            elif analysis_type == "Users":
                pie_query = f"""
                    SELECT Transaction_brand as Category, SUM(Transaction_count) as Value
                    FROM Aggregated_user
                    WHERE Year={analysis_year} AND Quarter={analysis_quarter}
                    GROUP BY Transaction_brand
                """
            else:
                pie_query = f"""
                    SELECT Transaction_type as Category, SUM(Transaction_count) as Value
                    FROM Aggregated_insurance
                    WHERE Year={analysis_year} AND Quarter={analysis_quarter}
                    GROUP BY Transaction_type
                """

            df_pie = run_sql(pie_query)

            if not df_pie.empty:
                df_pie = df_pie.dropna(subset=["Category"])
                if not df_pie.empty:
                    fig_pie = make_pie_chart(df_pie, "Category", "Value", f"{analysis_type} Distribution")
                    st.pyplot(fig_pie)
                    plt.close(fig_pie)

        # -------------------------
        # row 2 line chart
        # -------------------------
        st.markdown("<div class='chart-title'>Quarterly Trend Analysis</div>", unsafe_allow_html=True)

        if analysis_type == "Transactions":
            line_query = """
                SELECT Year, Quarter, SUM(Transaction_amount) as Value
                FROM Aggregated_transaction
                GROUP BY Year, Quarter
                ORDER BY Year, Quarter
            """
        elif analysis_type == "Users":
            line_query = """
                SELECT Year, Quarter, SUM(Registered_users) as Value
                FROM Map_user
                GROUP BY Year, Quarter
                ORDER BY Year, Quarter
            """
        else:
            line_query = """
                SELECT Year, Quarter, SUM(Transaction_amount) as Value
                FROM Aggregated_insurance
                GROUP BY Year, Quarter
                ORDER BY Year, Quarter
            """

        df_line = run_sql(line_query)

        if not df_line.empty:
            df_line["Period"] = df_line["Year"].astype(str) + "-Q" + df_line["Quarter"].astype(str)
            fig_line = make_line_chart(df_line, "Period", "Value", f"{analysis_type} Quarterly Trend")
            st.pyplot(fig_line)
            plt.close(fig_line)

        # -------------------------
        # row 3 yoy chart
        # -------------------------
        st.markdown("<div class='chart-title'>Year-over-Year Comparison</div>", unsafe_allow_html=True)

        if analysis_type == "Transactions":
            year_query = f"""
                SELECT Year, SUM(Transaction_amount) as Value
                FROM Aggregated_transaction
                WHERE Quarter={analysis_quarter}
                GROUP BY Year
                ORDER BY Year
            """
        elif analysis_type == "Users":
            year_query = f"""
                SELECT Year, SUM(Registered_users) as Value
                FROM Map_user
                WHERE Quarter={analysis_quarter}
                GROUP BY Year
                ORDER BY Year
            """
        else:
            year_query = f"""
                SELECT Year, SUM(Transaction_amount) as Value
                FROM Aggregated_insurance
                WHERE Quarter={analysis_quarter}
                GROUP BY Year
                ORDER BY Year
            """

        df_year = run_sql(year_query)

        if not df_year.empty:
            df_year["Year"] = df_year["Year"].astype(str)
            fig_year = make_year_chart(df_year, f"Q{analysis_quarter} Year Comparison")
            st.pyplot(fig_year)
            plt.close(fig_year)


# ============================================================
# ETL PAGE
# ============================================================

elif menu == "🔄 Run ETL Pipeline":

    st.markdown(
        "<h1 style='text-align: center; color: white;'>Run ETL Pipeline</h1>",
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("""
    <p style='color: #d1d5db; font-size: 1.05rem;'>
    This page will:
    <br>1. Create a new MySQL database
    <br>2. Create all required tables
    <br>3. Read JSON files from project folders
    <br>4. Save CSV copies
    <br>5. Load everything into MySQL
    </p>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("Start ETL Pipeline"):
        status_area = st.container()

        with st.spinner("Running ETL pipeline... please wait"):
            try:
                db_created = run_full_etl(status_area)
                st.balloons()
                st.success(f"ETL completed successfully. New database: {db_created}")
            except Exception as e:
                st.error(f"ETL failed: {e}")


# ============================================================
# DATA EXPLORER PAGE
# ============================================================

elif menu == "📊 Data Explorer":

    st.markdown(
        "<h1 style='text-align: center; color: white;'>Data Explorer</h1>",
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    if not latest_db:
        st.warning("No database found. Please run ETL first.")
    else:
        selected_table = st.selectbox(
            "Select Table",
            [
                "Aggregated_transaction",
                "Aggregated_insurance",
                "Aggregated_user",
                "Map_transaction",
                "Map_insurance",
                "Map_user",
                "top_map",
                "top_insurance",
                "Top_user"
            ]
        )

        row_limit = st.slider("Number of rows", 10, 1000, 100)

        df_table = run_sql(f"SELECT * FROM `{selected_table}` LIMIT {row_limit}")

        if not df_table.empty:
            st.markdown(f"Showing {len(df_table)} rows from `{selected_table}`")
            st.dataframe(df_table, use_container_width=True)

            csv_data = df_table.to_csv(index=False)
            st.download_button(
                label="Download as CSV",
                data=csv_data,
                file_name=f"{selected_table}.csv",
                mime="text/csv"
            )
        else:
            st.info("No data found in this table.")
