# ============================================================
# PHONEPE PULSE - UNIFIED ETL + DASHBOARD APPLICATION
# ============================================================

# --- IMPORTS ---
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import warnings
import os
import json
import requests
from datetime import datetime

warnings.filterwarnings("ignore", category=UserWarning)

st.set_page_config(
    page_title="PhonePe Pulse",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .stApp { background-color: #1a0b2e; color: white; }
    .pulse-number { font-size: 2.2rem; font-weight: 700; color: #25d366; margin-bottom: 0px; }
    .pulse-title { font-size: 1.1rem; color: #d1d5db; margin-bottom: 15px; }
    .category-row {
        display: flex; justify-content: space-between;
        margin-bottom: 10px; border-bottom: 1px solid #3b2a54; padding-bottom: 5px;
    }
    .category-name { color: white; font-weight: 500; }
    .category-value { color: #25d366; font-weight: bold; }
    .top-state-row {
        display: flex; justify-content: space-between;
        margin-bottom: 8px; border-bottom: 1px solid #3b2a54; padding-bottom: 4px;
    }
    .top-state-rank { color: #a3a3a3; font-weight: 500; margin-right: 10px; }
    .top-state-name { color: white; font-weight: 500; flex-grow: 1; }
    .top-state-value { color: #25d366; font-weight: bold; }
    .section-header {
        font-size: 1.4rem; font-weight: 700; color: #25d366;
        margin-top: 20px; margin-bottom: 15px;
        border-bottom: 2px solid #25d366; padding-bottom: 5px;
    }
    .chart-title {
        font-size: 1.3rem; font-weight: 700; color: #25d366;
        text-align: center; margin-top: 30px; margin-bottom: 10px;
    }
    header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# CONFIGURATION
# ============================================================

DB_USER = "YOUR_USERNAME"
DB_PASSWORD = "YOUR_PASSWORD"
DB_HOST = "localhost"
LATEST_DB_FILE = "latest_db.txt"
BASE_EXPORT_DIR = r"D:\Data\Project\data\export"

INDIA_GEOJSON_URL = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

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

TABLE_MAP = {
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

CREATE_TABLE_QUERIES = [
    """CREATE TABLE IF NOT EXISTS Aggregated_transaction (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50), Year INT, Quarter INT,
        Transaction_type VARCHAR(100), Transaction_count BIGINT,
        Transaction_amount DECIMAL(18,2)
    )""",
    """CREATE TABLE IF NOT EXISTS Aggregated_insurance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50), Year INT, Quarter INT,
        Transaction_type VARCHAR(50), Transaction_count BIGINT,
        Transaction_amount DECIMAL(18,2)
    )""",
    """CREATE TABLE IF NOT EXISTS Aggregated_user (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50), Year INT, Quarter INT,
        Transaction_count BIGINT, Transaction_user BIGINT,
        Transaction_brand VARCHAR(50), Transaction_percentage DECIMAL(10,4)
    )""",
    """CREATE TABLE IF NOT EXISTS Map_insurance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50), Year INT, Quarter INT,
        District VARCHAR(100), Metric_type VARCHAR(20),
        Transaction_count BIGINT, Transaction_amount DECIMAL(18,2)
    )""",
    """CREATE TABLE IF NOT EXISTS Map_transaction (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50), Year INT, Quarter INT,
        District VARCHAR(100), Metric_type VARCHAR(20),
        Transaction_count BIGINT, Transaction_amount DECIMAL(18,2)
    )""",
    """CREATE TABLE IF NOT EXISTS Map_user (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50), Year INT, Quarter INT,
        District VARCHAR(100), Registered_users BIGINT, App_opens BIGINT
    )""",
    """CREATE TABLE IF NOT EXISTS top_insurance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(100), Year INT, Quarter INT, `Level` VARCHAR(20),
        EntityName VARCHAR(100), Metric_type VARCHAR(50),
        Transaction_count BIGINT, Transaction_amount DECIMAL(20,4)
    )""",
    """CREATE TABLE IF NOT EXISTS top_map (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(100), Year INT, Quarter INT, `Level` VARCHAR(20),
        EntityName VARCHAR(100), Metric_type VARCHAR(50),
        Transaction_count BIGINT, Transaction_amount DECIMAL(20,2)
    )""",
    """CREATE TABLE IF NOT EXISTS Top_user (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State VARCHAR(50), Year INT, Quarter INT, `Level` VARCHAR(20),
        Name VARCHAR(50), Registered_users BIGINT
    )"""
]

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

CHART_TEMPLATE = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"color": "white"},
    "margin": {"r": 20, "t": 40, "l": 20, "b": 20}
}


# ============================================================
# ETL FUNCTIONS
# ============================================================

def get_columns(category):
    if category in ["aggr_transaction", "aggr_insurance"]:
        return {"State": [], "Year": [], "Quarter": [], "Transaction_type": [],
                "Transaction_count": [], "Transaction_amount": []}
    if category in ["map_insurance_hover", "map_transaction_hover"]:
        return {"State": [], "Year": [], "Quarter": [], "District": [],
                "Metric_type": [], "Transaction_count": [], "Transaction_amount": []}
    if category == "map_user_hover":
        return {"State": [], "Year": [], "Quarter": [], "District": [],
                "Registered_users": [], "App_opens": []}
    if category in ["top_transaction", "top_insurance"]:
        return {"State": [], "Year": [], "Quarter": [], "Level": [],
                "EntityName": [], "Metric_type": [], "Transaction_count": [],
                "Transaction_amount": []}
    if category == "top_user":
        return {"State": [], "Year": [], "Quarter": [], "Level": [],
                "Name": [], "Registered_users": []}
    return {"State": [], "Year": [], "Quarter": [], "Transaction_count": [],
            "Transaction_user": [], "Transaction_brand": [], "Transaction_percentage": []}


def process_category(path_arg, category):
    path = os.path.abspath(path_arg)
    clm = get_columns(category)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Path not found: {path}")

    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
            for file in os.listdir(year_path):
                if not file.endswith(".json"):
                    continue

                with open(os.path.join(year_path, file), "r", encoding="utf-8") as f:
                    data_points = json.load(f)

                data = data_points.get("data", {})
                if data is None:
                    continue
                quarter = int(os.path.splitext(file)[0])

                if category in ["aggr_transaction", "aggr_insurance"]:
                    for item in data.get("transactionData", []) or []:
                        p = (item.get("paymentInstruments") or [{}])[0]
                        clm["State"].append(state)
                        clm["Year"].append(int(year))
                        clm["Quarter"].append(quarter)
                        clm["Transaction_type"].append(item.get("name"))
                        clm["Transaction_count"].append(p.get("count", 0))
                        clm["Transaction_amount"].append(p.get("amount", 0))

                elif category == "aggr_user":
                    u = data.get("aggregated", {}).get("registeredUsers", 0)
                    for device in data.get("usersByDevice") or []:
                        clm["State"].append(state)
                        clm["Year"].append(int(year))
                        clm["Quarter"].append(quarter)
                        clm["Transaction_count"].append(device.get("count", 0))
                        clm["Transaction_user"].append(u)
                        clm["Transaction_brand"].append(device.get("brand"))
                        clm["Transaction_percentage"].append(device.get("percentage", 0))

                elif category in ["map_insurance_hover", "map_transaction_hover"]:
                    for item in data.get("hoverDataList") or []:
                        for m in item.get("metric") or []:
                            clm["State"].append(state)
                            clm["Year"].append(int(year))
                            clm["Quarter"].append(quarter)
                            clm["District"].append(item.get("name"))
                            clm["Metric_type"].append(m.get("type"))
                            clm["Transaction_count"].append(m.get("count", 0))
                            clm["Transaction_amount"].append(m.get("amount", 0))

                elif category == "map_user_hover":
                    for district, metrics in (data.get("hoverData") or {}).items():
                        clm["State"].append(state)
                        clm["Year"].append(int(year))
                        clm["Quarter"].append(quarter)
                        clm["District"].append(district)
                        clm["Registered_users"].append(metrics.get("registeredUsers", 0))
                        clm["App_opens"].append(metrics.get("appOpens", 0))

                elif category in ["top_transaction", "top_insurance"]:
                    for item in data.get("districts") or []:
                        m = item.get("metric") or {}
                        clm["State"].append(state)
                        clm["Year"].append(int(year))
                        clm["Quarter"].append(quarter)
                        clm["Level"].append("district")
                        clm["EntityName"].append(item.get("entityName"))
                        clm["Metric_type"].append(m.get("type"))
                        clm["Transaction_count"].append(m.get("count", 0))
                        clm["Transaction_amount"].append(m.get("amount", 0))
                    for item in data.get("pincodes") or []:
                        m = item.get("metric") or {}
                        clm["State"].append(state)
                        clm["Year"].append(int(year))
                        clm["Quarter"].append(quarter)
                        clm["Level"].append("pincode")
                        clm["EntityName"].append(item.get("entityName"))
                        clm["Metric_type"].append(m.get("type"))
                        clm["Transaction_count"].append(m.get("count", 0))
                        clm["Transaction_amount"].append(m.get("amount", 0))

                elif category == "top_user":
                    for item in data.get("districts") or []:
                        clm["State"].append(state)
                        clm["Year"].append(int(year))
                        clm["Quarter"].append(quarter)
                        clm["Level"].append("district")
                        clm["Name"].append(item.get("name"))
                        clm["Registered_users"].append(item.get("registeredUsers", 0))
                    for item in data.get("pincodes") or []:
                        clm["State"].append(state)
                        clm["Year"].append(int(year))
                        clm["Quarter"].append(quarter)
                        clm["Level"].append("pincode")
                        clm["Name"].append(item.get("name"))
                        clm["Registered_users"].append(item.get("registeredUsers", 0))

    return pd.DataFrame(clm)


def export_csv(df, category):
    folder = os.path.join(BASE_EXPORT_DIR, category)
    os.makedirs(folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(folder, f"{category}_data_{timestamp}.csv")
    df.to_csv(filepath, index=False)
    return filepath


def load_to_mysql(df, table_name, engine):
    if df.empty:
        return 0
    cols = ", ".join(f"`{c}`" for c in df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])
    query = text(f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})")
    with engine.connect() as conn:
        for _, row in df.iterrows():
            row_dict = {c: (None if pd.isna(row[c]) else row[c]) for c in df.columns}
            conn.execute(query, row_dict)
        conn.commit()
    return len(df)


def run_etl_pipeline(status_container):
    db_name = f"phone_pe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    encoded_password = quote_plus(DB_PASSWORD)

    status_container.info("Creating database...")
    admin_engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}")
    with admin_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE `{db_name}`"))
        conn.commit()
    admin_engine.dispose()

    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}/{db_name}")
    with engine.connect() as conn:
        for query in CREATE_TABLE_QUERIES:
            conn.execute(text(query))
        conn.commit()

    status_container.success(f"Database `{db_name}` created with all tables.")

    for task in TASKS:
        category = task["category"]
        table_name = TABLE_MAP[category]
        try:
            status_container.info(f"Processing: {category}...")
            df = process_category(task["path"], category)
            export_csv(df, category)
            rows_loaded = load_to_mysql(df, table_name, engine)
            status_container.success(f"✅ {category}: {rows_loaded} rows → {table_name}")
        except Exception as e:
            status_container.error(f"❌ {category} failed: {e}")

    engine.dispose()

    with open(LATEST_DB_FILE, "w", encoding="utf-8") as f:
        f.write(db_name)
    return db_name


# ============================================================
# DASHBOARD HELPER FUNCTIONS
# ============================================================

def get_latest_db_name():
    if os.path.exists(LATEST_DB_FILE):
        with open(LATEST_DB_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    return None


def get_engine():
    db_name = get_latest_db_name()
    if not db_name:
        return None
    encoded_password = quote_plus(DB_PASSWORD)
    return create_engine(f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}/{db_name}")


def run_query(query):
    try:
        engine = get_engine()
        if engine is None:
            st.error("No database found. Please run ETL first.")
            return pd.DataFrame()
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_india_geojson():
    try:
        response = requests.get(INDIA_GEOJSON_URL, timeout=15)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to load India GeoJSON. Status: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error loading India GeoJSON: {e}")
        return None


def create_india_map(df):
    df = df.copy()

    if df.empty or "State" not in df.columns or "Value" not in df.columns or "Count" not in df.columns:
        return go.Figure()

    india_geojson = load_india_geojson()
    if india_geojson is None:
        return go.Figure()

    df["lat"] = df["State"].map(lambda x: STATE_COORDS.get(x, [None, None])[0])
    df["lon"] = df["State"].map(lambda x: STATE_COORDS.get(x, [None, None])[1])
    df["State_Name"] = df["State"].map(lambda x: STATE_NAMES.get(x, x))
    df = df.dropna(subset=["lat", "lon"])

    if df.empty:
        return go.Figure()

    max_val = df["Value"].max()
    min_val = df["Value"].min()
    if max_val == min_val:
        df["bubble_size"] = 12
    else:
        df["bubble_size"] = 6 + 18 * (df["Value"] - min_val) / (max_val - min_val)

    fig = go.Figure()

    fig.add_trace(go.Choropleth(
        geojson=india_geojson,
        featureidkey="properties.ST_NM",
        locations=df["State_Name"],
        z=[1] * len(df),
        showscale=False,
        marker_line_color="white",
        marker_line_width=0.8,
        colorscale=[[0, "#2b144d"], [1, "#2b144d"]],
        hoverinfo="skip"
    ))

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
# CHART FUNCTIONS (MODIFIED - Removed right color scale only)
# ============================================================

def create_bar_chart(df, x, y, title, color=None):
    fig = px.bar(
        df, x=y, y=x, orientation="h",
        title=title, color=color or y,
        color_continuous_scale="Viridis",
        text=y
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig.update_layout(
        **CHART_TEMPLATE, 
        height=400, 
        showlegend=False,
        coloraxis_showscale=False,  # MODIFIED: Remove right color scale
        xaxis=dict(showgrid=False), 
        yaxis=dict(showgrid=False)
    )
    return fig


def create_pie_chart(df, names, values, title):
    fig = px.pie(
        df, names=names, values=values,
        title=title,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(**CHART_TEMPLATE, height=400, showlegend=True,
                      legend=dict(font=dict(color="white")))
    return fig


def create_line_chart(df, x, y, title, color=None):
    fig = px.line(
        df, x=x, y=y, title=title,
        color=color, markers=True,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig.update_layout(**CHART_TEMPLATE, height=400,
                      xaxis=dict(showgrid=False, title=""),
                      yaxis=dict(showgrid=True, gridcolor="#3b2a54", title=""))
    return fig


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.markdown("## 🟣 PhonePe Pulse")
st.sidebar.markdown("---")

menu = st.sidebar.radio("Navigate", [
    "🏠 Dashboard",
    "📈 Analytics",
    "🔄 Run ETL Pipeline",
    "📊 Data Explorer"
])

db_name = get_latest_db_name()
if db_name:
    st.sidebar.success(f"Active DB: {db_name}")
else:
    st.sidebar.warning("No database found. Run ETL first.")


# ============================================================
# DASHBOARD PAGE
# ============================================================

if menu == "🏠 Dashboard":

    st.markdown("<h1 style='text-align: center; color: white;'>PhonePe Pulse | Explore Data</h1>",
                unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if not db_name:
        st.warning("No database found. Please go to 'Run ETL Pipeline' first.")
    else:
        col_filters, col_map, col_metrics = st.columns([1, 3.2, 1.3], gap="medium")

        with col_filters:
            st.markdown("### Filters")
            data_type = st.selectbox("Data Type", ["Transactions", "Users", "Insurance"])
            year = st.selectbox("Select Year", [2024, 2023, 2022, 2021, 2020, 2019, 2018])
            quarter = st.selectbox("Select Quarter", [
                "Q1 (Jan-Mar)", "Q2 (Apr-Jun)", "Q3 (Jul-Sep)", "Q4 (Oct-Dec)"])
            q_num = int(quarter[1])

        if data_type == "Transactions":
            map_query = f"SELECT State, SUM(Transaction_amount) as Value, SUM(Transaction_count) as Count FROM Aggregated_transaction WHERE Year={year} AND Quarter={q_num} GROUP BY State"
            cat_query = f"SELECT Transaction_type as Category, SUM(Transaction_count) as Value FROM Aggregated_transaction WHERE Year={year} AND Quarter={q_num} GROUP BY Transaction_type"
        elif data_type == "Users":
            map_query = f"SELECT State, SUM(Registered_users) as Value, SUM(App_opens) as Count FROM Map_user WHERE Year={year} AND Quarter={q_num} GROUP BY State"
            cat_query = f"SELECT Transaction_brand as Category, SUM(Transaction_count) as Value FROM Aggregated_user WHERE Year={year} AND Quarter={q_num} GROUP BY Transaction_brand"
        else:
            map_query = f"SELECT State, SUM(Transaction_amount) as Value, SUM(Transaction_count) as Count FROM Aggregated_insurance WHERE Year={year} AND Quarter={q_num} GROUP BY State"
            cat_query = f"SELECT Transaction_type as Category, SUM(Transaction_count) as Value FROM Aggregated_insurance WHERE Year={year} AND Quarter={q_num} GROUP BY Transaction_type"

        df_map = run_query(map_query)
        df_cat = run_query(cat_query)

        with col_map:
            if not df_map.empty:
                fig = create_india_map(df_map)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.warning(f"No {data_type} data for {year} {quarter}.")

        with col_metrics:
            if not df_map.empty:
                st.markdown(f"### {data_type}")
                st.markdown(f"<p style='color:#a3a3a3;'>All India {data_type} ({year} {quarter[:2]})</p>",
                            unsafe_allow_html=True)

                total_count = df_map["Count"].sum()
                total_value = df_map["Value"].sum()

                if data_type == "Transactions":
                    st.markdown(f"<p class='pulse-number'>{total_count:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>Total Transactions</p>", unsafe_allow_html=True)
                    st.markdown(f"<p class='pulse-number'>₹{total_value:,.0f}</p>", unsafe_allow_html=True)
                    st.markdown("<p class='pulse-title'>Total Payment Value</p>", unsafe_allow_html=True)
                    if total_count > 0:
                        avg = total_value / total_count
                        st.markdown(f"<p class='pulse-number'>₹{avg:,.0f}</p>", unsafe_allow_html=True)
                        st.markdown("<p class='pulse-title'>Avg. Transaction Value</p>", unsafe_allow_html=True)

                elif data_type == "Users":
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
                if not df_cat.empty:
                    for _, row in df_cat.iterrows():
                        cat_name = row["Category"] if pd.notna(row["Category"]) and row["Category"] else "Unknown"
                        st.markdown(f"""
                            <div class='category-row'>
                                <span class='category-name'>{cat_name}</span>
                                <span class='category-value'>{row['Value']:,.0f}</span>
                            </div>
                        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        if not df_map.empty:
            col_s, col_d, col_p = st.columns(3, gap="large")

            with col_s:
                st.markdown("<div class='section-header'>Top 10 States</div>", unsafe_allow_html=True)
                top = df_map.nlargest(10, "Value").reset_index(drop=True)
                top["State_Name"] = top["State"].map(lambda x: STATE_NAMES.get(x, x))
                for i, row in top.iterrows():
                    st.markdown(f"""
                        <div class='top-state-row'>
                            <span class='top-state-rank'>{i+1}.</span>
                            <span class='top-state-name'>{row['State_Name']}</span>
                            <span class='top-state-value'>{row['Value']:,.0f}</span>
                        </div>
                    """, unsafe_allow_html=True)

            with col_d:
                st.markdown("<div class='section-header'>Top 10 Districts</div>", unsafe_allow_html=True)
                if data_type == "Transactions":
                    dq = f"SELECT District, SUM(Transaction_count) as Value FROM Map_transaction WHERE Year={year} AND Quarter={q_num} GROUP BY District ORDER BY Value DESC LIMIT 10"
                elif data_type == "Users":
                    dq = f"SELECT District, SUM(Registered_users) as Value FROM Map_user WHERE Year={year} AND Quarter={q_num} GROUP BY District ORDER BY Value DESC LIMIT 10"
                else:
                    dq = f"SELECT District, SUM(Transaction_count) as Value FROM Map_insurance WHERE Year={year} AND Quarter={q_num} GROUP BY District ORDER BY Value DESC LIMIT 10"
                df_d = run_query(dq)
                if not df_d.empty:
                    for i, row in df_d.reset_index(drop=True).iterrows():
                        district_name = row["District"] if pd.notna(row["District"]) and row["District"] else "Unknown"
                        st.markdown(f"""
                            <div class='top-state-row'>
                                <span class='top-state-rank'>{i+1}.</span>
                                <span class='top-state-name'>{district_name}</span>
                                <span class='top-state-value'>{row['Value']:,.0f}</span>
                            </div>
                        """, unsafe_allow_html=True)

            with col_p:
                st.markdown("<div class='section-header'>Top 10 Pincodes</div>", unsafe_allow_html=True)
                if data_type == "Transactions":
                    pq = f"SELECT EntityName, SUM(Transaction_count) as Value FROM top_map WHERE Year={year} AND Quarter={q_num} AND Level='pincode' GROUP BY EntityName ORDER BY Value DESC LIMIT 10"
                elif data_type == "Users":
                    pq = f"SELECT Name as EntityName, SUM(Registered_users) as Value FROM Top_user WHERE Year={year} AND Quarter={q_num} AND Level='pincode' GROUP BY Name ORDER BY Value DESC LIMIT 10"
                else:
                    pq = f"SELECT EntityName, SUM(Transaction_count) as Value FROM top_insurance WHERE Year={year} AND Quarter={q_num} AND Level='pincode' GROUP BY EntityName ORDER BY Value DESC LIMIT 10"
                df_p = run_query(pq)
                if not df_p.empty:
                    for i, row in df_p.reset_index(drop=True).iterrows():
                        entity_name = row["EntityName"] if pd.notna(row["EntityName"]) and row["EntityName"] else "Unknown"
                        st.markdown(f"""
                            <div class='top-state-row'>
                                <span class='top-state-rank'>{i+1}.</span>
                                <span class='top-state-name'>{entity_name}</span>
                                <span class='top-state-value'>{row['Value']:,.0f}</span>
                            </div>
                        """, unsafe_allow_html=True)


# ============================================================
# ANALYTICS PAGE (MODIFIED - Removed right color scale only)
# ============================================================

elif menu == "📈 Analytics":

    st.markdown("<h1 style='text-align: center; color: white;'>📈 Advanced Analytics</h1>",
                unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if not db_name:
        st.warning("No database found. Run ETL first.")
    else:
        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            a_type = st.selectbox("Analysis Type", ["Transactions", "Users", "Insurance"], key="a_type")
        with ac2:
            a_year = st.selectbox("Year", [2024, 2023, 2022, 2021, 2020, 2019, 2018], key="a_year")
        with ac3:
            a_quarter = st.selectbox("Quarter", [1, 2, 3, 4], key="a_quarter")

        st.markdown("---")

        # Row 1: Bar Chart + Pie Chart
        r1c1, r1c2 = st.columns(2, gap="large")

        with r1c1:
            st.markdown("<div class='chart-title'>📊 Top 10 States</div>", unsafe_allow_html=True)
            if a_type == "Transactions":
                bar_query = f"SELECT State, SUM(Transaction_amount) as Value FROM Aggregated_transaction WHERE Year={a_year} AND Quarter={a_quarter} GROUP BY State ORDER BY Value DESC LIMIT 10"
            elif a_type == "Users":
                bar_query = f"SELECT State, SUM(Registered_users) as Value FROM Map_user WHERE Year={a_year} AND Quarter={a_quarter} GROUP BY State ORDER BY Value DESC LIMIT 10"
            else:
                bar_query = f"SELECT State, SUM(Transaction_amount) as Value FROM Aggregated_insurance WHERE Year={a_year} AND Quarter={a_quarter} GROUP BY State ORDER BY Value DESC LIMIT 10"

            df_bar = run_query(bar_query)
            if not df_bar.empty:
                df_bar["State_Name"] = df_bar["State"].map(lambda x: STATE_NAMES.get(x, x))
                fig_bar = create_bar_chart(df_bar, "State_Name", "Value", f"Top 10 States - {a_type}")
                st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

        with r1c2:
            st.markdown("<div class='chart-title'>🥧 Category Distribution</div>", unsafe_allow_html=True)
            if a_type == "Transactions":
                pie_query = f"SELECT Transaction_type as Category, SUM(Transaction_count) as Value FROM Aggregated_transaction WHERE Year={a_year} AND Quarter={a_quarter} GROUP BY Transaction_type"
            elif a_type == "Users":
                pie_query = f"SELECT Transaction_brand as Category, SUM(Transaction_count) as Value FROM Aggregated_user WHERE Year={a_year} AND Quarter={a_quarter} GROUP BY Transaction_brand"
            else:
                pie_query = f"SELECT Transaction_type as Category, SUM(Transaction_count) as Value FROM Aggregated_insurance WHERE Year={a_year} AND Quarter={a_quarter} GROUP BY Transaction_type"

            df_pie = run_query(pie_query)
            if not df_pie.empty:
                df_pie = df_pie.dropna(subset=["Category"])
                if not df_pie.empty:
                    fig_pie = create_pie_chart(df_pie, "Category", "Value", f"{a_type} Distribution")
                    st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})

        # Row 2: Line Chart (Full Width)
        st.markdown("<div class='chart-title'>📈 Quarterly Trend Analysis</div>", unsafe_allow_html=True)
        if a_type == "Transactions":
            line_query = "SELECT Year, Quarter, SUM(Transaction_amount) as Value FROM Aggregated_transaction GROUP BY Year, Quarter ORDER BY Year, Quarter"
        elif a_type == "Users":
            line_query = "SELECT Year, Quarter, SUM(Registered_users) as Value FROM Map_user GROUP BY Year, Quarter ORDER BY Year, Quarter"
        else:
            line_query = "SELECT Year, Quarter, SUM(Transaction_amount) as Value FROM Aggregated_insurance GROUP BY Year, Quarter ORDER BY Year, Quarter"

        df_line = run_query(line_query)
        if not df_line.empty:
            df_line["Period"] = df_line["Year"].astype(str) + "-Q" + df_line["Quarter"].astype(str)
            fig_line = create_line_chart(df_line, "Period", "Value", f"{a_type} - Quarterly Trend")
            st.plotly_chart(fig_line, use_container_width=True, config={"displayModeBar": False})

        # Row 3: Year-over-Year Comparison (Full Width) - MODIFIED
        st.markdown("<div class='chart-title'>📊 Year-over-Year Comparison</div>", unsafe_allow_html=True)
        if a_type == "Transactions":
            yoy_query = f"SELECT Year, SUM(Transaction_amount) as Value FROM Aggregated_transaction WHERE Quarter={a_quarter} GROUP BY Year ORDER BY Year"
        elif a_type == "Users":
            yoy_query = f"SELECT Year, SUM(Registered_users) as Value FROM Map_user WHERE Quarter={a_quarter} GROUP BY Year ORDER BY Year"
        else:
            yoy_query = f"SELECT Year, SUM(Transaction_amount) as Value FROM Aggregated_insurance WHERE Quarter={a_quarter} GROUP BY Year ORDER BY Year"

        df_yoy = run_query(yoy_query)
        if not df_yoy.empty:
            df_yoy["Year"] = df_yoy["Year"].astype(str)
            fig_yoy = px.bar(df_yoy, x="Year", y="Value", title=f"Q{a_quarter} - Year Comparison",
                             color="Value", color_continuous_scale="Viridis", text="Value")
            fig_yoy.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig_yoy.update_layout(
                **CHART_TEMPLATE, 
                height=400, 
                showlegend=False,
                coloraxis_showscale=False  # MODIFIED: Remove right color scale
            )
            st.plotly_chart(fig_yoy, use_container_width=True, config={"displayModeBar": False})


# ============================================================
# ETL PIPELINE PAGE
# ============================================================

elif menu == "🔄 Run ETL Pipeline":

    st.markdown("<h1 style='text-align: center; color: white;'>ETL Pipeline</h1>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("""
    <p style='color: #d1d5db; font-size: 1.1rem;'>
    This will process all PhonePe Pulse JSON data and load it into a new MySQL database.
    <br><br><b>Steps:</b><br>
    1. Create a new MySQL database<br>
    2. Create all 9 tables<br>
    3. Read JSON files from each category<br>
    4. Export data as CSV files<br>
    5. Load data into MySQL tables
    </p>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("🚀 Start ETL Pipeline", type="primary"):
        status = st.container()
        with st.spinner("Running ETL Pipeline... This may take a few minutes."):
            try:
                new_db = run_etl_pipeline(status)
                st.balloons()
                st.success(f"🎉 ETL Complete! Database: {new_db}")
            except Exception as e:
                st.error(f"ETL Failed: {e}")


# ============================================================
# DATA EXPLORER PAGE
# ============================================================

elif menu == "📊 Data Explorer":

    st.markdown("<h1 style='text-align: center; color: white;'>Data Explorer</h1>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if not db_name:
        st.warning("No database found. Run ETL first.")
    else:
        table = st.selectbox("Select Table", [
            "Aggregated_transaction", "Aggregated_insurance", "Aggregated_user",
            "Map_transaction", "Map_insurance", "Map_user",
            "top_map", "top_insurance", "Top_user"
        ])
        limit = st.slider("Number of rows", 10, 1000, 100)

        df = run_query(f"SELECT * FROM `{table}` LIMIT {limit}")

        if not df.empty:
            st.markdown(f"**Showing {len(df)} rows from `{table}`**")
            st.dataframe(df, use_container_width=True)

            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"{table}_export.csv",
                mime="text/csv"
            )
        else:
            st.info("No data found in this table.")
