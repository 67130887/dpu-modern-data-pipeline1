import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

# -------------------- Config --------------------
st.set_page_config(
    page_title="Bangkok AQI Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------------- Sidebar --------------------
st.sidebar.header("AQI Dashboard Settings")

# -------------------- Connect PostgreSQL --------------------
@st.cache_resource
def get_data():
    conn = psycopg2.connect(
        host="db",          # ชื่อ service ใน docker-compose
        database="postgres",
        user="postgres",
        password="postgres",
        port=5432
    )
    sql = "SELECT * FROM bangkok_aqi ORDER BY timestamp DESC"
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

df = get_data()

# -------------------- Dashboard --------------------
st.title("Bangkok Air Quality Index (AQI) Dashboard")
st.markdown("""<small>Data Source: AirVisual API → PostgreSQL</small>""", unsafe_allow_html=True)

# -------------------- KPI --------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest AQI", df.iloc[0]['aqi'])
col2.metric("Latest Temp (°C)", df.iloc[0]['temperature'])
col3.metric("Latest Humidity (%)", df.iloc[0]['humidity'])
col4.metric("Latest PM2.5", df.iloc[0]['pm2_5'] if df.iloc[0]['pm2_5'] else "N/A")

st.markdown("---")

# -------------------- Line Chart --------------------
st.subheader("Trend: AQI / Temperature / Humidity")
st.line_chart(df.set_index("timestamp")[['aqi', 'temperature', 'humidity']])

# -------------------- Optional --------------------
st.subheader("Raw Data")
st.dataframe(df)

st.success("Dashboard Loaded Successfully")
