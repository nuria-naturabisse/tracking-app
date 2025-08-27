import streamlit as st
import sqlite3
import pandas as pd

DB_FILE = "events.db"

st.title("📊 Tracking Dashboard (Demo)")

# conectar a DB
conn = sqlite3.connect(DB_FILE)
df = pd.read_sql("SELECT * FROM events ORDER BY event_time DESC", conn)
conn.close()

if df.empty:
    st.warning("No hay eventos todavía")
else:
    st.subheader("Eventos recientes")
    st.dataframe(df.head(20))

    st.subheader("Eventos por canal (utm_source)")
    channel_stats = df.groupby("utm_source").agg(
        eventos=("event_name","count"),
        revenue=("value","sum")
    ).reset_index()
    st.bar_chart(channel_stats.set_index("utm_source"))

    st.subheader("Revenue total")
    st.metric("💰 Total Revenue", f"${df['value'].sum():.2f}")
