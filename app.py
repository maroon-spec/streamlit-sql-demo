import streamlit as st
from databricks import sql
import pandas as pd
import os

@st.experimental_singleton
def init_connection():
    return sql.connect(
        server_hostname = st.secrets["DATABRICKS_SERVER_HOSTNAME"],
        http_path       = st.secrets["DATABRICKS_HTTP_PATH"],
        access_token    = st.secrets["DATABRICKS_TOKEN"])

conn = init_connection()


@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# Print results.
st.title("Streamlit & Databricks Demo")
st.subheader("Databricks SQL Connectorを使って、Databricksのデータにアクセスする")

full = run_query("select * from small_table limit 5")
st.table(full)

rows = run_query("select Origin, count(*) as count from small_table group by Origin")
df = pd.DataFrame(rows, columns=["Origin","count"])
st.bar_chart(data=df, x="Origin", y="count", width=0, height=0, use_container_width=True)
#st.dataframe(df)