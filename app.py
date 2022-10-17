import streamlit as st
from databricks import sql
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

rows = run_query("select Origin, count(*) from small_table group by Origin")

# Print results.
st.dataframe(rows)

st.line_chart(data=rows)