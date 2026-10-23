import os

import streamlit as st
from sqlalchemy import create_engine

DB_URL = (
    f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ.get('DB_HOST', 'postgres')}:{os.environ.get('DB_PORT', '5432')}"
    f"/{os.environ.get('DB_NAME', 'airports')}"
)


@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


st.set_page_config(page_title="Global Airports", layout="wide")
st.title("Global Airports")
st.info("Placeholder - dashboard coming soon.")
