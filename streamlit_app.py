import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import udf

# Use st.cache_resource to create the Snowflake session only once.
@st.cache_resource
def create_session():
    try:
        return get_active_session()
    except Exception:
        connection_parameters = st.secrets["snowflake"]
        return Session.builder.configs(connection_parameters).create()

try:
    st.write("Attempting to create session...")
    session = create_session()
    st.success("Session created successfully!")

    st.write("Attempting to define a simple UDF...")
    @udf
    def hello_udf(x: str) -> str:
        return "Hello " + x
    st.success("UDF defined successfully!")

    st.write("Attempting to call the UDF...")
    result = session.sql("SELECT hello_udf('World')").collect()
    st.write(f"UDF result: {result[0][0]}")
    st.success("UDF called successfully!")

except Exception as e:
    st.error(f"An error occurred: {e}")
    st.write("This indicates an issue with session creation or UDF registration.")




