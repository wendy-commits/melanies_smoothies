import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType

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

    st.write("Attempting to define and register a simple UDF...")

    # Define the Python function
    def hello_python_udf(x: str) -> str:
        return "Hello " + x

    # Register the UDF explicitly
    # The name 'hello_udf_registered' will be the SQL function name
    hello_udf_registered = session.udf.register(
        func=hello_python_udf,
        name="hello_udf_registered",
        is_permanent=False, # Set to True for persistent UDFs
        stage_location="@~/udfs", # A temporary stage for non-permanent UDFs
        replace=True,
        return_type=StringType(),
        input_types=[StringType()]
    )
    st.success("UDF defined and registered successfully!")

    st.write("Attempting to call the registered UDF...")
    result = session.sql("SELECT hello_udf_registered(\'World\')").collect()
    st.write(f"UDF result: {result[0][0]}")
    st.success("UDF called successfully!")

except Exception as e:
    st.error(f"An error occurred: {e}")
    st.write("This indicates an issue with session creation or UDF registration.")




live
