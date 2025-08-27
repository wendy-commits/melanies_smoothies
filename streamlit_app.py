import streamlit as st

try:
    st.write("Attempting to read secrets...")
    if "snowflake" in st.secrets:
        st.write("Snowflake section found in secrets!")
        st.write(f"Account: {st.secrets['snowflake'].get('account', 'Not Found')}")
        st.write(f"User: {st.secrets['snowflake'].get('user', 'Not Found')}")
        st.write(f"Password (first 3 chars): {str(st.secrets['snowflake'].get('password', ''))[:3]}...")
        st.success("Secrets appear to be loaded correctly.")
    else:
        st.error("Snowflake section not found in st.secrets. Please ensure your secrets.toml is correctly configured.")
        st.write("Current st.secrets keys:", list(st.secrets.keys()))
except Exception as e:
    st.error(f"An error occurred while accessing secrets: {e}")
    st.write("Please ensure your secrets are correctly configured in Streamlit Cloud.")
