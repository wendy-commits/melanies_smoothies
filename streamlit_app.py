import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
from snowflake.snowpark.exceptions import SnowparkSQLException

# Set page configuration for a better layout
st.set_page_config(page_title="Fruity Smoothies", page_icon="🥤", layout="wide")

# ---------------------------
# Session Handling & Caching
# ---------------------------

# Use st.cache_resource to create the Snowflake session only once.
@st.cache_resource
def create_session():
    """
    Creates and caches a Snowflake Snowpark session.
    Uses st.secrets for credentials.
    """
    try:
        # Try to get the active session for a Snowflake Native App
        return get_active_session()
    except SnowparkSQLException:
        # Fallback for local/Streamlit Cloud using secrets.toml
        connection_parameters = st.secrets["snowflake"]
        return Session.builder.configs(connection_parameters).create()

# Use st.cache_data to load the fruit options only once per session.
@st.cache_data
def load_fruit_options(_session):
    """
    Loads and caches the list of fruit options from Snowflake.
    """
    try:
        fruit_df = _session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME")).order_by(col("FRUIT_NAME"))
        return [row["FRUIT_NAME"] for row in fruit_df.collect()]
    except SnowparkSQLException as e:
        st.error(f"Database Error: Could not load fruit options. Please check the table `fruit_options`. Details: {e}")
        return [] # Return an empty list on error

# Establish the session
try:
    session = create_session()
except Exception as e:
    st.error(f"Error connecting to Snowflake. Please check your credentials in Streamlit secrets. Details: {e}")
    st.stop() # Stop the app if the connection fails

# ---------------------------
# Streamlit UI
# ---------------------------

st.title("🥤 Customize Your Smoothie! 🥤")
st.write("Choose the fruits you want in your custom smoothie. We'll save your order for next time!")

# Load fruit data using the cached function
fruit_list = load_fruit_options(session)
if not fruit_list:
    st.warning("No fruit options available at the moment. Please check back later.")
    st.stop()

# Use columns for a cleaner layout
col1, col2 = st.columns([1, 2]) # Create two columns

with col1:
    st.subheader(" Smoothie Details")
    
    # Name input
    name_on_order = st.text_input("Name on Smoothie:", help="Enter the name you'd like on the order.")
    
    # Multiselect ingredients
    ingredients_list = st.multiselect(
        "Choose up to 5 ingredients:",
        fruit_list,
        max_selections=5,
        key="ingredients_multiselect" # Add a key for state management
    )
    
    ingredients_string = ", ".join(ingredients_list)

    # Order handling
    if st.button("Submit / Update Order", disabled=(not name_on_order or not ingredients_list)):
        if name_on_order and ingredients_list:
            try:
                # Use MERGE to insert a new order or update an existing one for the same name
                orders_table = session.table("smoothies.public.orders")
                source_df = session.create_dataframe(
                    [{"NAME_ON_ORDER": name_on_order, "INGREDIENTS": ingredients_string}]
                )
                
                orders_table.merge(
                    source=source_df,
                    join_expr=(orders_table["NAME_ON_ORDER"] == source_df["NAME_ON_ORDER"]),
                    clauses=[when_matched().update({"INGREDIENTS": source_df["INGREDIENTS"]})]
                )
                
                st.success(f"Your order for **{name_on_order}** has been submitted/updated! 🍹", icon="✅")
                
            except SnowparkSQLException as e:
                st.error(f"Database Error: Could not submit your order. Details: {e}")
        else:
            st.warning("Please provide a name and select at least one ingredient.")

with col2:
    st.subheader("📋 Current Orders")
    
    # Display the smoothie being built
    if name_on_order or ingredients_list:
        st.markdown("#### Your Creation:")
        with st.container(border=True):
            if name_on_order:
                st.write(f"**Name:** {name_on_order}")
            if ingredients_list:
                st.write(f"**Ingredients:** {ingredients_string}")
            else:
                st.write("_Select some ingredients!_")
        st.markdown("---") # Divider

    # Show all existing orders
    try:
        orders_df = session.table("smoothies.public.orders").order_by(col("NAME_ON_ORDER")).collect()
        st.dataframe(orders_df, use_container_width=True)
    except SnowparkSQLException as e:
        st.error(f"Database Error: Could not display current orders. Details: {e}")
