import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import os

# ---------------------------
# Session handling
# ---------------------------
def create_session():
    try:
        # Try Native App (inside Snowflake)
        return get_active_session()
    except Exception:
        # Fallback for local / Streamlit Cloud
        connection_parameters = {
            "account": os.getenv("RGDDDWQ-PRB75287"),
            "user": os.getenv("wendy87226"),
            "password": os.getenv("Snowflake-2025"),
            "role": os.getenv("SYSADMIN"),
            "warehouse": os.getenv("COMPUTE_WH"),
            "database": "SMOOTHIES",
            "schema": "PUBLIC"
        }
        return Session.builder.configs(connection_parameters).create()

session = create_session()

# ---------------------------
# Streamlit UI
# ---------------------------
st.title("🥤 Customize your Smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie!")

# Name input
name_on_order = st.text_input("Name on Smoothie")
if name_on_order:
    st.write("The name of your smoothie will be:", name_on_order)

# Load fruits
fruit_df = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME"))
fruit_list = [row["FRUIT_NAME"] for row in fruit_df.collect()]

# Multiselect ingredients
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_list,
    max_selections=5
)

# Order handling
if ingredients_list:
    ingredients_string = ", ".join(ingredients_list)
    st.write("✅ Your smoothie will include:", ingredients_string)

    if st.button("Submit Order"):
        # Insert into orders table
        session.table("smoothies.public.orders").insert(
            {"INGREDIENTS": ingredients_string, "NAME_ON_ORDER": name_on_order}
        )
        st.success(f"Your Smoothie is ordered! 🍹 {name_on_order}", icon="✅")

        # Show all existing orders
        orders_df = session.table("smoothies.public.orders").select("NAME_ON_ORDER", "INGREDIENTS").collect()
        st.subheader("📋 Current Orders")
        st.dataframe(orders_df, use_container_width=True)


    
