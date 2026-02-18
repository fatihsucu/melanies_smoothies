import streamlit as st
from snowflake.snowpark.functions import col, when_matched  # <- required import

# -----------------------------
# Setup
# -----------------------------
cnx = st.connection('snowflake')
session = cnx.session()

st.set_page_config(page_title="Smoothies", page_icon="🥤", layout="wide")
page = st.sidebar.radio("Choose a page", ["Ordering", "Pending Orders"])

# -----------------------------
# Ordering Page
# -----------------------------
if page == "Ordering":
    st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")
    st.title("My Parents new healthy dinner")
    st.write("Choose the fruits you want in your custom Smoothie!")

    name_on_order = st.text_input("Name on Smoothie Order")

    fruit_sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME"))
    fruit_pd_df = fruit_sp_df.to_pandas()
    fruit_names = fruit_pd_df["FRUIT_NAME"].tolist()

    ingredients_list = st.multiselect(
        "Choose up to 5 ingredients",
        fruit_names,
        max_selections=5
    )

    if ingredients_list:
        ingredient_string = ", ".join(ingredients_list)
        st.write("Ingredients:", ingredient_string)

    can_submit = bool(name_on_order) and bool(ingredients_list)
    submit = st.button("Submit Order", disabled=not can_submit)

    if submit:
        ingredient_string = ", ".join(ingredients_list)

        # ORDER_UID, ORDER_FILLED, ORDER_TS are defaults
        session.sql(
            """
            INSERT INTO SMOOTHIES.PUBLIC.ORDERS (NAME_ON_ORDER, INGREDIENTS)
            VALUES (?, ?)
            """,
            params=[name_on_order, ingredient_string],
        ).collect()

        st.success(f"Your Smoothie is ordered for {name_on_order}!", icon="✅")

# -----------------------------
# Pending Orders Page
# -----------------------------
else:
    st.title("🥤 Pending Smoothie Orders")
    st.write("Kitchen view: check ORDER_FILLED and save.")
    
    session = get_active_session()
    
    pending_sp_df = (
        session.table("SMOOTHIES.PUBLIC.ORDERS")
        .filter(col("ORDER_FILLED") == False)
        .select(col("ORDER_UID"), col("ORDER_FILLED"), col("NAME_ON_ORDER"), col("INGREDIENTS"), col("ORDER_TS"))
        .sort(col("ORDER_TS").desc())
    )
    
    pending_pd_df = pending_sp_df.to_pandas()
    
    # ✅ If no pending orders: show message and DO NOT render table
    if pending_pd_df.empty:
        st.success("No pending orders 🎉")
        st.stop()
    
    st.caption(f"Pending orders: {len(pending_pd_df)}")
    
    editable_df = st.data_editor(
        pending_pd_df,
        use_container_width=True,
        hide_index=True,
        disabled=["ORDER_UID", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_TS"],
    )
    
    changed = editable_df["ORDER_FILLED"] != pending_pd_df["ORDER_FILLED"]
    changed_df = editable_df.loc[changed, ["ORDER_UID", "ORDER_FILLED"]].copy()
    
    save = st.button("Save Changes ✅", disabled=changed_df.empty)
    
    if save:
        # ✅ Only try merge if something actually changed
        if changed_df.empty:
            st.info("No changes to save.")
            st.stop()
    
        try:
            og_dataset = session.table("SMOOTHIES.PUBLIC.ORDERS")
            edited_dataset = session.create_dataframe(changed_df)
    
            og_dataset.merge(
                edited_dataset,
                (og_dataset["ORDER_UID"] == edited_dataset["ORDER_UID"]),
                [when_matched().update({"ORDER_FILLED": edited_dataset["ORDER_FILLED"]})],
            )
    
            # ✅ Show success ONLY after merge executes
            st.success(f"Saved {len(changed_df)} change(s) to Snowflake!", icon="✅")
            st.rerun()
    
        except Exception as e:
            st.error(f"Save failed: {e}")
