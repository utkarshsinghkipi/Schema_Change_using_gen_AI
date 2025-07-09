import streamlit as st
import pandas as pd
import snowflake.connector
import difflib
import google.generativeai as genai
import time
# --- Streamlit App Title ---
st.title(":robot_face: Snowflake Schema Comparator with Gemini AI")
# --- Sidebar: Snowflake Credentials ---
st.sidebar.header(":closed_lock_with_key: Snowflake Connection")
account = st.sidebar.text_input("Account", value="qi36724.ap-south-1.aws", help="e.g., xyz12345.us-east-1")
user = st.sidebar.text_input("Username", value="Utkarsh")
password = st.sidebar.text_input("Password", type="password", value="Utkarsh@123456")
role = st.sidebar.text_input("Role", value="accountadmin")
warehouse = st.sidebar.text_input("Warehouse", value="compute_wh")
# --- Sidebar: Gemini API Key ---
st.sidebar.header(":key: Gemini AI")
gemini_api_key = st.sidebar.text_input("Gemini API Key", type="password")
# --- Connect to Snowflake ---
def connect_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role,
            warehouse=warehouse
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None
conn = connect_snowflake()
if conn:
    cur = conn.cursor()
    # Fetch available databases
    cur.execute("SHOW DATABASES")
    databases = [row[1] for row in cur.fetchall() if row[1] not in ("INFORMATION_SCHEMA", "SNOWFLAKE")]
    st.markdown("### 1. Choose Source and Target")
    db_source = st.selectbox("Source Database", databases)
    db_target = st.selectbox("Target Database", databases, index=1)
    if db_source and db_target:
        def get_schemas(db):
            cur.execute(f"SHOW SCHEMAS IN DATABASE {db}")
            return [row[1] for row in cur.fetchall() if row[1] not in ("INFORMATION_SCHEMA", "SNOWFLAKE")]
        schemas1 = get_schemas(db_source)
        schemas2 = get_schemas(db_target)
        common_schemas = sorted(set(schemas1).intersection(schemas2))
        if common_schemas:
            schema = st.selectbox("Schema to Compare", common_schemas)
            object_type = st.selectbox("Object Type", ["TABLE", "VIEW", "FUNCTION", "PROCEDURE"])
            def get_objects(db, schema, obj_type):
                cur.execute(f"SHOW {obj_type}s IN {db}.{schema}")
                return [row[1] for row in cur.fetchall()]
            def get_ddl(db, schema, obj_type, obj_name):
                cur.execute(f"SELECT GET_DDL('{obj_type}', '{db}.{schema}.{obj_name}')")
                result = cur.fetchone()
                return result[0] if result else ""
            if st.button(":magnifying_glass: Compare Objects (Generate Differences Only)"):
                objs1 = get_objects(db_source, schema, object_type)
                objs2 = get_objects(db_target, schema, object_type)
                all_objs = sorted(set(objs1).union(set(objs2)))
                results = []
                for obj in all_objs:
                    ddl1 = get_ddl(db_source, schema, object_type, obj) if obj in objs1 else ""
                    ddl2 = get_ddl(db_target, schema, object_type, obj) if obj in objs2 else ""
                    if not ddl1:
                        diff = ":x: Missing in Source"
                    elif not ddl2:
                        diff = ":x: Missing in Target"
                    else:
                        d1 = ddl1.strip().splitlines()
                        d2 = ddl2.strip().splitlines()
                        diff = "\n".join(difflib.unified_diff(d1, d2)) or ":white_tick: Identical"
                    results.append({
                        "Object Name": obj,
                        "Exists in Source": obj in objs1,
                        "Exists in Target": obj in objs2,
                        "DDL Difference": diff,
                        "Source DDL": ddl1,
                        "Target DDL": ddl2,
                        "AI Sync SQL": "Click to Generate"
                    })
                df = pd.DataFrame(results)
                st.session_state['ddl_diff_df'] = df
                st.dataframe(df.drop(columns=["Source DDL", "Target DDL"]), use_container_width=True)
                st.success("Differences generated. Select a row below to generate sync SQL.")
        # Generate AI Sync SQL for selected row
        if 'ddl_diff_df' in st.session_state:
            df = st.session_state['ddl_diff_df']
            selected_object = st.selectbox("Select Object to Generate Sync SQL", df['Object Name'])
            if st.button(":cog: Generate AI Sync SQL for Selected Object"):
                row = df[df['Object Name'] == selected_object].iloc[0]
                ddl1 = row['Source DDL']
                ddl2 = row['Target DDL']
                if gemini_api_key:
                    try:
                        genai.configure(api_key=gemini_api_key)
                        model = genai.GenerativeModel("models/gemini-1.5-pro-latest")
                        prompt = f"""
You are a Snowflake SQL expert. Given the source and target DDLs below, suggest a SQL statement to make the target object identical to the source.
Source DDL:
{ddl1}
Target DDL:
{ddl2}
Provide only the SQL command to synchronize the target.
"""
                        time.sleep(3)  # To avoid quota limit burst
                        response = model.generate_content(prompt)
                        sync_sql = response.text.strip()
                        st.code(sync_sql, language="sql")
                        # Update the session dataframe with new SQL
                        df.loc[df['Object Name'] == selected_object, 'AI Sync SQL'] = sync_sql
                        st.session_state['ddl_diff_df'] = df
                    except Exception as e:
                        st.error(f"Gemini error: {e}")
            if not df.empty:
                st.download_button(":inbox_tray: Download Report with Sync SQL", data=df.drop(columns=["Source DDL", "Target DDL"]).to_csv(index=False), file_name="ddl_diff_report.csv")