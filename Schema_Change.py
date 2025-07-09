import streamlit as st
import pandas as pd
import snowflake.connector
import difflib
import openai
# --- Streamlit App Title ---
st.title(":robot_face: Snowflake Schema Comparator with OpenAI")
# --- Sidebar: Snowflake Credentials ---
st.sidebar.header(":closed_lock_with_key: Snowflake Connection")
account = st.sidebar.text_input("Account", value="qi36724.ap-south-1.aws", help="e.g., xyz12345.us-east-1")
user = st.sidebar.text_input("Username", value="Utkarsh")
password = st.sidebar.text_input("Password", type="password", value="Utkarsh@123456")
role = st.sidebar.text_input("Role", value="accountadmin")
warehouse = st.sidebar.text_input("Warehouse", value="compute_wh")
# --- Sidebar: OpenAI API Key ---
st.sidebar.header(":key: OpenAI API")
openai.api_key = st.sidebar.text_input("OpenAI API Key", type="password")
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
            if st.button(":magnifying_glass: Compare & Generate Sync SQL"):
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
                    # Generate sync SQL via GPT
                    prompt = f"""
You are a Snowflake SQL expert. Given the source and target DDLs below, suggest a SQL statement to make the target object identical to the source.
Source DDL:
{ddl1}
Target DDL:
{ddl2}
Provide only the SQL command to synchronize the target.
"""
                    ai_sql = ""
                    if openai.api_key:
                        try:
                            res = openai.ChatCompletion.create(
                                model="gpt-3.5-turbo",
                                messages=[{"role": "user", "content": prompt}],
                                temperature=0.3,
                                max_tokens=500
                            )
                            ai_sql = res['choices'][0]['message']['content'].strip()
                        except Exception as e:
                            ai_sql = f":warning: OpenAI error: {e}"
                    results.append({
                        "Object Name": obj,
                        "Exists in Source": obj in objs1,
                        "Exists in Target": obj in objs2,
                        "DDL Difference": diff,
                        "AI Sync SQL": ai_sql
                    })
                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
                st.download_button(":inbox_tray: Download Report", data=df.to_csv(index=False), file_name="ddl_diff_report.csv")
        else:
            st.warning("No common schemas found between selected databases.")