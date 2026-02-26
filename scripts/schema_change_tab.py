import streamlit as st
import zipfile
import tempfile
import os
import shutil
from io import BytesIO
import pandas as pd
from scripts.schema_conversion_teradata_to_snowflake import process_sql_with_pandas_replace
import re


def schema_change_tab():
    def zip_folder(folder_path):
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)
        zip_buffer.seek(0)
        return zip_buffer

    st.markdown("<h2 style='color:#111111;'>🧬 Source To Target Schema Mapping</h2>", unsafe_allow_html=True)
    st.write("Upload a ZIP file containing SQL files and a separate CSV mapping file.")

    # Initialize session state
    if "schema_logs" not in st.session_state:
        st.session_state.schema_logs = []
    if "schema_report" not in st.session_state:
        st.session_state.schema_report = None
    if "schema_files" not in st.session_state:
        st.session_state.schema_files = ("", "")
    if "schema_df" not in st.session_state:
        st.session_state.schema_df = None
    
        
    # Uploaders
    col1, col2 = st.columns(2)
    with col1:
        uploaded_zip = st.file_uploader("📁 Upload ZIP file with SQLs", type="zip", key="zip_file")
    with col2:
        uploaded_csv = st.file_uploader("📁 Upload CSV mapping file", type="csv", key="csv_file")

    st.markdown("<div style='text-align: center;'><h2><strong>OR</strong></h2></div>",    unsafe_allow_html=True)

    github_link = st.text_input(label="Enter your GitHub repository link:",value="https://github.com/ey-org/", help="Please paste the full URL of your GitHub repository.",key="github_input_box")

    # Clear report if files are removed
    if not uploaded_zip or not uploaded_csv:
        st.session_state.schema_report = None
        st.session_state.schema_logs=[]


    log_placeholder = st.empty()
    
    def schema_logger(msg: str):
        st.session_state.schema_logs.append(msg)
    
    

    # Logger

    
    if st.session_state.schema_logs and not st.session_state.schema_report and uploaded_zip and uploaded_csv:
        st.text_area("📜 Conversion Logs", "\n".join(st.session_state.schema_logs), height=300, key="conversion_logs_static_schema")

    

    # Run transformation
    if uploaded_zip and uploaded_csv:
        if st.button("🚀 Run Transformation"):
            st.session_state.schema_logs = []  # Clear old logs
            st.session_state.schema_files = (uploaded_zip.name, uploaded_csv.name)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_path = os.path.join(temp_dir, uploaded_zip.name)
                csv_path = os.path.join(temp_dir, uploaded_csv.name)

                # Save uploaded files
                with open(zip_path, "wb") as f:
                    f.write(uploaded_zip.read())
                with open(csv_path, "wb") as f:
                    f.write(uploaded_csv.read())

                schema_logger("📦 Extracting ZIP contents...")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)

                source_sql_dir = os.path.join(temp_dir, "source_sqls")
                output_sql_dir = os.path.join(temp_dir, "transformed_sqls")
                os.makedirs(source_sql_dir, exist_ok=True)
                os.makedirs(output_sql_dir, exist_ok=True)

                # Move SQL files to source_sql_dir
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        full_path = os.path.join(root, file)
                        if file.endswith('.sql'):
                            dest_path = os.path.join(source_sql_dir, file)
                            if os.path.abspath(full_path) != os.path.abspath(dest_path):
                                shutil.copy(full_path, dest_path)

                schema_logger("🔄 Processing SQL files with CSV mapping...")
                process_sql_with_pandas_replace(
                    csv_file_path=csv_path,
                    sql_file_path=source_sql_dir,
                    output_dir=output_sql_dir,
                    logg=schema_logger
                )
                
                summary_json_file_name = "summary.json"
                if os.path.exists(summary_json_file_name):
                    with open(summary_json_file_name, "r", encoding="utf-8") as json_file:
                        # summary_data = json.load(json_file)
                        summary_data = json_file.read()
                        lines = [line.strip() for line in summary_data.strip().split('\n') if line.strip()]

                        rows = []
                        current = {}
                        for line in lines:
                            filename_match = re.search(r'Name of the filename\s*:\s*([^\s]+)', line)
                            if filename_match:
                                if current:
                                    rows.append(current)
                # Start new file's data
                                # current = {"Filename": filename_match.group(1)}
                                
                                raw_filename = filename_match.group(1).strip().rstrip(',').rstrip('"')
                                current = {"Filename": raw_filename}

                                continue
                            changes_expected_match = re.search(r'No of places changes expected\s*:\s*(\d+)', line)
                            if changes_expected_match:
                                current["Changes Expected"] = int(changes_expected_match.group(1))
                                continue
                            changes_implemented_match = re.search(r'No of places changes implemented\s*:\s*(\d+)', line)
                            if changes_implemented_match:
                                current["Changes Implemented"] = int(changes_implemented_match.group(1))
                                continue
                            sp_db_change_match = re.search(r'SP DB Change\s*:\s*(YES|NO)', line)
                            if sp_db_change_match:
                                current["SP DB Change"] = sp_db_change_match.group(1)
                                continue
        # Add last file's data
                        if current:
                            rows.append(current)
                        df = pd.DataFrame(rows)
                        st.session_state.schema_df = df

                schema_logger("📁 Zipping transformed SQLs...")
                zipped_output = zip_folder(output_sql_dir)
                st.session_state.schema_report = zipped_output

                st.success("✅ Transformation completed successfully!")

    # Show download button if report exists

    if st.session_state.schema_logs and st.session_state.schema_report and uploaded_zip and uploaded_csv:
        st.text_area("📜 Transformation Logs", "\n".join(st.session_state.schema_logs), height=300, key="conversion_logs_static")
    
    
    if "schema_df" in st.session_state and st.session_state.schema_report:
        # st.dataframe(st.session_state.schema_df)
        
        st.write("🧬 Transformed Schema summary")
        st.dataframe(
                st.session_state.schema_df,
                use_container_width=True
            )


    
    if st.session_state.schema_report and uploaded_zip and uploaded_csv:
        zip_name, csv_name = st.session_state.schema_files
        
        st.download_button(
            label="📄 Download Transformed SQLs",
            data=st.session_state.schema_report,
            # data = report_bytes,
            file_name=f"transformed_sqls_{zip_name.split('.')[0]}_{csv_name.split('.')[0]}.zip",
            mime="application/zip",
            key="download_transformed_sqls"
        )

