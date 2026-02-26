import streamlit as st
import zipfile
import tempfile
import os
import shutil
from io import BytesIO
import pandas as pd
import os
import zipfile
import shutil

def convert_pbix_to_pbit(input_folder, output_folder,logger):
   
   if not os.path.exists(output_folder):
       os.makedirs(output_folder)
   for file in os.listdir(input_folder):
       if file.lower().endswith(".pbix"):
           pbix_path = os.path.join(input_folder, file)
           pbit_path = os.path.join(output_folder, file.replace(".pbix", ".pbit"))
           logger(f"Converting {file} → {os.path.basename(pbit_path)}")
           
           temp_folder = os.path.join(output_folder, "temp_extract")
           if os.path.exists(temp_folder):
               shutil.rmtree(temp_folder)
           os.makedirs(temp_folder)
           
           with zipfile.ZipFile(pbix_path, "r") as zip_ref:
               zip_ref.extractall(temp_folder)
         
           mashup_path = os.path.join(temp_folder, "DataMashup")
           if os.path.exists(mashup_path):
               os.remove(mashup_path)
               print("Removed DataMashup (data cleared)")
           else:
               print("No DataMashup found (maybe already a template?)")
           
           shutil.make_archive(pbit_path.replace(".pbit", ""), "zip", temp_folder)
       
           if os.path.exists(pbit_path):
               os.remove(pbit_path)
           os.rename(pbit_path.replace(".pbit", ".zip"), pbit_path)
           
           shutil.rmtree(temp_folder)
           print(f"Saved {pbit_path}")
 
def convert_pbix():
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

    st.markdown("<h2 style='color:#111111;'>🧊 PBIX to PBIT conversion</h2>", unsafe_allow_html=True)
    st.write("Upload a ZIP file containing .pbix files to convert to .pbit")

    # Initialize session state
    if "schema_logs" not in st.session_state:
        st.session_state.schema_logs = []
    if "schema_report" not in st.session_state:
        st.session_state.schema_report = None
    if "schema_files" not in st.session_state:
        st.session_state.schema_files = ("", "")

    # Uploaders
    uploaded_zip = st.file_uploader("📁 Upload ZIP file with SQLs", type="zip", key="zip_file")

    # Clear report if files are removed
    if not uploaded_zip:
        st.session_state.schema_report = None
        st.session_state.schema_logs=[]

    log_placeholder = st.empty()

    def schema_logger(msg: str):
        st.session_state.schema_logs.append(msg)
        log_placeholder.text_area("📜 Conversion Logs", "\n".join(st.session_state.schema_logs), height=300)

    # Run transformation
    if uploaded_zip:
        if st.button("🚀 Run Transformation"):
            st.session_state.schema_logs = []  # Clear old logs
            st.session_state.schema_files = (uploaded_zip.name)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_path = os.path.join(temp_dir, uploaded_zip.name)

                # Save uploaded files
                with open(zip_path, "wb") as f:
                    f.write(uploaded_zip.read())

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
                        if file.endswith('.pbix'):
                            dest_path = os.path.join(source_sql_dir, file)
                            if os.path.abspath(full_path) != os.path.abspath(dest_path):
                                shutil.copy(full_path, dest_path)

                schema_logger("🔄 Processing pbix files...")
                convert_pbix_to_pbit(
                    source_sql_dir,
                    output_sql_dir, logger=schema_logger
                )
                

                schema_logger("📁 Zipping transformed pbit files...")
                zipped_output = zip_folder(output_sql_dir)
                st.session_state.schema_report = zipped_output

                st.success("✅ Transformation completed successfully!")

                log_placeholder.empty()

    # Show download button if report exists

    if st.session_state.schema_logs and st.session_state.schema_report and uploaded_zip:
        st.text_area("📜 Transformation Logs", "\n".join(st.session_state.schema_logs), height=300, key="conversion_logs_static")




    if st.session_state.schema_report and uploaded_zip :
        zip_name = st.session_state.schema_files
        
        st.download_button(
            label="📄 Download Transformed PBIT files",
            data=st.session_state.schema_report,
            # data = report_bytes,
            file_name=f"transformed_sqls_{zip_name.split('.')[0]}.zip",
            mime="application/zip",
            key="download_transformed_sqls"
        )
