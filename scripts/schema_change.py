import streamlit as st
import zipfile
import tempfile
import os
import shutil
from io import BytesIO
import pandas as pd
import re
import time

# Dummy output folder path (update this to your actual output folder)
OUTPUT_FOLDER = "Output_files"

def schema_transformer():
    st.markdown("<h2 style='color:#111111;'>🧪 Schema Transformer</h2>", unsafe_allow_html=True)
    st.write("Upload a .pbit file and download the corresponding transformed .pbit file.")

    # Initialize session state
    if "transformed_file_ready" not in st.session_state:
        st.session_state.transformed_file_ready = False
    if "transformed_file_path" not in st.session_state:
        st.session_state.transformed_file_path = ""
    if "file_available" not in st.session_state:
        st.session_state.file_available = False

    
    col1, col2 = st.columns(2)
    with col1:
        uploaded_file = st.file_uploader("📁 Upload PBIT file", type="pbit", key="pbit_file")
    with col2:
        uploaded_csv = st.file_uploader("📁 Upload CSV mapping file", type="csv", key="csv_file")

    if st.button("🚀 Run Schema Transformer"):
        if not uploaded_file:
            st.write("Please upload the file before running")
        if uploaded_file:
            st.session_state.file_available = True
            input_filename = uploaded_file.name
            output_file_path = os.path.join(OUTPUT_FOLDER, input_filename)

            # Button to trigger transformation
            
            with st.spinner("🔄 Running transformation... Please wait"):
                time.sleep(5)  # Simulate processing delay


            if os.path.exists(output_file_path):
                st.session_state.transformed_file_ready = True
                st.session_state.transformed_file_path = output_file_path
                
            else:
                st.session_state.transformed_file_ready = False
                st.session_state.transformed_file_path = ""
                st.error("❌ Unable to Transform the file. Please check")

    if not uploaded_file:
        st.session_state.file_available = False

    # Show download button if file is ready
    if st.session_state.transformed_file_ready and os.path.exists(st.session_state.transformed_file_path) and st.session_state.file_available:
        st.success("✅ Schema changes completed. File ready for download!")
        with open(st.session_state.transformed_file_path, "rb") as f:
            st.download_button(
                label="📄 Download Transformed File",
                data=f,
                file_name=os.path.basename(st.session_state.transformed_file_path),
                mime="application/octet-stream"
            )