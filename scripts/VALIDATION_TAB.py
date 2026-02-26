import streamlit as st
import tempfile
import os
import subprocess
from datetime import datetime
import io
from io import BytesIO
import streamlit as st
from datetime import datetime
from scripts.validation2 import compare_analysis, analyze_file

def validation_tab():
    st.markdown("<h2 style='color:#111111;'>✅ Script Validation</h2>", unsafe_allow_html=True)
    st.write("Upload your Teradata and converted Snowflake SQL files for comparison and validation.")

    # Initialize session state variables
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "report" not in st.session_state:
        st.session_state.report = None
    if "file_names" not in st.session_state:
        st.session_state.file_names = ("", "")

    # --- Layout: Two side-by-side uploaders ---
    col1, col2 = st.columns(2)

    with col1:
        td_file = st.file_uploader(
            "📁 Upload Teradata SQL file",
            type=["sql", "ddl"],
            key="td_file"
        )

    with col2:
        sf_file = st.file_uploader(
            "📁 Upload Snowflake SQL file",
            type=["sql", "ddl"],
            key="sf_file"
        )

    # --- Logger function ---
    st.markdown("<div style='text-align: center;'><h2><strong>OR</strong></h2></div>",    unsafe_allow_html=True)

    github_link = st.text_input(label="Enter your GitHub repository link:",value="https://github.com/ey-org/", help="Please paste the full URL of your GitHub repository.",key="github_input_box_validation")

    # --- Logs placeholder ---
    

    # --- Run validation button ---
    if td_file and sf_file:
        if st.button("🚀 Run Validation"):
            log_placeholder = st.empty()

            def streamlit_logger(msg: str):
                st.session_state.logs.append(msg)

            temp_dir = tempfile.mkdtemp()
            td_path = os.path.join(temp_dir, td_file.name)
            sf_path = os.path.join(temp_dir, sf_file.name)

            with open(td_path, "wb") as f1:
                f1.write(td_file.getbuffer())
            with open(sf_path, "wb") as f2:
                f2.write(sf_file.getbuffer())

            st.session_state.logs = []  # clear old logs
            st.session_state.file_names = (td_file.name, sf_file.name)

            streamlit_logger(f"▶️ Analyzing {td_file.name}")
            analysis1 = analyze_file(td_path)

            streamlit_logger(f"▶️ Analyzing {sf_file.name}")
            analysis2 = analyze_file(sf_path)

            streamlit_logger("🔍 Comparing analyses...")
            report = compare_analysis(analysis1, analysis2, logger=streamlit_logger)

            st.session_state.report = report  # store in session state

            st.success("✅ Validation completed successfully!")
    
    if st.session_state.report and td_file and sf_file:
        st.text_area("📜 Transformation Logs", "\n".join(st.session_state.logs), height=300)

    
    if not td_file or not sf_file:
        st.session_state.report = None



    # --- Show download button if report exists ---
    if st.session_state.report and td_file and sf_file:
        td_name, sf_name = st.session_state.file_names
        report_bytes = io.BytesIO(st.session_state.report.encode("utf-8"))
        st.download_button(
            label="📄 Download Validation Report",
            data=report_bytes,
            file_name=f"validation_report_{sf_name.split('.')[0]}.txt",
            mime="text/plain",
            key="download_report"
        )
