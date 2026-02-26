import os
import subprocess
from datetime import datetime

import streamlit as st

import tempfile, os
import io
import contextlib
from scripts import self_healing_script


def self_healing(sf_session):
    st.markdown("<h2 style='color:#111111;'>🧠 Self-Healing Code Executor</h2>", unsafe_allow_html=True)
    # st.write("Upload a SQL, BTEQ, or Python file for AI-assisted self-healing and automatic correction using Snowflake Cortex.")

    uploaded_file = st.file_uploader(
        "📁 Upload script file for AI assistant self healing",
        type=["sql", "bteq", "py", "txt"],
        key="heal_file"
    )
    
    st.markdown("<div style='text-align: center;'><h2><strong>OR</strong></h2></div>",    unsafe_allow_html=True)

    github_link = st.text_input(label="Enter your GitHub repository link:",value="https://github.com/ey-org/", help="Please paste the full URL of your GitHub repository.")

    # Initialize session state flags
    if "healing_done" not in st.session_state:
        st.session_state.healing_done = False
    if "log_file_path" not in st.session_state:
        st.session_state.log_file_path = None

    if uploaded_file is not None:
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, uploaded_file.name)

        with open(temp_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        st.success(f"✅ Uploaded file: `{uploaded_file.name}`")

        if st.button("🚀 Run Self-Healing Code", type="primary"):
            st.session_state.healing_done = False  # Reset for new run
            st.markdown("### 🧩 Execution Logs")
            log_placeholder = st.empty()
            logs = ""
            log_dir = "logs"

            os.makedirs(log_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_path = os.path.join(log_dir, f"log_{timestamp}.txt")
            st.session_state.log_file_path = log_file_path  # store path for persistence

            class StreamlitLogger(io.StringIO):
                def write(self, txt):
                    nonlocal logs
                    logs += txt
                    # Custom small scrollable log box
                    log_html = f"""
                    <div style="
                        background-color:#f5f5f5;
                        color:#000000;
                        font-family: monospace;
                        white-space: pre-wrap;
                        padding: 10px;
                        border-radius: 10px;
                        height: 250px;
                        overflow-y: auto;
                        font-size: 13px;
                    ">{logs}</div>
                    """
                    log_placeholder.markdown(log_html, unsafe_allow_html=True)

                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(txt)

                    return super().write(txt)

            with contextlib.redirect_stdout(StreamlitLogger()), contextlib.redirect_stderr(StreamlitLogger()):
                try:
                    with st.spinner("Running self-healing logic... please wait ⏳"):
                        result = self_healing_script.run_self_healing_from_ui(temp_file_path,sf_session)
                        st.success("✅ Self-healing process completed successfully!")

                except Exception as e:
                    st.error(f"❌ Process failed: {e}")

            st.session_state.healing_done = True  # mark process as done
    if not uploaded_file:
        st.session_state.healing_done = False
    # ✅ Keep showing log download after rerun
    if st.session_state.healing_done and st.session_state.log_file_path and os.path.exists(st.session_state.log_file_path):
        st.success(f"✅ Execution completed. Log saved to `{st.session_state.log_file_path}`")
        with open(st.session_state.log_file_path, "rb") as f:
            st.download_button(
                "⬇️ Download Log File",
                f,
                file_name=os.path.basename(st.session_state.log_file_path),
                mime="text/plain"
            )




























# import os
# import subprocess
# from datetime import datetime

# import streamlit as st

# import tempfile, os
# import io
# import contextlib
# import self_healing_script 
# import json

# def self_healing():

#     st.markdown("<h2 style='color:#4CAF50;'>🧠 Self-Healing Code Executor</h2>", unsafe_allow_html=True)
#     st.write("Upload a SQL, BTEQ, or Python file for AI-assisted self-healing and automatic correction using Snowflake Cortex.")

#     uploaded_file = st.file_uploader(
#         "📁 Upload a SQL or Script file",
#         type=["sql", "bteq", "py", "txt"],
#         key="heal_file"
#     )

#     if uploaded_file is not None:
#         temp_dir = tempfile.mkdtemp()
#         temp_file_path = os.path.join(temp_dir, uploaded_file.name)

#         with open(temp_file_path, "wb") as f:
#             f.write(uploaded_file.getbuffer())

#         st.success(f"✅ Uploaded file: `{uploaded_file.name}`")

#         if st.button("🚀 Run Self-Healing Code", type="primary"):
#             st.markdown("### 🧩 Execution Logs")
#             log_placeholder = st.empty()
#             logs = ""
#             log_dir = "logs"

#             os.makedirs(log_dir, exist_ok=True)
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             log_file_path = os.path.join(log_dir, f"log_{timestamp}.txt")

#             class StreamlitLogger(io.StringIO):
#                 def write(self, txt):
#                     nonlocal logs
#                     logs += txt
#                     # Custom small scrollable log box
#                     log_html = f"""
#                     <div style="
#                         background-color:#f5f5f5;
#                         color:#000000;
#                         font-family: monospace;
#                         white-space: pre-wrap;
#                         padding: 10px;
#                         border-radius: 10px;
#                         height: 250px;
#                         overflow-y: auto;
#                         font-size: 13px;
#                     ">{logs}</div>
#                     """
#                     log_placeholder.markdown(log_html, unsafe_allow_html=True)

#                     with open(log_file_path, "a", encoding="utf-8") as f:
#                         f.write(txt)

#                     return super().write(txt)

#             with contextlib.redirect_stdout(StreamlitLogger()), contextlib.redirect_stderr(StreamlitLogger()):
#                 try:
#                     with st.spinner("Running self-healing logic... please wait ⏳"):
#                         result = self_healing_script.run_self_healing_from_ui(temp_file_path)
#                         st.success("✅ Self-healing process completed successfully!")
#                         st.subheader("🧾 Final Result:")

#                         try:
#                             if isinstance(result, str):
#                                 try:
#                                     parsed = json.loads(result)
#                                     st.json(parsed)
#                                 except json.JSONDecodeError:
#                                     st.code(result, language="json")
#                             elif isinstance(result, dict):
#                                 st.json(result)
#                             else:
#                                 st.code(str(result), language="bash")
#                         except Exception as e:
#                             st.error(f"⚠️ Could not render result: {e}")
#                             st.code(str(result))
#                 except Exception as e:
#                     st.error(f"❌ Process failed: {e}")
            
#             if os.path.exists(log_file_path):
#                 st.success(f"✅ Execution completed. Log saved to `{log_file_path}`")
#                 with open(log_file_path, "rb") as f:
#                     st.download_button(
#                         "⬇️ Download Log File",
#                         f,
#                         file_name=os.path.basename(log_file_path),
#                         mime="text/plain"
#                     )