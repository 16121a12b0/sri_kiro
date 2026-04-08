import streamlit as st
import pandas as pd
from snowflake_client import fetch_openflow_jobs, fetch_job_summary

st.set_page_config(page_title="OpenFlow Job Monitor", layout="wide")
st.title("OpenFlow Job Monitor")

# --- Sidebar controls ---
st.sidebar.header("Filters")

STATUS_OPTIONS = ["RUNNING", "SUCCESS", "FAILED", "PENDING", "CANCELLED"]
selected_statuses = st.sidebar.multiselect(
    "Filter by Status",
    options=STATUS_OPTIONS,
    default=[],
)

auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
if auto_refresh:
    import time
    st.sidebar.caption("Page will refresh every 30 seconds.")

refresh = st.sidebar.button("Refresh Now")

# --- Load data ---
@st.cache_data(ttl=30)
def load_jobs(statuses):
    return fetch_openflow_jobs(status_filter=statuses if statuses else None)

@st.cache_data(ttl=30)
def load_summary():
    return fetch_job_summary()

try:
    if refresh:
        st.cache_data.clear()

    summary_df = load_summary()
    jobs_df = load_jobs(tuple(selected_statuses))

    # --- Summary metrics ---
    st.subheader("Summary")
    cols = st.columns(len(summary_df) if not summary_df.empty else 1)
    for i, row in summary_df.iterrows():
        cols[i % len(cols)].metric(label=row["STATUS"], value=int(row["JOB_COUNT"]))

    st.divider()

    # --- Status color map ---
    STATUS_COLORS = {
        "RUNNING": "🔵",
        "SUCCESS": "🟢",
        "FAILED": "🔴",
        "PENDING": "🟡",
        "CANCELLED": "⚫",
    }

    # --- Jobs table ---
    st.subheader("Job Executions")

    if jobs_df.empty:
        st.info("No jobs found for the selected filters.")
    else:
        # Add emoji status indicator
        jobs_df["STATUS"] = jobs_df["STATUS"].apply(
            lambda s: f"{STATUS_COLORS.get(s, '')} {s}"
        )

        st.dataframe(
            jobs_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "JOB_ID": st.column_config.TextColumn("Job ID"),
                "JOB_NAME": st.column_config.TextColumn("Job Name"),
                "STATUS": st.column_config.TextColumn("Status"),
                "START_TIME": st.column_config.DatetimeColumn("Start Time"),
                "END_TIME": st.column_config.DatetimeColumn("End Time"),
                "DURATION_SECONDS": st.column_config.NumberColumn("Duration (s)"),
                "ERROR_MESSAGE": st.column_config.TextColumn("Error"),
            },
        )

        # --- Failed jobs detail ---
        failed = jobs_df[jobs_df["STATUS"].str.contains("FAILED")]
        if not failed.empty:
            with st.expander(f"Failed Jobs ({len(failed)})", expanded=True):
                st.dataframe(failed[["JOB_ID", "JOB_NAME", "START_TIME", "ERROR_MESSAGE"]],
                             use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Failed to connect to Snowflake: {e}")
    st.info("Make sure your `.env` file is configured correctly (see `.env.example`).")

# --- Auto-refresh via meta refresh ---
if auto_refresh:
    import streamlit.components.v1 as components
    components.html("<meta http-equiv='refresh' content='30'>", height=0)
