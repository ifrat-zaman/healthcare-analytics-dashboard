"""dashboard.py — Streamlit single-page dashboard with five tabs.

Reads all data directly from PostgreSQL via SQLAlchemy.
Run with: streamlit run scripts/dashboard.py
"""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Hospital Analytics", layout="wide")

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
# On Streamlit Cloud, st.secrets["database"]["url"] holds the full connection
# string (e.g. postgresql+psycopg2://user:pass@host/db).
# Locally, fall back to individual DB_* environment variables.
def _build_url() -> str:
    try:
        return st.secrets["database"]["url"]
    except (KeyError, FileNotFoundError):
        pass
    DB_NAME = os.getenv("DB_NAME", "hospital_analytics")
    DB_USER = os.getenv("DB_USER", "ifratzaman")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5433")
    if DB_PASSWORD:
        return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


@st.cache_resource
def get_engine():
    url = _build_url()
    try:
        engine = create_engine(url, connect_args={"sslmode": "require"})
        # Verify the connection is actually reachable
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as exc:
        st.error(f"Database connection failed: {exc}")
        st.stop()


# ---------------------------------------------------------------------------
# Cached query helpers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_services_weekly() -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text("SELECT * FROM services_weekly"), conn)


@st.cache_data(ttl=300)
def load_patients() -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text("SELECT * FROM patients"), conn)


@st.cache_data(ttl=300)
def load_staff_schedule() -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text("SELECT * FROM staff_schedule"), conn)


@st.cache_data(ttl=300)
def load_staffing_vs_satisfaction() -> pd.DataFrame:
    engine = get_engine()
    sql = text("""
        SELECT
            sw.week,
            sw.service,
            COALESCE(SUM(ss.present::int), 0) AS staff_present_count,
            sw.patient_satisfaction,
            sw.staff_morale,
            sw.occupancy_rate
        FROM services_weekly sw
        LEFT JOIN staff_schedule ss
            ON sw.week = ss.week AND sw.service = ss.service
        GROUP BY sw.week, sw.service,
                 sw.patient_satisfaction, sw.staff_morale, sw.occupancy_rate
        ORDER BY sw.week, sw.service
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


# ---------------------------------------------------------------------------
# Load base data
# ---------------------------------------------------------------------------
sw_all = load_services_weekly()
pat_all = load_patients()
sched_all = load_staff_schedule()
svs_all = load_staffing_vs_satisfaction()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
st.sidebar.title("Filters")

all_services = sorted(sw_all["service"].unique().tolist())
selected_services = st.sidebar.multiselect(
    "Service", options=["All"] + all_services, default=["All"]
)
week_range = st.sidebar.slider("Week range", min_value=1, max_value=52, value=(1, 52))
all_events = sorted(sw_all["event"].unique().tolist())
selected_events = st.sidebar.multiselect(
    "Event", options=["All"] + all_events, default=["All"]
)


def filter_sw(df: pd.DataFrame) -> pd.DataFrame:
    """Apply sidebar filters to a services_weekly-shaped DataFrame."""
    d = df.copy()
    if "All" not in selected_services and selected_services:
        d = d[d["service"].isin(selected_services)]
    d = d[d["week"].between(week_range[0], week_range[1])]
    if "All" not in selected_events and selected_events:
        d = d[d["event"].isin(selected_events)]
    return d


def filter_pat(df: pd.DataFrame) -> pd.DataFrame:
    """Apply service filter to patients-shaped DataFrame."""
    d = df.copy()
    if "All" not in selected_services and selected_services:
        d = d[d["service"].isin(selected_services)]
    return d


def filter_sched(df: pd.DataFrame) -> pd.DataFrame:
    """Apply service + week filters to staff_schedule-shaped DataFrame."""
    d = df.copy()
    if "All" not in selected_services and selected_services:
        d = d[d["service"].isin(selected_services)]
    d = d[d["week"].between(week_range[0], week_range[1])]
    return d


def filter_svs(df: pd.DataFrame) -> pd.DataFrame:
    """Apply service + week filters to staffing_vs_satisfaction DataFrame."""
    d = df.copy()
    if "All" not in selected_services and selected_services:
        d = d[d["service"].isin(selected_services)]
    d = d[d["week"].between(week_range[0], week_range[1])]
    return d


sw = filter_sw(sw_all)
pat = filter_pat(pat_all)
sched = filter_sched(sched_all)
svs = filter_svs(svs_all)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Overview", "Bed Management", "Patient Flow", "Staff Operations", "Event Impact"]
)

# ── Tab 1: Overview ─────────────────────────────────────────────────────────
with tab1:
    st.header("Overview")

    if sw.empty or pat.empty:
        st.warning("No data for selected filters.")
    else:
        avg_los = pat["length_of_stay"].mean()
        avg_occ = sw["occupancy_rate"].mean()
        total_refused = sw["patients_refused"].sum()
        avg_sat = sw["patient_satisfaction"].mean()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Avg Length of Stay", f"{avg_los:.1f} days")
        c2.metric("Overall Occupancy Rate", f"{avg_occ:.1f}%")
        c3.metric("Total Patients Refused", f"{int(total_refused):,}")
        c4.metric("Avg Patient Satisfaction", f"{avg_sat:.1f}")

        st.subheader("Metrics by Service")
        summary = (
            sw.groupby("service")
            .agg(
                avg_occupancy_rate=("occupancy_rate", "mean"),
                total_refused=("patients_refused", "sum"),
                avg_satisfaction=("patient_satisfaction", "mean"),
            )
            .round(2)
            .reset_index()
        )
        los_by_service = (
            pat.groupby("service")["length_of_stay"]
            .mean()
            .round(2)
            .reset_index()
            .rename(columns={"length_of_stay": "avg_length_of_stay"})
        )
        summary = summary.merge(los_by_service, on="service", how="left")
        st.dataframe(summary, use_container_width=True)

# ── Tab 2: Bed Management ────────────────────────────────────────────────────
with tab2:
    st.header("Bed Management")

    if sw.empty:
        st.warning("No data for selected filters.")
    else:
        fig_occ = px.line(
            sw.sort_values("week"),
            x="week",
            y="occupancy_rate",
            color="service",
            title="Weekly Bed Occupancy Rate by Service",
            labels={"week": "Week", "occupancy_rate": "Occupancy Rate (%)", "service": "Service"},
        )
        st.plotly_chart(fig_occ, use_container_width=True)

        heatmap_data = (
            sw.groupby(["month", "service"])["patients_refused"]
            .sum()
            .reset_index()
        )
        heatmap_pivot = heatmap_data.pivot(
            index="service", columns="month", values="patients_refused"
        ).fillna(0)

        fig_heat = px.imshow(
            heatmap_pivot,
            title="Patient Refusals by Month and Service",
            labels={"x": "Month", "y": "Service", "color": "Patients Refused"},
            color_continuous_scale="Reds",
            aspect="auto",
        )
        st.plotly_chart(fig_heat, use_container_width=True)

# ── Tab 3: Patient Flow ──────────────────────────────────────────────────────
with tab3:
    st.header("Patient Flow")

    if sw.empty or pat.empty:
        st.warning("No data for selected filters.")
    else:
        weekly_flow = (
            sw.groupby("week")[["patients_admitted", "patients_request"]]
            .sum()
            .reset_index()
        )
        flow_melted = weekly_flow.melt(
            id_vars="week",
            value_vars=["patients_admitted", "patients_request"],
            var_name="metric",
            value_name="count",
        )
        fig_flow = px.line(
            flow_melted,
            x="week",
            y="count",
            color="metric",
            title="Weekly Admissions vs Demand",
            labels={"week": "Week", "count": "Patients", "metric": "Metric"},
        )
        st.plotly_chart(fig_flow, use_container_width=True)

        fig_box = px.box(
            pat,
            x="service",
            y="length_of_stay",
            color="service",
            title="Length of Stay Distribution by Service",
            labels={"service": "Service", "length_of_stay": "Length of Stay (days)"},
        )
        st.plotly_chart(fig_box, use_container_width=True)

        age_counts = (
            pat.groupby(["age_group", "service"])
            .size()
            .reset_index(name="count")
        )
        fig_age = px.bar(
            age_counts,
            x="age_group",
            y="count",
            color="service",
            barmode="group",
            title="Patient Age Group Distribution by Service",
            labels={"age_group": "Age Group", "count": "Count", "service": "Service"},
        )
        st.plotly_chart(fig_age, use_container_width=True)

# ── Tab 4: Staff Operations ──────────────────────────────────────────────────
with tab4:
    st.header("Staff Operations")

    if sched.empty or svs.empty:
        st.warning("No data for selected filters.")
    else:
        attendance = (
            sched.groupby(["service", "role"])
            .apply(lambda x: x["present"].sum() / len(x) * 100)
            .reset_index(name="attendance_rate")
            .round({"attendance_rate": 2})
        )
        fig_att = px.bar(
            attendance,
            x="service",
            y="attendance_rate",
            color="role",
            barmode="group",
            title="Staff Attendance Rate by Role and Service",
            labels={
                "service": "Service",
                "attendance_rate": "Attendance Rate (%)",
                "role": "Role",
            },
        )
        st.plotly_chart(fig_att, use_container_width=True)

        fig_scatter = px.scatter(
            svs,
            x="staff_present_count",
            y="patient_satisfaction",
            color="service",
            trendline="ols",
            title="Staffing Level vs Patient Satisfaction",
            labels={
                "staff_present_count": "Staff Present Count",
                "patient_satisfaction": "Patient Satisfaction",
                "service": "Service",
            },
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        fig_morale = px.line(
            sw.sort_values("week"),
            x="week",
            y="staff_morale",
            color="service",
            title="Weekly Staff Morale by Service",
            labels={"week": "Week", "staff_morale": "Staff Morale", "service": "Service"},
        )
        st.plotly_chart(fig_morale, use_container_width=True)

# ── Tab 5: Event Impact ──────────────────────────────────────────────────────
with tab5:
    st.header("Event Impact")

    if sw.empty:
        st.warning("No data for selected filters.")
    else:
        event_agg = (
            sw.groupby("event")
            .agg(
                patient_satisfaction=("patient_satisfaction", "mean"),
                staff_morale=("staff_morale", "mean"),
                patients_admitted=("patients_admitted", "mean"),
                patients_refused=("patients_refused", "mean"),
                occupancy_rate=("occupancy_rate", "mean"),
            )
            .round(2)
            .reset_index()
        )

        event_bar_data = event_agg.melt(
            id_vars="event",
            value_vars=["patient_satisfaction", "staff_morale", "patients_admitted"],
            var_name="metric",
            value_name="value",
        )
        fig_event_bar = px.bar(
            event_bar_data,
            x="event",
            y="value",
            color="metric",
            barmode="group",
            title="Key Metrics by Event Type",
            labels={"event": "Event", "value": "Average Value", "metric": "Metric"},
        )
        st.plotly_chart(fig_event_bar, use_container_width=True)

        fig_refusal_box = px.box(
            sw,
            x="event",
            y="refusal_rate",
            color="event",
            title="Patient Refusal Rate by Event Type",
            labels={"event": "Event", "refusal_rate": "Refusal Rate (%)"},
        )
        st.plotly_chart(fig_refusal_box, use_container_width=True)

        st.subheader("Average Metrics by Event Type")
        st.dataframe(event_agg, use_container_width=True)
