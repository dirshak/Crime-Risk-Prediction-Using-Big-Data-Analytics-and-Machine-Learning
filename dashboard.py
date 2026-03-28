import streamlit as st
import pandas as pd
import glob
import plotly.express as px

st.set_page_config(layout="wide")

st.title("🚨 Crime Intelligence System")

# -------------------------------
# LOAD DATA
# -------------------------------
files = glob.glob("outputs/risk_predictions/*.csv")
df = pd.concat([pd.read_csv(f) for f in files])

df = df.dropna()

# -------------------------------
# SIDEBAR CONTROLS
# -------------------------------
st.sidebar.header("🔧 Controls")

risk_threshold = st.sidebar.slider(
    "Risk Threshold",
    0.0, 1.0, 0.3, 0.05
)

sample_size = st.sidebar.slider(
    "Map Sample Size (Performance)",
    500, 5000, 2000, 500
)

crime_types = st.sidebar.multiselect(
    "Crime Type",
    options=sorted(df["Primary Type"].unique()),
    default=sorted(df["Primary Type"].unique())[:5]
)

# Apply filters
df = df[
    (df["risk_score"] >= risk_threshold) &
    (df["Primary Type"].isin(crime_types))
]

# -------------------------------
# KPIs
# -------------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("Records", len(df))
col2.metric("Avg Risk", round(df["risk_score"].mean(), 3))
col3.metric("Max Risk", round(df["risk_score"].max(), 3))
col4.metric("Unique Crime Types", df["Primary Type"].nunique())

# -------------------------------
# TABS (PRO LEVEL UI)
# -------------------------------
tab1, tab2, tab3 = st.tabs([
    "📍 Map View",
    "📊 Analytics",
    "🏙️ District Insights"
])

# =====================================================
# 📍 TAB 1: MAP (SMOOTH + FAST)
# =====================================================
with tab1:

    st.subheader("Crime Risk Map")

    # Sample for performance
    map_df = df.sample(min(sample_size, len(df)))

    fig = px.scatter_mapbox(
        map_df,
        lat="Latitude",
        lon="Longitude",
        color="Primary Type",
        size="risk_score",
        hover_data=["risk_score", "District"],
        zoom=10,
        height=650
    )

    fig.update_layout(
        mapbox_style="carto-darkmatter",
        margin={"r":0,"t":40,"l":0,"b":0}
    )

    st.plotly_chart(fig, use_container_width=True)

    # 🔥 Density map (better movement)
    st.subheader("🔥 Risk Density View")

    density_df = df.sample(min(3000, len(df)))

    fig2 = px.density_mapbox(
        density_df,
        lat="Latitude",
        lon="Longitude",
        z="risk_score",
        radius=8,
        zoom=10,
        height=600
    )

    fig2.update_layout(mapbox_style="carto-darkmatter")

    st.plotly_chart(fig2, use_container_width=True)

# =====================================================
# 📊 TAB 2: ANALYTICS
# =====================================================
with tab2:

    st.subheader("Risk Distribution")

    fig3 = px.histogram(
        df,
        x="risk_score",
        nbins=50,
        color="Primary Type"
    )

    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Crime Type Risk")

    crime_df = df.groupby("Primary Type")["risk_score"].mean().reset_index()

    fig4 = px.bar(
        crime_df.sort_values("risk_score", ascending=False),
        x="risk_score",
        y="Primary Type",
        orientation="h"
    )

    st.plotly_chart(fig4, use_container_width=True)

# =====================================================
# 🏙️ TAB 3: DISTRICT INSIGHTS
# =====================================================
with tab3:

    st.subheader("District Risk Levels")

    district_df = df.groupby("District")["risk_score"].mean().reset_index()

    fig5 = px.bar(
        district_df,
        x="District",
        y="risk_score"
    )

    st.plotly_chart(fig5, use_container_width=True)

    st.subheader("Top High-Risk Locations")

    st.dataframe(
        df.sort_values("risk_score", ascending=False).head(20),
        use_container_width=True
    )
