import pandas as pd
import glob
import plotly.express as px

# -------------------------------
# LOAD ALL CSV PARTS (IMPORTANT)
# -------------------------------
files = glob.glob("outputs/risk_predictions/*.csv")

df_list = [pd.read_csv(f) for f in files]
df = pd.concat(df_list, ignore_index=True)

# Sample for performance
df = df.sample(frac=0.1)

# -------------------------------
# CREATE INTERACTIVE MAP
# -------------------------------
fig = px.scatter_mapbox(
    df,
    lat="Latitude",
    lon="Longitude",
    color="risk_score",
    size="risk_score",
    color_continuous_scale="Turbo",
    zoom=10,
    title="🚨 Crime Risk Prediction Map"
)

fig.update_layout(mapbox_style="carto-darkmatter")
fig.write_html("outputs/risk_map.html")

print("✅ Risk Map Generated")
