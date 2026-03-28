from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# -------------------------------
# STYLE SETTINGS (IMPORTANT)
# -------------------------------
sns.set_theme(style="darkgrid")
plt.rcParams["figure.figsize"] = (10, 6)

spark = SparkSession.builder.appName("AdvancedAnalytics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("outputs/clean_data")

# -------------------------------
# 🔥 1. TOP DISTRICTS (BEAUTIFUL BAR)
# -------------------------------
district_df = df.groupBy("District") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10) \
    .toPandas()

plt.figure()
sns.barplot(
    data=district_df,
    x="count",
    y=district_df["District"].astype(str),
    palette="magma"
)
plt.title("Top 10 Crime Districts", fontsize=16, weight="bold")
plt.xlabel("Total Crimes")
plt.ylabel("District")
plt.tight_layout()
plt.savefig("outputs/visual_top_districts.png", dpi=300)
plt.close()

# -------------------------------
# 🔥 2. CRIME BY HOUR (SMOOTH LINE)
# -------------------------------
hour_df = df.groupBy("hour") \
    .count() \
    .orderBy("hour") \
    .toPandas()

plt.figure()
sns.lineplot(data=hour_df, x="hour", y="count", marker="o")
plt.title("Crime Distribution by Hour", fontsize=16, weight="bold")
plt.xlabel("Hour of Day")
plt.ylabel("Number of Crimes")
plt.xticks(range(0, 24))
plt.tight_layout()
plt.savefig("outputs/visual_hour_trend.png", dpi=300)
plt.close()

# -------------------------------
# 🔥 3. YEARLY TREND (PRO LOOK)
# -------------------------------
year_df = df.groupBy("year") \
    .count() \
    .orderBy("year") \
    .toPandas()

plt.figure()
sns.lineplot(data=year_df, x="year", y="count", linewidth=2.5)
plt.title("Crime Trend Over Years", fontsize=16, weight="bold")
plt.xlabel("Year")
plt.ylabel("Total Crimes")
plt.tight_layout()
plt.savefig("outputs/visual_year_trend.png", dpi=300)
plt.close()

# -------------------------------
# 🔥 4. HEATMAP (DISTRICT VS HOUR)
# -------------------------------
heat_df = df.groupBy("District", "hour") \
    .count() \
    .toPandas()

pivot = heat_df.pivot(index="District", columns="hour", values="count").fillna(0)

plt.figure(figsize=(14, 6))
sns.heatmap(pivot, cmap="rocket")
plt.title("Crime Intensity Heatmap (District vs Hour)", fontsize=16, weight="bold")
plt.xlabel("Hour")
plt.ylabel("District")
plt.tight_layout()
plt.savefig("outputs/visual_heatmap.png", dpi=300)
plt.close()

# -------------------------------
# 🔥 5. GEO VISUAL (PLOTLY INTERACTIVE)
# -------------------------------
geo_df = df.select("Latitude", "Longitude") \
    .sample(0.02) \
    .toPandas()

fig = px.scatter_mapbox(
    geo_df,
    lat="Latitude",
    lon="Longitude",
    zoom=10,
    height=600,
    title="Crime Distribution Map"
)

fig.update_layout(mapbox_style="carto-darkmatter")
fig.write_html("outputs/visual_geo_map.html")

# -------------------------------
# 🔥 6. CRIME TYPE DISTRIBUTION
# -------------------------------
type_df = df.groupBy("Primary Type") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10) \
    .toPandas()

plt.figure()
sns.barplot(
    data=type_df,
    x="count",
    y="Primary Type",
    palette="coolwarm"
)
plt.title("Top Crime Types", fontsize=16, weight="bold")
plt.xlabel("Count")
plt.ylabel("Crime Type")
plt.tight_layout()
plt.savefig("outputs/visual_crime_types.png", dpi=300)
plt.close()

print("🔥 High-level visuals generated in outputs/")
