from pyspark.sql import SparkSession
import plotly.express as px

spark = SparkSession.builder.appName("TimeAnimation").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("outputs/clean_data")

# Reduce size
pdf = df.select("Latitude", "Longitude", "year") \
    .sample(0.02) \
    .toPandas()

# Animation
fig = px.scatter_mapbox(
    pdf,
    lat="Latitude",
    lon="Longitude",
    animation_frame="year",
    zoom=10,
    title="🎬 Crime Evolution Over Time"
)

fig.update_layout(mapbox_style="carto-darkmatter")
fig.write_html("outputs/time_animation.html")

print("🎬 Time animation created")
