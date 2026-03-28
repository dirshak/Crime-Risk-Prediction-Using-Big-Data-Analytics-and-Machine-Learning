from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AreaRisk").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("outputs/clean_data")

# -------------------------------
# CREATE AREA RISK SCORE
# -------------------------------
area_risk = df.groupBy("District") \
    .agg(
        count("*").alias("total_crimes"),
        avg("Arrest").alias("arrest_rate"),
        avg("Domestic").alias("domestic_rate")
    )

# Normalize risk score
area_risk = area_risk.withColumn(
    "risk_score",
    (col("total_crimes") * 0.6 +
     (1 - col("arrest_rate")) * 100 * 0.2 +
     col("domestic_rate") * 100 * 0.2)
)

# Sort highest risk
area_risk = area_risk.orderBy("risk_score", ascending=False)

# Save
area_risk.toPandas().to_csv("outputs/area_risk_scores.csv", index=False)

area_risk.show(10)

print("Area Risk Scores Generated")
