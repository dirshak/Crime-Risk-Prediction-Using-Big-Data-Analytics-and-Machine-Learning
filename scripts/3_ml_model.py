from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array

# -------------------------------
# START SPARK
# -------------------------------
spark = SparkSession.builder.appName("CrimeML").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# -------------------------------
# LOAD DATA
# -------------------------------
df = spark.read.parquet("outputs/clean_data")

# -------------------------------
# CLEAN NULLS
# -------------------------------
df = df.dropna(subset=[
    "Latitude", "Longitude",
    "District", "Community Area",
    "hour", "day", "month", "Primary Type"
])

# -------------------------------
# AREA CRIME DENSITY
# -------------------------------
area_density = df.groupBy("Community Area") \
    .count() \
    .withColumnRenamed("count", "area_crime_density")

df = df.join(area_density, on="Community Area", how="left")

# -------------------------------
# NORMALIZE DENSITY
# -------------------------------
max_density = df.agg(max("area_crime_density")).collect()[0][0]

df = df.withColumn(
    "density_norm",
    col("area_crime_density") / max_density
)

# -------------------------------
# TIME RISK (SMART)
# -------------------------------
df = df.withColumn(
    "time_risk",
    when((col("hour") >= 22) | (col("hour") <= 4), 1)
    .when((col("hour") >= 18), 0.6)
    .otherwise(0.2)
)

# -------------------------------
# CRIME SEVERITY (KEY FEATURE)
# -------------------------------
df = df.withColumn(
    "crime_severity",
    when(col("Primary Type").isin("HOMICIDE", "ROBBERY"), 1)
    .when(col("Primary Type").isin("ASSAULT", "BATTERY"), 0.7)
    .when(col("Primary Type").isin("THEFT", "BURGLARY"), 0.5)
    .otherwise(0.3)
)

# -------------------------------
# COMPOSITE RISK SCORE
# -------------------------------
df = df.withColumn(
    "risk_score_raw",
    (col("density_norm") * 0.5 +
     col("time_risk") * 0.3 +
     col("crime_severity") * 0.2)
)

# -------------------------------
# TARGET (BALANCED)
# -------------------------------
df = df.withColumn(
    "high_risk",
    when(col("risk_score_raw") > 0.6, 1).otherwise(0)
)

# -------------------------------
# FEATURES
# -------------------------------
features = [
    "hour",
    "day",
    "month",
    "District",
    "Community Area",
    "density_norm",
    "time_risk",
    "crime_severity"
]

assembler = VectorAssembler(
    inputCols=features,
    outputCol="features",
    handleInvalid="skip"
)

# -------------------------------
# MODEL
# -------------------------------
rf = RandomForestClassifier(
    labelCol="high_risk",
    featuresCol="features",
    numTrees=50,
    maxDepth=12
)

pipeline = Pipeline(stages=[assembler, rf])

# -------------------------------
# TRAIN
# -------------------------------
model = pipeline.fit(df)

predictions = model.transform(df)

# -------------------------------
# FIX PROBABILITY VECTOR
# -------------------------------
predictions = predictions.withColumn(
    "prob_array",
    vector_to_array(col("probability"))
)

# FINAL RISK SCORE
predictions = predictions.withColumn(
    "risk_score",
    col("prob_array")[1] * col("risk_score_raw")
)

# -------------------------------
# SAVE OUTPUT (WITH CRIME TYPE)
# -------------------------------
predictions.select(
    "Latitude",
    "Longitude",
    "risk_score",
    "Primary Type",
    "District"
).write.mode("overwrite") \
 .option("header", True) \
 .csv("outputs/risk_predictions")

print("ML MODEL COMPLETED SUCCESSFULLY")
