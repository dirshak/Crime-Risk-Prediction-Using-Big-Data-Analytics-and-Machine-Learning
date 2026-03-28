from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------
# START SPARK
# -------------------------------
spark = SparkSession.builder \
    .appName("CrimePipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------
# LOAD DATA
# -------------------------------
df = spark.read.csv(
    "data/Crimes_-_2001_to_Present.csv",
    header=True,
    inferSchema=True
)

print("Initial count:", df.count())

# -------------------------------
# CLEAN DATA
# -------------------------------

# Remove missing coordinates
df = df.dropna(subset=["Latitude", "Longitude"])

# Fix date format safely

from pyspark.sql.functions import expr

df = df.withColumn(
    "Date",
    expr("try_to_timestamp(Date, 'MM/dd/yyyy hh:mm:ss a')")
)

# Drop invalid dates
df = df.dropna(subset=["Date"])

# -------------------------------
# FEATURE ENGINEERING
# -------------------------------
df = df.withColumn("hour", hour("Date")) \
       .withColumn("day", dayofweek("Date")) \
       .withColumn("month", month("Date")) \
       .withColumn("year", year("Date"))

# Convert boolean
df = df.withColumn("Arrest", col("Arrest").cast("int")) \
       .withColumn("Domestic", col("Domestic").cast("int"))

# -------------------------------
# SAVE AS PARQUET (FAST)
# -------------------------------
df.write.mode("overwrite").parquet("outputs/clean_data")

print("Cleaned count:", df.count())
print("✅ Data pipeline complete")
