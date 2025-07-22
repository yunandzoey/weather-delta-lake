from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pytest

# --- Spark session ---
try:
    # Works in Databricks CE, but may fail locally
    spark = SparkSession.active
    if isinstance(spark, type(SparkSession.active)):  # guard if it's just a method
        raise AttributeError
except AttributeError:
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_silver_transform")
        .getOrCreate()
    )

print("DEBUG spark type:", type(spark))  # should be SparkSession

# --- Load table or sample ---
def load_silver():
    try:
        return spark.table("weather_silver.daily")
    except Exception:
        data = [
            ("2025-07-15", 52.52, 13.405, 24, 20.0, 30.0, 45.0, True),
        ]
        cols = [
            "date",
            "location_lat",
            "location_lon",
            "row_count",
            "avg_temp_c",
            "max_wind_kmh",
            "min_humidity_pct",
            "dq_passed",
        ]
        return spark.createDataFrame(data, cols)

df = load_silver()

# --- Tests ---
def test_row_count_complete_days():
    bad = df.filter((F.col("date") < F.current_date()) & (F.col("row_count") != 24))
    assert bad.count() == 0

def test_dq_passed():
    assert df.filter(F.col("dq_passed") == False).count() == 0
