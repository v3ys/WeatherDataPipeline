from pyspark.sql import SparkSession
import requests
from datetime import datetime

API_KEY = "3f6b1a4fc1c50b0e5301f743c322ec27"
CITY_NAME = "Istanbul"
API_URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY_NAME}&appid={API_KEY}"

spark = SparkSession.builder \
    .appName("WeatherDataPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def load_raw_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        weather_data = response.json()
        
        df = spark.read.json(spark.sparkContext.parallelize([weather_data]))

        date_str = datetime.now().strftime('%Y/%m/%d')
        
        df.write.format("delta").mode("overwrite").save(f"/data/raw/weather_tr/{date_str}")
    else:
        print(f"API isteği başarısız oldu: {response.status_code}")

def validate_data():
    date_str = datetime.now().strftime('%Y/%m/%d')
    raw_df = spark.read.format("delta").load(f"/data/raw/weather_tr/{date_str}")

    validated_df = raw_df.filter("main.temp is not null and main.humidity is not null")

    validated_df.write.format("delta").mode("overwrite").save(f"/data/validated/weather_tr/{date_str}")

def curate_data():
    date_str = datetime.now().strftime('%Y/%m/%d')
    validated_df = spark.read.format("delta").load(f"/data/validated/weather_tr/{date_str}")

    curated_df = validated_df.select("main.temp", "main.humidity")
    curated_df.write.format("delta").mode("append").save("/data/curated/weather_tr")

load_raw_data()
validate_data()
curate_data()

spark.stop()
