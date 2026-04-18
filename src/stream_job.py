from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

TOPIC = "soccer_matches"
BOOTSTRAP = "kafka:9092"
OUTPUT_PATH = "/workspace/data/outputs/streaming"
CHECKPOINT_PATH = "/workspace/data/checkpoints/streaming"

def main():
    spark = SparkSession.builder \
        .appName("SoccerStreamJob") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("season",      StringType(),  True),
        StructField("date",        StringType(),  True),
        StructField("home_team",   StringType(),  True),
        StructField("away_team",   StringType(),  True),
        StructField("home_goals",  IntegerType(), True),
        StructField("away_goals",  IntegerType(), True),
        StructField("result",      StringType(),  True),
        StructField("total_goals", IntegerType(), True),
        StructField("match_year",  IntegerType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    matches_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp")) \
        .select("data.*", "timestamp") \
        .withWatermark("timestamp", "2 minutes")

    home_df = matches_df.select(
        col("home_team").alias("team"),
        when(col("home_goals") > col("away_goals"), 3)
            .when(col("home_goals") == col("away_goals"), 1)
            .otherwise(0).alias("points"),
        when(col("home_goals") > col("away_goals"), 1).otherwise(0).alias("wins"),
        when(col("home_goals") == col("away_goals"), 1).otherwise(0).alias("draws"),
        when(col("home_goals") < col("away_goals"), 1).otherwise(0).alias("losses"),
        col("home_goals").alias("gf"),
        col("away_goals").alias("ga"),
        lit(1).alias("played")
    )

    away_df = matches_df.select(
        col("away_team").alias("team"),
        when(col("away_goals") > col("home_goals"), 3)
            .when(col("away_goals") == col("home_goals"), 1)
            .otherwise(0).alias("points"),
        when(col("away_goals") > col("home_goals"), 1).otherwise(0).alias("wins"),
        when(col("away_goals") == col("home_goals"), 1).otherwise(0).alias("draws"),
        when(col("away_goals") < col("home_goals"), 1).otherwise(0).alias("losses"),
        col("away_goals").alias("gf"),
        col("home_goals").alias("ga"),
        lit(1).alias("played")
    )

    all_df = home_df.union(away_df)
    ranking_df = all_df.groupBy("team").agg(
        sum("played").alias("MJ"),
        sum("wins").alias("V"),
        sum("draws").alias("N"),
        sum("losses").alias("D"),
        sum("gf").alias("BP"),
        sum("ga").alias("BC"),
        (sum("gf") - sum("ga")).alias("GD"),
        sum("points").alias("PTS")
    ).orderBy(col("PTS").desc(), col("GD").desc())

    def write_batch(batch_df, batch_id):
        if batch_df.count() > 0:
            batch_df.write.mode("overwrite").parquet(OUTPUT_PATH)
            print(f"Batch {batch_id} written!")

    query = ranking_df.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("complete") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
