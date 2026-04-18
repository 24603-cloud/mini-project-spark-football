# producer.py - Kafka Producer for Soccer Match Results
import json
import time
from pyspark.sql import SparkSession
from kafka import KafkaProducer

TOPIC = "soccer_matches"
BOOTSTRAP = "kafka:9092"
DELAY = 0.5

def main():
    spark = SparkSession.builder \
        .appName("SoccerProducer") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    df = spark.read.parquet("/workspace/data/outputs/prepared")
    matches = df.select(
        "season", "date", "home_team", "away_team",
        "home_goals", "away_goals", "result",
        "total_goals", "match_year"
    ).collect()
    
    print(f" {len(matches)} matchs à envoyer...")
    
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for i, row in enumerate(matches):
        message = {
            "season":      row["season"],
            "date":        str(row["date"]),
            "home_team":   row["home_team"],
            "away_team":   row["away_team"],
            "home_goals":  row["home_goals"],
            "away_goals":  row["away_goals"],
            "result":      row["result"],
            "total_goals": row["total_goals"],
            "match_year":  row["match_year"]
        }
        producer.send(TOPIC, value=message)
        if (i+1) % 100 == 0:
            print(f" {i+1}/{len(matches)} envoyés...")
        time.sleep(DELAY)
    
    producer.flush()
    print(f" Tous les matchs envoyés!")
    spark.stop()

if __name__ == "__main__":
    main()