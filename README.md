# Mini-Project: Spark Streaming and ML - Soccer Results Analytics

## Author
Raghye Meissara Bilal

## Project Description
Data pipeline for Mauritanian soccer match results combining:
- Batch processing with Spark
- Real-time streaming with Kafka and Spark Structured Streaming
- Machine Learning with Spark MLlib

## Dataset
- Historical: rim_championnat_results_2007-2025.csv (2744 matches)
- Scraped: Wikipedia FR - Championnat de Mauritanie 2024-2025

## Structure
- notebooks/prepare_data.ipynb: Data preparation + Kafka producer
- notebooks/train_model.ipynb: ML model training and evaluation
- src/producer.py: Kafka producer script
- src/stream_job.py: Spark Structured Streaming job

## How to Run
1. docker-compose up -d
2. Run prepare_data.ipynb
3. python src/stream_job.py
4. python src/producer.py
5. Run train_model.ipynb

## Results
- Random Forest Accuracy: 40.4%
- Logistic Regression Accuracy: 38.3%
- Scraping source: https://fr.wikipedia.org/wiki/Championnat_de_Mauritanie_de_football_2024-2025
