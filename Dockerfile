FROM apache/airflow:2.0.2
USER root
RUN apt-get update && apt-get install -y unzip wget