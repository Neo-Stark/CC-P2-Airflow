FROM apache/airflow:2.0.2
USER root
RUN apt-get update && apt-get install -y unzip wget git
USER airflow
COPY ./requeriments.txt /
RUN sudo pip install -r /requeriments.txt
