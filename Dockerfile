FROM apache/airflow:2.0.2
USER root
RUN apt-get update && apt-get install -y unzip wget git
RUN curl https://cli-assets.heroku.com/install.sh | sh
COPY .netrc /home/airflow/
RUN chown airflow:airflow /home/airflow/.netrc
USER airflow
COPY ./requeriments.txt /
RUN pip install -r /requeriments.txt
