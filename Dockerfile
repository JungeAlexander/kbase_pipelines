FROM quay.io/astronomer/ap-airflow:2.0.0-buster-onbuild

ENV AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
