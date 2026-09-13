ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.1-python3.10

FROM ${AIRFLOW_BASE_IMAGE}

COPY airflow/requirements.txt /tmp/airflow-requirements.txt

RUN pip install \
    --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/airflow-requirements.txt
