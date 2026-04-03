FROM apache/airflow:2.9.2-python3.9

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.9
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements-airflow.txt /requirements-airflow.txt
COPY requirements-dbt.txt /requirements-dbt.txt
COPY requirements-cosmos.txt /requirements-cosmos.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt --constraint "${CONSTRAINT_URL}"
RUN pip install --no-cache-dir -r /requirements-cosmos.txt
RUN pip install --no-cache-dir -r /requirements-dbt.txt
RUN pip install --no-cache-dir --upgrade protobuf==6.32.1

COPY --chown=airflow:root zillow_transformation/dbt_project.yml /opt/airflow/zillow_transformation/dbt_project.yml
COPY --chown=airflow:root zillow_transformation/packages.yml /opt/airflow/zillow_transformation/packages.yml
COPY --chown=airflow:root zillow_transformation/profiles.yml /opt/airflow/zillow_transformation/profiles.yml
RUN cd /opt/airflow/zillow_transformation && dbt deps
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root zillow_transformation/ /opt/airflow/zillow_transformation/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
