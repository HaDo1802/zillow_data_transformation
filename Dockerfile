FROM apache/airflow:2.9.2-python3.9

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.9
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install system build tools (gcc compiles psycopg2, libpq-dev is the Postgres client lib)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Run pip as the airflow user so packages land in /home/airflow/.local/ —
# the same place Airflow lives. Everything shares one location, no path tricks needed.
USER airflow

# Packages that touch Airflow's own dependency graph go through the constraint file.
# It pins exact versions Airflow was tested with, preventing subtle breakage.
RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
    "python-dotenv>=1.0.1" \
    "pandas>=2.0.0" \
    "numpy>=1.24.0"

# Install supabase and psycopg2 (our DB connection packages, not part of Airflow's graph)
RUN pip install --no-cache-dir \
    "supabase>=2.28.0" \
    "psycopg2-binary>=2.9.11"

# Install cosmos and dbt together so pip resolves all constraints in one pass.
# dbt-core 1.8.x requires protobuf<5 — the same range Airflow's opentelemetry-proto
# already requires — so there is no conflict and no install-order trick needed.
RUN pip install --no-cache-dir \
    "astronomer-cosmos==1.8.2" \
    "dbt-core==1.8.7" \
    "dbt-postgres==1.8.2"

# astronomer-cosmos installs its code under cosmos/ not astronomer/cosmos/
# Python's import system needs an astronomer/ namespace directory to find it
RUN mkdir -p /home/airflow/.local/lib/python3.9/site-packages/astronomer && \
    ln -s /home/airflow/.local/lib/python3.9/site-packages/cosmos \
          /home/airflow/.local/lib/python3.9/site-packages/astronomer/cosmos

# Copy application code last so Docker's layer cache keeps the slow pip steps above
# unchanged when you only edit a DAG or a dbt model.
COPY --chown=airflow:root zillow_transformation/dbt_packages/ /opt/airflow/zillow_transformation/dbt_packages/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root zillow_transformation/ /opt/airflow/zillow_transformation/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
