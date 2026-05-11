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

# Install cosmos BEFORE dbt — cosmos resolves through Airflow's existing opentelemetry
# packages and would downgrade protobuf to <5 if dbt were already present.
# With cosmos installed first, protobuf is still at the base-image version (4.25.x).
RUN pip install --no-cache-dir "astronomer-cosmos==1.8.2"

# Install dbt after cosmos. dbt-adapters and dbt-common both require protobuf>=6.0,
# so pip upgrades protobuf to 6.32.1 here. Cosmos is already installed and pip won't
# re-resolve it, so protobuf stays at 6.32.1 for the rest of the image.
RUN pip install --no-cache-dir \
    "dbt-core==1.10.11" \
    "dbt-postgres==1.9.1" \
    "dbt-adapters==1.16.7" \
    "dbt-common==1.31.0" \
    "protobuf==6.32.1"

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
