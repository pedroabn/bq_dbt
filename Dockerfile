FROM quay.io/astronomer/astro-runtime:8.8.0
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --upgrade pip && \
    pip install --no-cache-dir --prefer-binary dbt-bigquery==1.5.3 && deactivate