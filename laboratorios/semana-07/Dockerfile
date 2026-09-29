# Imagen de Kestra + entorno Python aislado con dbt-snowflake y el conector
# de Snowflake. Kestra ejecuta la tubería con el task runner "Process", así
# que no necesitamos montar el socket de Docker.
FROM kestra/kestra:v1.3.37

USER root
COPY requirements.txt /tmp/requirements.txt
RUN apt-get update  && apt-get install -y --no-install-recommends python3-venv  && rm -rf /var/lib/apt/lists/*  && /usr/bin/python3 -m venv /opt/pipeline \
 && /opt/pipeline/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements.txt

ENV PIPELINE_PY=/opt/pipeline/bin/python \
    DBT_BIN=/opt/pipeline/bin/dbt
