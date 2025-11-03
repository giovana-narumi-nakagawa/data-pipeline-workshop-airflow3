# Minimal Dockerfile to extend the official Apache Airflow 3 image
# Allows installing extra Python packages and copying DAGs/plugins when building a custom image.

ARG AIRFLOW_IMAGE=apache/airflow:3.0.0
FROM ${AIRFLOW_IMAGE}

USER root
COPY requirements.txt /tmp/requirements.txt
# Allow installing as root inside the image without pip failing.
# pip >= 23 supports --root-user-action=ignore which avoids the error
# "You are running pip as root. Please use 'airflow' user to run pip!"
RUN if [ -s /tmp/requirements.txt ]; then \
			# Run pip as the 'airflow' user to satisfy the base image policy
			su -s /bin/bash airflow -c "pip install --no-cache-dir -r /tmp/requirements.txt"; \
		fi

# Copy dags and plugins at build time (optional; can also mount volumes)
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

# Ensure correct ownership
RUN chown -R airflow: /opt/airflow/dags /opt/airflow/plugins /tmp/requirements.txt || true

USER airflow

# Image ready to be used by docker-compose services (webserver, scheduler, worker, init)
