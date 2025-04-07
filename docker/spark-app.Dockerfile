FROM bitnami/spark:latest

USER root

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --upgrade pip

# Install requirements if needed
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY spark_jobs/entrypoint.sh /opt/spark_jobs/entrypoint.sh
WORKDIR /opt/spark_jobs
RUN chmod +x /opt/spark_jobs/entrypoint.sh
ENTRYPOINT ["/opt/spark_jobs/entrypoint.sh"]