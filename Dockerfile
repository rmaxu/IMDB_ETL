FROM apache/airflow:1.10.10

ARG AIRFLOW_HOME=/home/airflow

ENV AIRFLOW_HOME=${AIRFLOW_HOME}
# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

USER root

# Set workdir (it's like a cd inside the container)
WORKDIR ${AIRFLOW_HOME}

# Create the "dags" folder
#RUN mkdir dags

# Copy the entrypoint.sh from host to container 
COPY ./start-airflow.sh ./start-airflow.sh

# Copy the content of the folder dags to container
#COPY --chown=airflow:root data-ops_etl/dags /home/airflow/dags

# Set the start-airflow.sh file to be executable
#RUN chmod +x ./start-airflow.sh

# Set the username to use
USER airflow

EXPOSE 8080

ENTRYPOINT [ "bash"]