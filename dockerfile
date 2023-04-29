FROM python:3.9

#Instalamos AIRFLOW
WORKDIR /home
RUN mkdir airflow
ENV AIRFLOW_HOME='~/../home/airflow'
ENV PYTHON_VERSION=3.9
ENV AIRFLOW_VERSION=2.5.3
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" 
RUN pip install psycopg2

ENV AIRFLOW__CORE__EXECUTOR='LocalExecutor'
ENV AIRFLOW__CORE__PARALLELISM=2
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql+psycopg2://postgres:udesa856@database-pa-udesa-test.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com:8000'
ENV AIRFLOW__WEBSERVER__WORKERS=1

RUN airflow db init


EXPOSE 8000
CMD ["tail", "-f", "/dev/null"]

