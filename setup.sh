export SPARK_HOME=/workspace/datapipeline/spark-3.1.2-bin-hadoop3.2
export AIRFLOW_HOME=~/airflow

AIRFLOW_VERSION=2.0.2
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"


CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install apache-airflow-providers-apache-spark
pip install boto3
pip install python-dotenv
pip install glob2

# initialize the database
airflow db init

airflow users create \
    --username admin \
    --firstname Lais \
    --lastname Perpetuo \
    --role Admin \
    --email laisperpetuo9@gmail.com

# start the web server, default port is 8080
airflow webserver --port 8080 -D

# start scheduler
airflow scheduler