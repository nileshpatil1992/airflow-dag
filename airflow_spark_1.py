import airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 



dag_conf = DAG(dag_id='airflow_spark_1')
spark_submit_local=SparkSubmitOperator(task_id='sparksubmitjobs',application='sparksubmitcode.py',conn_id='myk8s',dag=dag_conf)
spark_submit_local


if  __name__=='__main__':
    dag_conf.cli()
