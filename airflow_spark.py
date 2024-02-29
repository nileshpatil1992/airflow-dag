import airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 



dag_conf = DAG(dag_id='airflow_spark')
spark_submit_local=SparkSubmitOperator(task_id='sparksubmitjobs',application='sparksubmitcode.py',conn_id='spark_default',dag=dag_conf,spark_binary='spark-submit',deploy_mode='client')
spark_submit_local


if  __name__=='__main__':
    dag_conf.cli()
