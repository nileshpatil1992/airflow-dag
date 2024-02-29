import airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 


try:
  dag_conf = DAG(dag_id='airflow_spark')
  spark_submit_local=SparkSubmitOperator(task_id='sparksubmitjobs',application='sparksubmitcode.py',conn_id='spark_local',dag=dag_conf)
except Exception as error:
  print("An exception occurred")
  print("An exception occurred:", error)
  import time
  print("sleeping")
  time.sleep(300)
  print("wakeup")

if  __name__=='__main__':
    dag_conf.cli()


import time
print("sleeping")
time.sleep(300)
print("wakeup")
