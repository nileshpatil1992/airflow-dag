import airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 



dag_conf = DAG(dag_id='airflow_spark_1')
spark_submit_local=SparkSubmitOperator(task_id='sparksubmitjobs',application_file='sparksubmitcode.py',kubernetes_conn_id='myk8s',namespace="spark-jobs",api_group="sparkoperator.k8s.io",
       api_version="v1beta2",do_xcom_push=True,dag=dag_conf)
spark_submit_local


if  __name__=='__main__':
    dag_conf.cli()
