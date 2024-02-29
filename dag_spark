import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession, functions

def processo_etl_spark():
    spark=SparkSession.builder.appName("test_airflow").master("local").getOrCreate()
    #.config("spark.jars", "/home/nilesh/project/airflow/src/jars/org.apache.hadoop:hadoop-azure:3.2.4,/home/nilesh/project/airflow/src/jars/azure-storage-8.5.0.jar")

    storageAccount='pocstorageforkazhuga'
    container='data'
    accessKey='nMcRBtQ1Wz3tp+JPh97kpqRerSZprIvkiSDbSQpJKjUwz28H0Jf5awKkEu4NyU5WmHzXnU7KFboQ+AStRnuhJw=='
    sasToken='sp=racwdli&st=2024-02-05T15:28:30Z&se=2024-02-28T23:28:30Z&spr=https&sv=2022-11-02&sr=c&sig=7SdkUPorz4B8PWKKgWdYQDXg3zHILhbcGoULLVVgRqM%3D'
    employees = [
      (1, "Scott", "Tiger", 1000.0, 
        "united states", "+1 123 456 7890", "123 45 6789"
      ),
      (2, "Henry", "Ford", 1250.0, 
        "India", "+91 234 567 8901", "456 78 9123"
      ),
      (3, "Nick", "Junior", 750.0, 
        "united KINGDOM", "+44 111 111 1111", "222 33 4444"
      ),
      (4, "Bill", "Gomes", 1500.0, 
        "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
      )
    ]

    employeesDF = spark.createDataFrame(employees,
                      schema="""employee_id INT, first_name STRING, 
                      last_name STRING, salary FLOAT, nationality STRING,
                      phone_number STRING, ssn STRING"""
                    )


    employeesDF.show(truncate=False)

    
    termino = datetime.now()
    print(termino)
    print(termino - inicio)

default_args = {
    'owner': 'jozimar',
    'start_date': datetime(2020, 11, 18),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('dag_teste_spark_documento_vencido_v01',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    task_elt_documento_pagar = PythonOperator(
        task_id='elt_documento_pagar_spark',
        python_callable=processo_etl_spark
    )
    task_elt_documento_pagar
