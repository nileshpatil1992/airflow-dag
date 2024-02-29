from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark import SparkContext

try:


  sc=SparkContext("local","StreamTest")

  #sp=racwdli&st=2024-02-05T15:28:30Z&se=2024-02-28T23:28:30Z&spr=https&sv=2022-11-02&sr=c&sig=7SdkUPorz4B8PWKKgWdYQDXg3zHILhbcGoULLVVgRqM%3D
  #https://pocstorageforkazhuga.blob.core.windows.net/data?sp=racwdli&st=2024-02-05T15:28:30Z&se=2024-02-28T23:28:30Z&spr=https&sv=2022-11-02&sr=c&sig=7SdkUPorz4B8PWKKgWdYQDXg3zHILhbcGoULLVVgRqM%3D
  spark=SparkSession.builder.appName("test_airflow").master("local").config("spark.jars", "/home/nilesh/project/airflow/src/jars/org.apache.hadoop:hadoop-azure:3.2.4,/home/nilesh/project/airflow/src/jars/azure-storage-8.5.0.jar").getOrCreate()

  storageAccount='pocstorageforkazhuga'
  container='data'
  accessKey='nMcRBtQ1Wz3tp+JPh97kpqRerSZprIvkiSDbSQpJKjUwz28H0Jf5awKkEu4NyU5WmHzXnU7KFboQ+AStRnuhJw=='
  sasToken='sp=racwdli&st=2024-02-05T15:28:30Z&se=2024-02-28T23:28:30Z&spr=https&sv=2022-11-02&sr=c&sig=7SdkUPorz4B8PWKKgWdYQDXg3zHILhbcGoULLVVgRqM%3D'

  #spark.conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6")
  #spark.conf.set(f"fs.azure.account.auth.type.{storageAccount}.dfs.core.windows.net", "SAS")
  spark.conf.set(f"fs.azure.account.key.{storageAccount}.dfs.core.windows.net",accessKey)
  #spark.conf.set(f"fs.azure.sas.token.provider.type.{storageAccount}.dfs.core.windows.net", "home.nilesh.project.airflow.src.FixedSASTokenProvider")
  #spark.conf.set(f"fs.azure.sas.fixed.token.{storageAccount}.dfs.core.windows.net", sasToken)

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
  path1=f"abfss://{container}@{storageAccount}.dfs.core.windows.net/electricity.csv"
  spark.read.csv(path1).show()

  path=f"abfss://{container}@{storageAccount}.dfs.core.windows.net/folder1/emp"
  print(path)
  employeesDF.write.mode('append').csv(path)
  
except Exception as error:
  print("An exception occurred")
  print("An exception occurred:", error)
  import time
  print("sleeping except")
  time.sleep(10)
  print("wakeup except")

import time
print("sleeping")
time.sleep(10)
print("wakeup")
#dbutils.fs.ls(f"abfss://{container}@{storageAccount}.dfs.core.windows.net/")
