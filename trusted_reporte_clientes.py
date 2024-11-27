from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

from datetime import *

print("Argumentos: ")
for i, x in enumerate(sys.argv):
    print(f'{i}: {x}')

# argumentos provenientes del yaml
bucket_landing= sys.argv[1]
folder= sys.argv[2]
bucket_prefix= sys.argv[3]
bucket_temp= sys.argv[4]
bucket_temp_prefix= sys.argv[5]
project_raw= sys.argv[6]
dataset_raw= sys.argv[7]
table_raw= sys.argv[8]
project_trusted= sys.argv[9]
dataset_trusted= sys.argv[10]
table_trusted= sys.argv[11]

bucket = f"{bucket_temp}/{bucket_temp_prefix}" # donde se guardan archivos temporales que genera el proceso

spark = SparkSession \
    .builder \
    .appName("script_customers") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.2.0") \
    .config('temporaryGcsBucket', bucket) \
    .getOrCreate()


df_raw = (
    spark.read.format("bigquery")
    .option("project", project_raw)
    .option("dataset", dataset_raw)
    .load(table_raw)
)

df_trusted = df_raw.withColumn("fecha_suscripcion", df_raw.fecha_suscripcion.cast('date')) # se cambia el tipo de dato

df_trusted.show(10, truncate = False) # una vista previa para validar
df_trusted.printSchema()

print('******************** GUARDANDO DATOS EN TRUSTED ... ************************') 
df_trusted.write.format("bigquery").mode('overwrite') \
    .option("project", project_trusted)\
    .option("dataset", dataset_trusted)\
    .option("table", table_trusted)\
    .save()

