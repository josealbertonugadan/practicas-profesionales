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

bucket = f"{bucket_temp}/{bucket_temp_prefix}" # donde se guardan archivos temporales que genera el proceso

spark = SparkSession \
    .builder \
    .appName("script_customers") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.2.0") \
    .config('temporaryGcsBucket', bucket) \
    .getOrCreate()


# INGESTA EN LA CAPA RAW 
df = spark.read.format("csv")\
    .options(header=True)\
    .load(f"gs://{bucket_landing}/{folder}/{bucket_prefix}") # ubicacion del archivo de clientes en Storage

# Se renombran los campos
df = df.withColumnRenamed("Customer Id", "id_cliente")
df = df.withColumnRenamed("First Name", "nombre")
df = df.withColumnRenamed("Last Name", "apellido")
df = df.withColumnRenamed("Company", "empresa")
df = df.withColumnRenamed("City", "ciudad")
df = df.withColumnRenamed("Country", "pais")
df = df.withColumnRenamed("Phone 1", "telefono_celular")
df = df.withColumnRenamed("Phone 2", "telefono_fijo")
df = df.withColumnRenamed("Email", "correo")
df = df.withColumnRenamed("Subscription Date", "fecha_suscripcion")

df.show(10, truncate = False) # una vista previa para validar
df.printSchema()

print('******************** GUARDANDO DATOS EN RAW ... ************************') 
df.write.format("bigquery").mode('overwrite') \
    .option("project", project_raw)\
    .option("dataset", dataset_raw)\
    .option("table", table_raw)\
    .save()

