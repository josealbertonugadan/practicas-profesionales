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
dataset_refined= sys.argv[12]
table_refined= sys.argv[13]

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


# INGESTA EN LA CAPA TRUSTED 
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


# INGESTA EN LA CAPA REFINED
df_trusted = (
    spark.read.format("bigquery")
    .option("project", project_trusted)
    .option("dataset", dataset_trusted)
    .load(table_trusted)
)

df_refined = df_trusted.select("empresa","ciudad","pais","fecha_suscripcion")

df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Albania', 'United States of America'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Algeria', 'United States of America'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Andorra', 'United States of America'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Angola', 'United States of America'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Armenia', 'United States of America'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Bahamas', 'Canada'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Belarus', 'Canada'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Bhutan', 'Canada'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Benin', 'Mexico'))
df_refined = df_refined.withColumn('pais', regexp_replace('pais', 'Bermuda', 'Mexico'))
df_refined = df_refined.withColumn('empresa', regexp_replace('empresa', '^M.*', 'Liverpool'))
df_refined = df_refined.withColumn('empresa', regexp_replace('empresa', '^C.*', 'Coppel'))
df_refined = df_refined.withColumn('empresa', regexp_replace('empresa', '^B.*', 'Banco Azteca'))
df_refined = df_refined.withColumn('empresa', regexp_replace('empresa', '^E.*', 'Electra'))
df_refined = df_refined.withColumn('empresa', regexp_replace('empresa', '^N.*', 'BanCoppel'))
df_refined = df_refined.withColumn('empresa', regexp_replace('empresa', '^P.*', 'BanCoppel'))

df_refined = df_refined.where((col('pais') == 'United States of America') | 
                            (col('pais') == 'Canada') | 
                            (col('pais') == 'Mexico'))
df_refined = df_refined.where((col('empresa') == 'Liverpool') | 
                            (col('empresa') == 'Coppel') | 
                            (col('empresa') == 'Banco Azteca') |
                            (col('empresa') == 'Electra') |
                            (col('empresa') == 'BanCoppel'))

df_refined = df_refined.groupBy("empresa","ciudad","pais","fecha_suscripcion").count().withColumnRenamed("count","clientes")

df_refined.show(10, truncate = False)
df_refined.printSchema()

print('******************** GUARDANDO DATOS EN REFINED ... ************************') 
df_refined.write.format("bigquery").mode('overwrite') \
    .option("project", project_trusted)\
    .option("dataset", dataset_refined)\
    .option("table", table_refined)\
    .save()

