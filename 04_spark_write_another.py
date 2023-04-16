from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder \
    .appName("Nessie-Iceberg-Example") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0") \
    .getOrCreate()


conf = {
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri": "http://127.0.0.1:19120/api/v1",
    "spark.sql.catalog.nessie.ref": "modified",
    "spark.sql.catalog.nessie.warehouse": "file:///tmp/nessie_warehouse"
}

for k, v in conf.items():
    spark.conf.set(k, v)

original_df = spark.read \
    .format("iceberg") \
    .load("nessie.public.test_table_1")

modified_df = original_df.withColumn("city", lit("unknown"))

modified_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable("nessie.public.test_table_1")

original_df = spark.read \
    .format("iceberg") \
    .load("nessie.public.test_table_1")

original_df.show()


