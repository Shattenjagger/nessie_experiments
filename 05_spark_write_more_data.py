from pyspark.sql import SparkSession
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

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [(4, "Jack", 34, "Moscow")]
df = spark.createDataFrame(data, schema)

df.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("nessie.public.test_table_1")

original_df = spark.read \
    .format("iceberg") \
    .load("nessie.public.test_table_1")

original_df.show()