from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession.builder.appName("proyectoFinal1").master("local[1]").config("spark.streaming.stopGracefullyOnShutdown", "true").config("spark.sql.shuffle.partitions", 2).getOrCreate()

cities_df = spark.read.csv("cities.csv",inferSchema = True,sep = ";",header = True)
cities_df.show()
deparments_df = spark.read.csv("departments.csv",inferSchema = True,sep = ";",header = True)
deparments_df.show()
citydep_df = cities_df.join(deparments_df, expr("Cod_Departamento == Codigo"), "inner")
citydep_df.show()

kafka_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "final1") \
  .load()

#print(kafka_df.show())

dish_schema = StructType([
	StructField("dish",StringType()),
	StructField("hour",StringType()),
	StructField("codCity",IntegerType()),
	StructField("amount",IntegerType())
	])
#print("value")
value_df = kafka_df.select(from_json(col("value").cast("string"), dish_schema).alias("value"))
#print(value_df.show())

transformed_df = value_df.select("value.*").withColumn("dish",col("dish")).withColumn("CreatedTime", to_timestamp(col("hour"), "HH:mm:ss")).withColumn("codCity",col("codCity")).withColumn("amount", col("amount"))
joined_df = citydep_df.join(transformed_df, expr("Cod_Ciudad == codCity"),"inner")
window_agg = joined_df.withWatermark("CreatedTime", "1 hour").groupBy(col("Nom_Departamento"),window(col("CreatedTime"), "1 hour", "15 minute"),col("dish")).agg(sum("amount").alias("TotalAmount"))

output_df = window_agg.select("Nom_Departamento","window.start", "window.end", "dish", "TotalAmount")

window_query = output_df.writeStream.format("console").outputMode("update").option("checkpointLocation", "./checkpoint/tumbling-window").trigger(processingTime="15 second").start()

window_query.awaitTermination()


spark.stop()