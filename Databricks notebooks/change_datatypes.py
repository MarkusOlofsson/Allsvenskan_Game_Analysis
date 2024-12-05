df = spark.read.table("hive_metastore.default.azure_queue_data_transformed")

##----


from pyspark.sql.functions import col

# Konvertera kolumnen 'date' till datatypen 'date'
df_transformed = df.withColumn("date", col("date").cast("date"))

##----

# Skriv tillbaka till tabellen med den nya datatypen för 'date'
df_transformed.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("hive_metastore.default.azure_queue_data_transformed")

