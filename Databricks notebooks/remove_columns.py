df = spark.table("hive_metastore.default.azure_queue_data_transformed")
display(df)
# Ta bort kolumnerna 'column1', 'column2' och 'column3' från DataFrame
df = df.drop("opp_formation", "match_report", "notes")
display(df)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("default.azure_queue_data_transformed")