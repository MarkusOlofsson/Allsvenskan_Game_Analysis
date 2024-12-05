%sql
drop table hive_metastore.default.football_data_from_storage
%sql
drop table hive_metastore.default.football_matches_analysis
%sql
drop table hive_metastore.default.matches
%sql
select * from azure_queue_data limit 300
%python
#läsa från en tabell i en katalog (t.ex. Delta Lake)
df = spark.table("azure_queue_data")

# Anta att df är din DataFrame och du vill ta bort kolumnerna "kolumn1" och "kolumn2"
df_cleaned = df.drop("notes", "dist")

df.head(5)
from pyspark.sql.functions import when

# Lägg till en kolumn 'points' som ger 3 poäng för vinst, 1 för oavgjort och 0 för förlust
df = df.withColumn("points", when(df["result"] == "W", 3)  # Vinst ger 3 poäng
                                   .when(df["result"] == "D", 1)  # Oavgjort ger 1 poäng
                                   .otherwise(0))  # Förlust ger 0 poäng

from pyspark.sql.window import Window
from pyspark.sql.functions import avg, when

# Lägg till en kolumn 'points' för att beräkna poäng (3 för vinst, 1 för oavgjort, 0 för förlust)
df = df.withColumn("points", when(df["result"] == "W", 3)
                                   .when(df["result"] == "D", 1)
                                   .otherwise(0))

# Skapa ett fönster som grupperar per lag och sorterar på datum
window_spec = Window.partitionBy("team").orderBy("date").rowsBetween(-4, 0)

# Lägg till en kolumn 'form_index' som är ett rullande genomsnitt av poängen de senaste 5 matcherna
df = df.withColumn("form_index", avg("points").over(window_spec))

# Nu har vi en kolumn 'form_index' som innehåller det rullande genomsnittet av poängen för varje lag

# Beräkna målskillnad (mål gjorda - mål släppta)
df = df.withColumn("goal_difference", df["gf"] - df["ga"])

from pyspark.sql.functions import avg

# Hemmaplanseffekter
home_performance = df.filter(df["venue"] == "Home") \
    .groupBy("team") \
    .agg(
        avg("gf").alias("avg_home_goals"),  # Genomsnittliga mål gjorda hemma
        avg("points").alias("avg_home_points")  # Genomsnittliga poäng hemma
    )

# Bortaplanseffekter
away_performance = df.filter(df["venue"] == "Away") \
    .groupBy("team") \
    .agg(
        avg("gf").alias("avg_away_goals"),  # Genomsnittliga mål gjorda borta
        avg("points").alias("avg_away_points")  # Genomsnittliga poäng borta
    )

# Kombinera hemmaplan och bortaplan data
performance = home_performance.join(
    away_performance, on="team", how="outer"
)

# Visa resultatet
performance.show()

df.head(5)
# Skriv tillbaka den transformerade DataFrame till en ny tabell i katalogen
df.write.format("delta").mode("overwrite").option("path", "<path>").saveAsTable("default.azure_queue_data_transformed")
