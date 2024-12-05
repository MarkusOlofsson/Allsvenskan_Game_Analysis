%python
%pip install azure-storage-queue
from azure.storage.queue import QueueServiceClient
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Ange din Azure Queue-anslutningssträng och kö-namn
connection_string = ""  # Ersätt med din anslutningssträng
queue_name = "assignment3markus"  # Ersätt med ditt kö-namn

# Anslut till Azure Queue
queue_service_client = QueueServiceClient.from_connection_string(connection_string)
queue_client = queue_service_client.get_queue_client(queue_name)

# Hämta meddelanden från kön och omvandla dem till en lista med dictionaries
messages = []
for message in queue_client.receive_messages(messages_per_page=10):  # Justera antalet vid behov
    content = message.content
    try:
        message_dict = json.loads(content)
        messages.append(message_dict)
        queue_client.delete_message(message)  # Ta bort meddelandet från kön
    except json.JSONDecodeError:
        print("Meddelandet kunde inte omvandlas från JSON.")
    except Exception as e:
        print(f"Ett fel uppstod: {e}")

# Omvandla listan med dictionaries till en Spark DataFrame
# Definiera ett schema baserat på datans struktur för att undvika fel
schema = StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("comp", StringType(), True),
    StructField("round", StringType(), True),
    StructField("day", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("result", StringType(), True),
    StructField("gf", IntegerType(), True),
    StructField("ga", IntegerType(), True),
    StructField("opponent", StringType(), True),
    StructField("poss", FloatType(), True),
    StructField("attendance", FloatType(), True),
    StructField("captain", StringType(), True),
    StructField("formation", StringType(), True),
    StructField("opp_formation", StringType(), True),
    StructField("referee", StringType(), True),
    StructField("match_report", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("sh", FloatType(), True),
    StructField("sot", FloatType(), True),
    StructField("pk", IntegerType(), True),
    StructField("pkatt", IntegerType(), True),
    StructField("season", IntegerType(), True),
    StructField("team", StringType(), True)
])

# Skapa Spark DataFrame från listan med meddelanden
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(messages, schema=schema)

# Spara Spark DataFrame som en Delta-tabell i Databricks-katalogen
table_name = "azure_queue_data"  # Ange ett namn för tabellen
spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# Bekräfta att tabellen skapades korrekt genom att visa de första raderna
spark.sql(f"SELECT * FROM {table_name}").show()
