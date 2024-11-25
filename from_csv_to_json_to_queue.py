import pandas as pd
import os
from azure.storage.queue import QueueServiceClient
from dotenv import load_dotenv
import json

# Ladda variabler från .env-filen
load_dotenv()

# Hämta variabler
connection_string = os.getenv('AZURE_CONNECTION_STRING')  # Kontrollera att namnet är korrekt
queue_name = "assignment3markus"

# Skapa en QueueServiceClient
queue_service_client = QueueServiceClient.from_connection_string(connection_string)

# Hämta köreferensen
queue_client = queue_service_client.get_queue_client(queue_name)

# Ange sökvägen till CSV-filen
csv_file_path = r'C:\Users\marku\OneDrive\Skrivbord\csv\matcheslatest.csv'

# Kontrollera om filen finns
if not os.path.exists(csv_file_path):
    print(f"Filen '{csv_file_path}' hittades inte. Kontrollera sökvägen.")
    exit(1)

# Läs CSV-filen
try:
    data = pd.read_csv(csv_file_path)
    print("CSV-filen har lästs in framgångsrikt.")
except Exception as e:
    print(f"Ett fel uppstod vid läsning av CSV-filen: {e}")
    exit(1)

# Konvertera datan till JSON-format och skicka till kön
for index, row in data.iterrows():
    message = row.to_json()  # Omvandla raden till JSON-format
    try:
        queue_client.send_message(message)  # Skicka meddelandet till kön
        print(f"Meddelande skickat: {message}")  # Bekräftelse på skickat meddelande
    except Exception as e:
        print(f"Ett fel uppstod vid skickande av meddelande till kön: {e}")

print("Alla meddelanden har skickats till Azure Storage Queue.")
