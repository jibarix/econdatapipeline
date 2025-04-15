from azure.data.tables import TableServiceClient
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
table_name = "federalfundsrate"

# Connect to the service and table
service = TableServiceClient.from_connection_string(conn_str=AZURE_STORAGE_CONNECTION_STRING)
table_client = service.get_table_client(table_name=table_name)

# List all tables for sanity check
print("Available tables:")
for t in service.list_tables():
    print("-", t.name)
print("\nEntities in table:", table_name)

# List entities
entities = list(table_client.list_entities())

print(f"\nTotal entities found: {len(entities)}")

# Preview the first few entities
for i, entity in enumerate(entities[:5]):
    print(f"\nEntity {i+1}:")
    for k, v in entity.items():
        print(f"  {k}: {v}")