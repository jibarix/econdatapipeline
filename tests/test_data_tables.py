import os
from azure.data.tables import TableServiceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve connection string from environment
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

def test_azure_connection_and_print_tables():
    if not AZURE_STORAGE_CONNECTION_STRING:
        print("Azure connection string not found in environment variables.")
        return

    try:
        service = TableServiceClient.from_connection_string(conn_str=AZURE_STORAGE_CONNECTION_STRING)
        tables = service.list_tables()
        print("Available tables in Azure Storage:")
        for table in tables:
            print(f"- {table.name}")
    except Exception as e:
        print(f"Error connecting to Azure Table Storage: {e}")

if __name__ == "__main__":
    test_azure_connection_and_print_tables()