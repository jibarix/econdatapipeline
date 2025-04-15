"""
Azure connector for economic data pipeline.
This module handles interactions with Azure Table Storage and Blob Storage,
replacing the Supabase functionality in the original implementation.
"""
import os
import logging
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from io import BytesIO

# Azure SDK imports
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.data.tables import TableServiceClient, TableClient, UpdateMode, EntityProperty
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

logger = logging.getLogger(__name__)

class AzureConnector:
    """
    Azure connector for handling interactions with Azure services.
    This class provides methods for accessing Azure Table Storage and Blob Storage.
    """
    
    def __init__(self, use_managed_identity: bool = True, key_vault_url: Optional[str] = None):
        """
        Initialize the Azure connector.
        
        Args:
            use_managed_identity: Whether to use managed identity for authentication.
                                 Set to False when running locally.
            key_vault_url: URL of the Azure Key Vault. Required if secrets are being accessed.
        """
        self.use_managed_identity = use_managed_identity
        self.key_vault_url = key_vault_url
        
        # Initialize Azure clients
        self._initialize_storage_clients()
        
        # Initialize Key Vault client if URL is provided
        self.secret_client = None
        if key_vault_url:
            self._initialize_key_vault_client()
    
    def _initialize_storage_clients(self):
        """Initialize Azure Storage clients."""
        try:
            # Choose authentication method based on environment
            if self.use_managed_identity:
                # Use managed identity in Azure environment
                credential = ManagedIdentity()
                # Get storage account name from environment variable or a default
                storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", "econdatastorage")
                # Construct the Table and Blob service URLs
                table_service_url = f"https://{storage_account}.table.core.windows.net/"
                blob_service_url = f"https://{storage_account}.blob.core.windows.net/"
                
                # Create table service client
                self.table_service = TableServiceClient(endpoint=table_service_url, credential=credential)
                # Create blob service client
                self.blob_service = BlobServiceClient(account_url=blob_service_url, credential=credential)
            else:
                # Use connection string for local development
                conn_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
                if not conn_string:
                    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required when not using managed identity")
                
                # Create table service client using connection string
                self.table_service = TableServiceClient.from_connection_string(conn_string)
                # Create blob service client using connection string
                self.blob_service = BlobServiceClient.from_connection_string(conn_string)
                
            logger.info("Azure Storage clients initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Azure Storage clients: {e}")
            raise
    
    def _initialize_key_vault_client(self):
        """Initialize Azure Key Vault client."""
        try:
            # Choose authentication method based on environment
            if self.use_managed_identity:
                credential = ManagedIdentity()
            else:
                credential = DefaultAzureCredential()
            
            # Create Key Vault client
            self.secret_client = SecretClient(vault_url=self.key_vault_url, credential=credential)
            logger.info("Azure Key Vault client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Azure Key Vault client: {e}")
            raise
    
    def get_secret(self, secret_name: str) -> str:
        """
        Get a secret from Azure Key Vault.
        
        Args:
            secret_name: The name of the secret to retrieve
            
        Returns:
            The secret value
        """
        if not self.secret_client:
            raise ValueError("Key Vault client not initialized. Provide key_vault_url when creating AzureConnector.")
        
        try:
            secret = self.secret_client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logger.error(f"Failed to retrieve secret '{secret_name}': {e}")
            raise
    
    # Table Storage Methods
    
    def create_table(self, table_name: str) -> bool:
        """
        Create a table if it doesn't exist.
        
        Args:
            table_name: Name of the table to create
            
        Returns:
            True if table was created or already exists, False otherwise
        """
        try:
            self.table_service.create_table(table_name)
            logger.info(f"Table '{table_name}' created successfully")
            return True
        except ResourceExistsError:
            logger.info(f"Table '{table_name}' already exists")
            return True
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            return False
    
    def delete_table(self, table_name: str) -> bool:
        """
        Delete a table if it exists.
        
        Args:
            table_name: Name of the table to delete
            
        Returns:
            True if table was deleted, False otherwise
        """
        try:
            self.table_service.delete_table(table_name)
            logger.info(f"Table '{table_name}' deleted successfully")
            return True
        except ResourceNotFoundError:
            logger.info(f"Table '{table_name}' does not exist")
            return False
        except Exception as e:
            logger.error(f"Failed to delete table '{table_name}': {e}")
            return False
    
    def upsert_entity(self, table_name: str, entity: Dict[str, Any]) -> bool:
        """
        Insert or update an entity in a table.
        
        Args:
            table_name: Name of the table
            entity: Entity dictionary including PartitionKey and RowKey
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get table client
            table_client = self.table_service.get_table_client(table_name)
            
            # Upsert entity
            table_client.upsert_entity(entity)
            return True
        except Exception as e:
            logger.error(f"Failed to upsert entity in table '{table_name}': {e}")
            return False
    
    def batch_upsert(self, table_name: str, entities: List[Dict[str, Any]]) -> bool:
        """
        Insert or update multiple entities in a table using batches.
        
        Args:
            table_name: Name of the table
            entities: List of entity dictionaries, each including PartitionKey and RowKey
            
        Returns:
            True if all operations were successful, False otherwise
        """
        try:
            # Get table client
            table_client = self.table_service.get_table_client(table_name)
            
            # Azure Tables supports a maximum of 100 operations per batch
            batch_size = 100
            success = True
            
            # Process entities in batches
            for i in range(0, len(entities), batch_size):
                batch = entities[i:i+batch_size]
                operations = [("upsert", entity) for entity in batch]
                
                try:
                    table_client.submit_transaction(operations)
                except Exception as batch_error:
                    logger.error(f"Batch operation failed: {batch_error}")
                    success = False
            
            return success
        except Exception as e:
            logger.error(f"Failed to batch upsert entities in table '{table_name}': {e}")
            return False
    
    def get_entity(self, table_name: str, partition_key: str, row_key: str) -> Optional[Dict[str, Any]]:
        """
        Get an entity from a table.
        
        Args:
            table_name: Name of the table
            partition_key: Partition key of the entity
            row_key: Row key of the entity
            
        Returns:
            Entity dictionary or None if not found
        """
        try:
            # Get table client
            table_client = self.table_service.get_table_client(table_name)
            
            # Get entity
            entity = table_client.get_entity(partition_key, row_key)
            return dict(entity)
        except ResourceNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to get entity from table '{table_name}': {e}")
            return None
    
    def query_entities(self, table_name: str, query_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Query entities from a table.
        
        Args:
            table_name: Name of the table
            query_filter: Optional filter string (e.g. "PartitionKey eq 'auto_sales'")
            
        Returns:
            List of entity dictionaries
        """
        try:
            # Get table client
            table_client = self.table_service.get_table_client(table_name)
            
            # Query entities
            entities = table_client.query_entities(query_filter)
            
            # Convert to list of dictionaries
            return [dict(entity) for entity in entities]
        except Exception as e:
            logger.error(f"Failed to query entities from table '{table_name}': {e}")
            return []
    
    def delete_entity(self, table_name: str, partition_key: str, row_key: str) -> bool:
        """
        Delete an entity from a table.
        
        Args:
            table_name: Name of the table
            partition_key: Partition key of the entity
            row_key: Row key of the entity
            
        Returns:
            True if successful or entity doesn't exist, False otherwise
        """
        try:
            # Get table client
            table_client = self.table_service.get_table_client(table_name)
            
            # Delete entity
            table_client.delete_entity(partition_key, row_key)
            return True
        except ResourceNotFoundError:
            # Entity doesn't exist, which is fine
            return True
        except Exception as e:
            logger.error(f"Failed to delete entity from table '{table_name}': {e}")
            return False
    
    # Blob Storage Methods
    
    def create_container(self, container_name: str) -> bool:
        """
        Create a blob container if it doesn't exist.
        
        Args:
            container_name: Name of the container to create
            
        Returns:
            True if container was created or already exists, False otherwise
        """
        try:
            self.blob_service.create_container(container_name)
            logger.info(f"Container '{container_name}' created successfully")
            return True
        except ResourceExistsError:
            logger.info(f"Container '{container_name}' already exists")
            return True
        except Exception as e:
            logger.error(f"Failed to create container '{container_name}': {e}")
            return False
    
    def upload_blob(self, container_name: str, blob_name: str, data: Union[bytes, BytesIO, str]) -> bool:
        """
        Upload data to a blob.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob
            data: The data to upload (bytes, BytesIO, or string)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get container client
            container_client = self.blob_service.get_container_client(container_name)
            
            # Upload data
            blob_client = container_client.get_blob_client(blob_name)
            
            if isinstance(data, str):
                blob_client.upload_blob(data, overwrite=True)
            else:
                blob_client.upload_blob(data, overwrite=True)
                
            logger.info(f"Uploaded blob '{blob_name}' to container '{container_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to upload blob '{blob_name}' to container '{container_name}': {e}")
            return False
    
    def download_blob(self, container_name: str, blob_name: str) -> Optional[bytes]:
        """
        Download data from a blob.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob
            
        Returns:
            Blob data as bytes or None if not found
        """
        try:
            # Get container client
            container_client = self.blob_service.get_container_client(container_name)
            
            # Download data
            blob_client = container_client.get_blob_client(blob_name)
            download = blob_client.download_blob()
            
            return download.readall()
        except ResourceNotFoundError:
            logger.warning(f"Blob '{blob_name}' not found in container '{container_name}'")
            return None
        except Exception as e:
            logger.error(f"Failed to download blob '{blob_name}' from container '{container_name}': {e}")
            return None
    
    def list_blobs(self, container_name: str, name_starts_with: Optional[str] = None) -> List[str]:
        """
        List blobs in a container.
        
        Args:
            container_name: Name of the container
            name_starts_with: Optional prefix to filter blobs
            
        Returns:
            List of blob names
        """
        try:
            # Get container client
            container_client = self.blob_service.get_container_client(container_name)
            
            # List blobs
            blobs = container_client.list_blobs(name_starts_with=name_starts_with)
            
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list blobs in container '{container_name}': {e}")
            return []
    
    def delete_blob(self, container_name: str, blob_name: str) -> bool:
        """
        Delete a blob.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get container client
            container_client = self.blob_service.get_container_client(container_name)
            
            # Delete blob
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            
            logger.info(f"Deleted blob '{blob_name}' from container '{container_name}'")
            return True
        except ResourceNotFoundError:
            # Blob doesn't exist, which is fine
            return True
        except Exception as e:
            logger.error(f"Failed to delete blob '{blob_name}' from container '{container_name}': {e}")
            return False
    
    # Economic Data Specific Methods
    
    def initialize_tables(self) -> bool:
        """
        Initialize all required tables for the economic data pipeline.
        
        Returns:
            True if all tables were created successfully, False otherwise
        """
        tables = [
            # Economic indicator tables
            "auto_sales", "bankruptcies", "cement_production", "electricity_consumption",
            "gas_price", "gas_consumption", "labor_participation", "unemployment_rate",
            "employment_rate", "unemployment_claims", "trade_employment", "consumer_price_index",
            "transportation_price_index", "retail_sales", "imports",
            # FRED data tables
            "federal_funds_rate", "auto_manufacturing_orders", "used_car_retail_sales",
            "domestic_auto_inventories", "domestic_auto_production", "liquidity_credit_facilities",
            "semiconductor_manufacturing_units", "aluminum_new_orders", "real_gdp", "gdp_now_forecast",
            # NYU Stern data table
            "equity_risk_premium",
            # Metadata and revision tracking tables
            "datarevisions", "scrapermetadata"
        ]
        
        success = True
        for table_name in tables:
            if not self.create_table(table_name):
                success = False
        
        return success
    
    def initialize_containers(self) -> bool:
        """
        Initialize all required blob containers for the economic data pipeline.
        
        Returns:
            True if all containers were created successfully, False otherwise
        """
        containers = [
            "raw-files",  # For storing original downloaded files
            "exports",     # For storing exported data files
            "logs"         # For storing log files
        ]
        
        success = True
        for container_name in containers:
            if not self.create_container(container_name):
                success = False
        
        return success
    
    def dataset_to_table(self, table_name: str, data_df: pd.DataFrame, 
                          partition_key_field: str, row_key_field: str,
                          date_field: Optional[str] = None) -> bool:
        """
        Convert a pandas DataFrame to Azure Table entities and insert them.
        
        Args:
            table_name: Name of the Azure table
            data_df: DataFrame with the data
            partition_key_field: Field to use as partition key (usually dataset name)
            row_key_field: Field to use as row key (usually date or ID)
            date_field: Optional field to format as date string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Make a copy to avoid modifying the original
            df = data_df.copy()
            
            # Format date field if specified
            if date_field and date_field in df.columns:
                df[date_field] = pd.to_datetime(df[date_field]).dt.strftime('%Y-%m-%d')
            
            # Convert DataFrame to list of entities
            entities = []
            for _, row in df.iterrows():
                # Convert row to dictionary
                entity = row.to_dict()
                
                # Set partition key and row key
                entity["PartitionKey"] = str(entity.pop(partition_key_field) if partition_key_field in entity else table_name)
                entity["RowKey"] = str(entity.pop(row_key_field))
                
                # Convert all values to strings or numbers (Azure Tables limitation)
                for key, value in entity.items():
                    if pd.isna(value):
                        entity[key] = None
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        entity[key] = value.isoformat()
                
                entities.append(entity)
            
            # Batch upsert entities
            return self.batch_upsert(table_name, entities)
        except Exception as e:
            logger.error(f"Failed to convert dataset to table entities: {e}")
            return False
    
    def table_to_dataframe(self, table_name: str, query_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve data from Azure Table and convert to DataFrame.
        
        Args:
            table_name: Name of the Azure table
            query_filter: Optional filter string
            
        Returns:
            DataFrame with the data
        """
        try:
            # Query entities from table
            entities = self.query_entities(table_name, query_filter)
            
            if not entities:
                return pd.DataFrame()
            
            # Convert list of entities to DataFrame
            df = pd.DataFrame(entities)
            
            # Convert 'Timestamp' column to datetime if it exists
            if 'Timestamp' in df.columns:
                df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            
            return df
        except Exception as e:
            logger.error(f"Failed to convert table to DataFrame: {e}")
            return pd.DataFrame()
    
    def update_last_run(self, dataset_name: str) -> bool:
        """
        Update timestamp of last scraper run.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            True if successful, False otherwise
        """
        try:
            timestamp = datetime.utcnow().isoformat()
            
            entity = {
                "PartitionKey": "dataset",
                "RowKey": dataset_name,
                "last_run": timestamp
            }
            
            return self.upsert_entity("scrapermetadata", entity)
        except Exception as e:
            logger.error(f"Failed to update last run for dataset '{dataset_name}': {e}")
            return False
    
    def get_last_run(self, dataset_name: str) -> Optional[datetime]:
        """
        Get timestamp of last scraper run.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Timestamp of last run or None if not found
        """
        try:
            entity = self.get_entity("scrapermetadata", "dataset", dataset_name)
            
            if entity and "last_run" in entity:
                from dateutil import parser
                return parser.parse(entity["last_run"])
            
            return None
        except Exception as e:
            logger.error(f"Failed to get last run for dataset '{dataset_name}': {e}")
            return None
    
    def should_update(self, dataset_name: str, update_frequency_hours: int = 24) -> bool:
        """
        Check if dataset should be updated based on last update time.
        
        Args:
            dataset_name: Name of the dataset
            update_frequency_hours: Minimum hours between updates
            
        Returns:
            True if dataset should be updated, False otherwise
        """
        last_run = self.get_last_run(dataset_name)
        
        if not last_run:
            return True
        
        now = datetime.utcnow()
        hours_since_update = (now - last_run).total_seconds() / 3600
        
        return hours_since_update >= update_frequency_hours
    
    def track_revision(self, dataset: str, data_date: str, value_field: str, 
                       old_value: float, new_value: float) -> bool:
        """
        Track a data revision.
        
        Args:
            dataset: Name of the dataset (table)
            data_date: Date of the data point that was revised
            value_field: Name of the value field that was revised
            old_value: Previous value
            new_value: New value
            
        Returns:
            True if successful, False otherwise
        """
        try:
            revision_date = datetime.utcnow().isoformat()
            row_key = f"{dataset}_{data_date}_{value_field}_{revision_date}"
            
            entity = {
                "PartitionKey": dataset,
                "RowKey": row_key,
                "dataset": dataset,
                "data_date": data_date,
                "value_field": value_field,
                "old_value": old_value,
                "new_value": new_value,
                "revision_date": revision_date
            }
            
            return self.upsert_entity("datarevisions", entity)
        except Exception as e:
            logger.error(f"Failed to track revision: {e}")
            return False
    
    def get_revision_history(self, dataset: str, date: Optional[str] = None, 
                             field: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get revision history for a dataset or specific data point.
        
        Args:
            dataset: The dataset name
            date: Optional specific date to filter by
            field: Optional specific field to filter by
            limit: Optional maximum number of revisions to return
            
        Returns:
            DataFrame with revision history
        """
        try:
            # Build filter string
            filter_parts = [f"PartitionKey eq '{dataset}'"]
            
            if date:
                filter_parts.append(f"data_date eq '{date}'")
            
            if field:
                filter_parts.append(f"value_field eq '{field}'")
            
            query_filter = " and ".join(filter_parts)
            
            # Query revisions
            df = self.table_to_dataframe("datarevisions", query_filter)
            
            if df.empty:
                return pd.DataFrame(columns=[
                    'PartitionKey', 'RowKey', 'dataset', 'data_date', 'value_field', 
                    'old_value', 'new_value', 'revision_date'
                ])
            
            # Sort by revision date (descending)
            if 'revision_date' in df.columns:
                df['revision_date'] = pd.to_datetime(df['revision_date'])
                df = df.sort_values('revision_date', ascending=False)
            
            # Limit results if specified
            if limit and len(df) > limit:
                df = df.head(limit)
            
            return df
        except Exception as e:
            logger.error(f"Failed to get revision history: {e}")
            return pd.DataFrame()