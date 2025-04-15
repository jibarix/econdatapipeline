"""
Consolidated module for economic data pipeline on Azure Automation.
This file combines all necessary components to simplify imports in Azure Automation.
"""

import os
import logging
import json
import pandas as pd
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from io import BytesIO

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('economic_data_pipeline')

# Azure SDK imports
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.data.tables import TableServiceClient, TableClient, UpdateMode, EntityProperty
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

#########################
# Azure Connector Class #
#########################

class AzureConnector:
    """
    Azure connector for handling interactions with Azure services.
    This class provides methods for accessing Azure Table Storage and Blob Storage.
    """
    
    def __init__(self, use_managed_identity: bool = True, key_vault_url: Optional[str] = None, 
                 storage_account: Optional[str] = None):
        """
        Initialize the Azure connector.
        
        Args:
            use_managed_identity: Whether to use managed identity for authentication.
                                 Set to False when running locally.
            key_vault_url: URL of the Azure Key Vault. Required if secrets are being accessed.
            storage_account: Name of the storage account. If not provided, will use environment variable.
        """
        self.use_managed_identity = use_managed_identity
        self.key_vault_url = key_vault_url
        self.storage_account = storage_account or os.environ.get("AZURE_STORAGE_ACCOUNT", "econdatastorage")
        
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
                logger.info("Using Managed Identity for authentication")
                credential = ManagedIdentityCredential()
                
                # Construct the Table and Blob service URLs
                table_service_url = f"https://{self.storage_account}.table.core.windows.net/"
                blob_service_url = f"https://{self.storage_account}.blob.core.windows.net/"
                
                # Create table service client
                self.table_service = TableServiceClient(endpoint=table_service_url, credential=credential)
                # Create blob service client
                self.blob_service = BlobServiceClient(account_url=blob_service_url, credential=credential)
            else:
                # Use connection string for local development
                logger.info("Using connection string for authentication")
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
                credential = ManagedIdentityCredential()
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
            "autosales", "bankruptcies", "cementproduction", "electricityconsumption",
            "gasprice", "gasconsumption", "laborparticipation", "unemploymentrate",
            "employmentrate", "unemploymentclaims", "tradeemployment", "consumerpriceindex",
            "transportationpriceindex", "retailsales", "imports",
            "federalfundsrate", "automanufacturingorders", "usedcarretailsales",
            "domesticautoinventories", "domesticautoproduction", "liquiditycreditfacilities",
            "semiconductormanufacturingunits", "aluminumneworders", "realgdp", "gdpnowforecast",
            "equityriskpremium",
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

##########################
# Data Tracker Functions #
##########################

def initialize_revision_tracking(azure_connector: AzureConnector) -> bool:
    """
    Initialize the data revision tracking table in Azure.
    
    Args:
        azure_connector: AzureConnector instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    return azure_connector.create_table("datarevisions")

def smart_update(azure_connector: AzureConnector, dataset_name: str, 
                data_df: pd.DataFrame, date_field: str, value_fields: List[str]) -> Dict[str, int]:
    """
    Smart insert/update that tracks revisions.
    
    Args:
        azure_connector: AzureConnector instance
        dataset_name: Name of the dataset (table)
        data_df: DataFrame with processed data
        date_field: Name of the date column
        value_fields: List of value columns to track
        
    Returns:
        Dictionary with counts of new, updated, and revision records
    """
    if data_df.empty:
        logger.warning(f"Empty DataFrame provided for {dataset_name}. No update performed.")
        return {"new": 0, "updated": 0, "revisions": 0}
        
    # Ensure date column is properly formatted
    data_df[date_field] = pd.to_datetime(data_df[date_field]).dt.strftime('%Y-%m-%d')
    
    # Create the table if it doesn't exist
    if not azure_connector.create_table(dataset_name):
        logger.error(f"Failed to create or access table {dataset_name}")
        return {"new": 0, "updated": 0, "revisions": 0}
    
    # Get existing data for comparison
    existing_data = {}
    
    # Query all entities for this dataset
    # Note: In a production system with large datasets, you might want to filter by date range
    existing_entities = azure_connector.query_entities(dataset_name)
    
    # Convert to dictionary indexed by date for easier comparison
    for entity in existing_entities:
        if date_field in entity:
            existing_data[entity[date_field]] = entity
    
    # Track new, updated, and unchanged records
    new_records = []
    updates = []
    revisions = []
    
    # Compare each record
    for _, row in data_df.iterrows():
        record_dict = row.to_dict()
        record_date = record_dict[date_field]
        
        # Set partition and row keys for Azure Tables
        entity = {
            "PartitionKey": dataset_name,
            "RowKey": record_date,
        }
        
        # Add all fields from the row
        for field, value in record_dict.items():
            entity[field] = value
        
        if record_date not in existing_data:
            # New record
            new_records.append(entity)
            continue
            
        # Check for value changes
        existing_row = existing_data[record_date]
        record_changed = False
        
        for field in value_fields:
            # Skip if either value is NaN
            if field not in record_dict or field not in existing_row:
                continue
                
            new_value = record_dict[field]
            old_value = existing_row[field]
            
            # Handle NaN values
            if pd.isna(new_value) or pd.isna(old_value):
                continue
                
            # Convert to float for comparison
            try:
                new_value_float = float(new_value)
                old_value_float = float(old_value)
                
                # Check if value changed (allow small float precision diffs)
                if abs(new_value_float - old_value_float) > 0.001:
                    record_changed = True
                    
                    # Track revision
                    revision = {
                        "PartitionKey": dataset_name,
                        "RowKey": f"{record_date}_{field}_{datetime.utcnow().isoformat()}",
                        "dataset": dataset_name,
                        "data_date": record_date,
                        "value_field": field,
                        "old_value": old_value_float,
                        "new_value": new_value_float,
                        "revision_date": datetime.utcnow().isoformat()
                    }
                    revisions.append(revision)
            except (ValueError, TypeError):
                # Skip if conversion to float fails
                continue
        
        if record_changed:
            updates.append(entity)
    
    # Execute storage operations
    results = {"new": 0, "updated": 0, "revisions": 0}
    
    # Insert new records
    if new_records:
        logger.info(f"Inserting {len(new_records)} new records for {dataset_name}")
        if azure_connector.batch_upsert(dataset_name, new_records):
            results["new"] = len(new_records)
        else:
            logger.error(f"Failed to insert new records for {dataset_name}")
    
    # Update changed records
    if updates:
        logger.info(f"Updating {len(updates)} changed records for {dataset_name}")
        if azure_connector.batch_upsert(dataset_name, updates):
            results["updated"] = len(updates)
        else:
            logger.error(f"Failed to update records for {dataset_name}")
    
    # Record revisions
    if revisions:
        logger.info(f"Recording {len(revisions)} data revisions for {dataset_name}")
        if azure_connector.batch_upsert("datarevisions", revisions):
            results["revisions"] = len(revisions)
        else:
            logger.error(f"Failed to record data revisions for {dataset_name}")
    
    logger.info(f"Smart update complete for {dataset_name}: {results['new']} new, "
                f"{results['updated']} updated, {results['revisions']} revisions tracked")
    
    return results

def get_revision_history(azure_connector: AzureConnector, dataset: str, 
                         date: Optional[str] = None, field: Optional[str] = None, 
                         limit: Optional[int] = None) -> pd.DataFrame:
    """
    Get revision history for a dataset or specific data point.
    
    Args:
        azure_connector: AzureConnector instance
        dataset: The dataset name
        date: Optional specific date to filter by
        field: Optional specific field to filter by
        limit: Optional maximum number of revisions to return
    
    Returns:
        DataFrame with revision history
    """
    # Build filter string
    filter_parts = [f"PartitionKey eq '{dataset}'"]
    
    if date:
        filter_parts.append(f"data_date eq '{date}'")
    
    if field:
        filter_parts.append(f"value_field eq '{field}'")
    
    query_filter = " and ".join(filter_parts)
    
    # Query revisions
    revisions = azure_connector.query_entities("datarevisions", query_filter)
    
    if not revisions:
        # Return empty dataframe with expected columns
        return pd.DataFrame(columns=[
            'dataset', 'data_date', 'value_field', 
            'old_value', 'new_value', 'revision_date'
        ])
    
    # Convert to DataFrame
    df = pd.DataFrame(revisions)
    
    # Convert date columns to datetime
    if 'data_date' in df.columns:
        df['data_date'] = pd.to_datetime(df['data_date'])
    if 'revision_date' in df.columns:
        df['revision_date'] = pd.to_datetime(df['revision_date'])
    
    # Sort by revision date (most recent first)
    df = df.sort_values('revision_date', ascending=False)
    
    # Apply limit if specified
    if limit and len(df) > limit:
        df = df.head(limit)
    
    # Rename columns for consistency with original implementation
    column_mapping = {
        'PartitionKey': 'dataset',
        'data_date': 'data_date',
        'value_field': 'value_field',
        'old_value': 'old_value',
        'new_value': 'new_value',
        'revision_date': 'revision_date'
    }
    
    # Select and rename columns
    columns_to_keep = [col for col in column_mapping.keys() if col in df.columns]
    df = df[columns_to_keep].rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    return df

#############################
# Common Scraper Base Class #
#############################

class BaseEDBScraper:
    """Base class for Economic Development Bank scrapers"""
    def __init__(self, azure_connector: AzureConnector):
        self.azure = azure_connector

    def create_table(self) -> None:
        """Create database table if it doesn't exist"""
        pass

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw data into a standardized format"""
        pass

    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        pass

    def download_excel(self, url: str, file_name: str) -> Optional[bytes]:
        """Download Excel file from specified URL"""
        try:
            # First check if the file is already in blob storage
            blob_name = file_name
            container_name = "raw-files"
            
            # Ensure container exists
            self.azure.create_container(container_name)
            
            # Try to download from blob storage first
            excel_content = self.azure.download_blob(container_name, blob_name)
            
            if excel_content:
                logger.info(f"Retrieved {file_name} from blob storage")
                return excel_content
            
            # If not in blob storage, download from URL
            response = requests.get(url + file_name)
            response.raise_for_status()
            excel_content = response.content
            
            # Save to blob storage for future use
            self.azure.upload_blob(container_name, blob_name, excel_content)
            logger.info(f"Downloaded {file_name} from URL and saved to blob storage")
            
            return excel_content
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None

    def extract_data(self, excel_content: bytes, sheet_name: str, 
                     data_location: str) -> Optional[pd.DataFrame]:
        """Extract data from specific location in Excel file"""
        try:
            df = pd.read_excel(BytesIO(excel_content), sheet_name=sheet_name, header=None)
            start_cell, end_cell = data_location.split(":")
            start_row = int(start_cell[1:]) - 1
            start_col = ord(start_cell[0].upper()) - ord('A')
            end_row = int(end_cell[1:]) - 1
            end_col = ord(end_cell[0].upper()) - ord('A')
            return df.iloc[start_row:end_row + 1, start_col:end_col + 1]
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            return None

    def update_last_run(self, dataset_name: str) -> None:
        """Update timestamp of last scraper run"""
        self.azure.update_last_run(dataset_name)

    def get_last_run(self, dataset_name: str) -> Optional[datetime]:
        """Get timestamp of last scraper run"""
        return self.azure.get_last_run(dataset_name)

    def should_update(self, dataset_name: str, update_frequency_hours: int = 24) -> bool:
        """Check if dataset should be updated based on last update time"""
        return self.azure.should_update(dataset_name, update_frequency_hours)

##########################
# Monthly Data Scraper #
##########################

class MonthlyDataScraper(BaseEDBScraper):
    """
    Generic scraper for monthly data that follows the common EDB pattern.
    
    This handles data where:
    - Data is organized by months (rows) and fiscal years (columns)
    - First row contains fiscal year headers
    - First column contains month names
    - Data follows the fiscal year pattern (July-June)
    """
    
    def __init__(self, azure_connector: AzureConnector, config: Dict[str, Any]):
        super().__init__(azure_connector)
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        self.azure.create_table(self.table_name)

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw data into standardized format"""
        # Set the fiscal years as column headers
        df.columns = ['Month'] + [int(year) for year in df.iloc[0, 1:]]
        df = df.iloc[1:].reset_index(drop=True)
        
        # Melt the dataframe to transform from wide to long format
        df_melted = pd.melt(df, id_vars=['Month'], var_name='Year', value_name=self.value_column)
        
        # Create dates from month names and fiscal years
        df_melted['Date'] = df_melted.apply(self._create_date, axis=1)
        df_melted = df_melted.dropna(subset=['Date'])
        df_melted = df_melted.sort_values(by='Date').reset_index(drop=True)
        
        # Convert values to the appropriate type
        if self.value_type == 'int':
            df_melted[self.value_column] = pd.to_numeric(df_melted[self.value_column], errors='coerce')
            df_melted = df_melted.dropna(subset=[self.value_column])
            df_melted[self.value_column] = df_melted[self.value_column].round().astype(int)
        else:  # float
            df_melted[self.value_column] = pd.to_numeric(df_melted[self.value_column], errors='coerce')
            df_melted = df_melted.dropna(subset=[self.value_column])
        
        return df_melted[['Date', self.value_column]]
    
    def _create_date(self, row: pd.Series) -> Optional[pd.Timestamp]:
        """
        Create proper dates based on month name and fiscal year.
        
        For Economic Development Bank Puerto Rico data:
        - July-December: use the year before fiscal year
        - January-June: use the same year as fiscal year
        """
        month_mapping = {
            'July': 7, 'August': 8, 'September': 9, 'October': 10,
            'November': 11, 'December': 12, 'January': 1, 'February': 2,
            'March': 3, 'April': 4, 'May': 5, 'June': 6
        }
        month_num = month_mapping.get(row['Month'])
        if not month_num:
            return None
            
        year = int(row['Year'])
        if month_num >= 7:  # July through December
            return pd.to_datetime(f'{year - 1}-{month_num}-01')  # Use year BEFORE fiscal year
        else:  # January through June
            return pd.to_datetime(f'{year}-{month_num}-01')  # Use same year as fiscal year
    
    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        if data.empty:
            logger.warning(f"No data to insert for {self.table_name}")
            return
            
        # Convert column name to lowercase with underscore format
        column_name = ''.join(['_'+i.lower() if i.isupper() else i.lower() for i in self.value_column]).lstrip('_')
        
        # Rename columns to match database schema
        data = data.rename(columns={'Date': 'date', self.value_column: column_name})
        
        # Format date as string for database
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        # Use data tracker's smart update
        smart_update(
            azure_connector=self.azure,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=[column_name]
        )


class QuarterlyDataScraper(BaseEDBScraper):
    """
    Generic scraper for quarterly data that follows the common EDB pattern.
    
    This handles data where:
    - Data is organized by quarters (rows) and fiscal years (columns)
    - First row contains fiscal year headers
    - First column contains quarter names (e.g., "Jul-Sep")
    - Data follows the fiscal year pattern (July-June)
    """
    
    def __init__(self, azure_connector: AzureConnector, config: Dict[str, Any]):
        super().__init__(azure_connector)
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        self.azure.create_table(self.table_name)

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw data into standardized format"""
        # Set the fiscal year as column headers
        fiscal_years = df.iloc[0, 1:].astype(int)
        df.columns = ['Quarter'] + list(fiscal_years)
        
        # Clean up the quarters data
        df = df.iloc[1:].copy()
        df['Quarter'] = df['Quarter'].str.strip()
        
        # Melt the dataframe
        df_melted = pd.melt(df, id_vars=['Quarter'], var_name='Year', value_name=self.value_column)
        
        # Create proper dates
        df_melted['Date'] = df_melted.apply(self._create_date, axis=1)
        
        # Clean and sort
        df_melted = df_melted.dropna(subset=['Date'])
        df_melted[self.value_column] = pd.to_numeric(df_melted[self.value_column], errors='coerce')
        df_melted = df_melted.dropna(subset=[self.value_column])
        df_melted = df_melted.sort_values(by='Date').reset_index(drop=True)
        
        return df_melted[['Date', self.value_column]]
    
    def _create_date(self, row: pd.Series) -> Optional[pd.Timestamp]:
        """
        Create proper dates for quarterly data based on quarter and fiscal year.
        
        For Economic Development Bank Puerto Rico data:
        - Jul-Sep: 1st quarter (Q1) of fiscal year - use first day of next month (Oct 1)
        - Oct-Dec: 2nd quarter (Q2) of fiscal year - use first day of next month (Jan 1)
        - Jan-Mar: 3rd quarter (Q3) of fiscal year - use first day of next month (Apr 1)
        - Apr-Jun: 4th quarter (Q4) of fiscal year - use first day of next month (Jul 1)
        
        This aligns with the first-of-month pattern used in monthly data.
        """
        quarter_map = {
            'Jul-Sep': ('10-01', -1),  # (month-day, year offset from fiscal year)
            'Oct-Dec': ('01-01', 0),   # Note: Jan 1 of the fiscal year
            'Jan-Mar': ('04-01', 0),
            'Apr-Jun': ('07-01', 0)
        }
        
        if row['Quarter'] not in quarter_map:
            return None
        
        month_day, year_offset = quarter_map[row['Quarter']]
        fiscal_year = int(row['Year'])
        calendar_year = fiscal_year + year_offset
        
        return pd.to_datetime(f'{calendar_year}-{month_day}')
        
    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        if data.empty:
            logger.warning(f"No data to insert for {self.table_name}")
            return
            
        # Convert column name to lowercase with underscore format
        column_name = ''.join(['_'+i.lower() if i.isupper() else i.lower() for i in self.value_column]).lstrip('_')
        
        # For IndividualLoans specifically, ensure it becomes individual_loans
        if self.value_column == 'IndividualLoans':
            column_name = 'individual_loans'
        
        # Rename columns to match database schema
        data = data.rename(columns={'Date': 'date', self.value_column: column_name})
        
        # Format date as string for database
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        # Use data tracker's smart update
        smart_update(
            azure_connector=self.azure,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=[column_name]
        )


class FREDScraper:
    """
    Scraper for FRED (Federal Reserve Economic Data) API.
    """
    
    def __init__(self, azure_connector: AzureConnector, config: Dict[str, Any]):
        """
        Initialize the FRED scraper.
        
        Args:
            azure_connector: Azure connector instance
            config: Scraper configuration
        """
        self.azure = azure_connector
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        self.fred_series_id = config['fred_series_id']
        self.frequency = config.get('frequency', 'm')  # Default to monthly
        
        # Try to get API key from Key Vault first, then environment variable
        try:
            if azure_connector.secret_client:
                self.api_key = azure_connector.get_secret("FRED-API-KEY")
            else:
                # Fallback to environment variable
                self.api_key = os.environ.get("FRED_API_KEY")
        except Exception as e:
            logger.warning(f"Could not retrieve FRED API key from Key Vault: {e}")
            # Fallback to environment variable
            self.api_key = os.environ.get("FRED_API_KEY")
        
        if not self.api_key:
            raise ValueError("FRED API key not found in Key Vault or environment variables")
        
        # Set default start date
        self.start_date = "2014-01-01"
    
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        self.azure.create_table(self.table_name)
    
    def fetch_fred_data(self, start_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetch data from FRED API.
        
        Args:
            start_date: Optional start date in YYYY-MM-DD format
            
        Returns:
            DataFrame with date and value columns or None if failed
        """
        # First check if we have cached data in blob storage
        container_name = "raw-files"
        blob_name = f"fred_{self.fred_series_id}.json"
        
        # Ensure container exists
        self.azure.create_container(container_name)
        
        base_url = "https://api.stlouisfed.org/fred/series/observations"
        
        params = {
            "series_id": self.fred_series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "frequency": self.frequency,  # Set from config
            "sort_order": "desc",
            "limit": 1000  # Get more historical data
        }
        
        # Use the provided start date, or fall back to the default one
        params["observation_start"] = start_date if start_date else self.start_date
            
        try:
            # Try to get data from FRED API
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'observations' not in data:
                logger.error(f"No observations in FRED API response for {self.fred_series_id}")
                return None
                
            # Save the raw JSON to blob storage for future reference
            self.azure.upload_blob(container_name, blob_name, json.dumps(data))
            
            # Convert to DataFrame
            df = pd.DataFrame(data['observations'])
            
            # Rename columns and convert types
            df = df.rename(columns={'date': 'Date', 'value': self.value_column})
            df['Date'] = pd.to_datetime(df['Date'])
            
            # Handle cases where value is "." (missing data)
            df[self.value_column] = df[self.value_column].replace('.', None)
            df[self.value_column] = pd.to_numeric(df[self.value_column], errors='coerce')
            
            # Drop rows with missing values
            df = df.dropna(subset=[self.value_column])
            
            # Sort by date
            df = df.sort_values('Date').reset_index(drop=True)
            
            # Keep only essential columns
            return df[['Date', self.value_column]]
            
        except Exception as e:
            logger.exception(f"Error processing {name}: {str(e)}")
            return 'failed'
    
    # Run all scrapers and track results
    edb_updated = []
    edb_no_update = []
    edb_failed = []
    
    # Run EDB scrapers
    logger.info("Starting EDB data scrapers")
    for name, config in edb_configs.items():
        logger.info(f"\nProcessing {name}...")
        try:
            scraper = create_scraper(config)
            status = run_scraper(scraper, name, config)
            if status == 'updated':
                edb_updated.append(name)
            elif status == 'no_update_needed':
                edb_no_update.append(name)
            else:
                edb_failed.append(name)
        except Exception as e:
            logger.exception(f"Error setting up {name}: {str(e)}")
            edb_failed.append(name)
    
    # Run FRED scrapers
    fred_updated = []
    fred_no_update = []
    fred_failed = []
    
    logger.info("\nStarting FRED data scrapers")
    for name, config in fred_configs.items():
        logger.info(f"\nProcessing {name}...")
        try:
            scraper = create_scraper(config)
            status = run_scraper(scraper, name, config)
            if status == 'updated':
                fred_updated.append(name)
            elif status == 'no_update_needed':
                fred_no_update.append(name)
            else:
                fred_failed.append(name)
        except Exception as e:
            logger.exception(f"Error setting up {name}: {str(e)}")
            fred_failed.append(name)
    
    # Run NYU Stern scraper
    nyu_updated = []
    nyu_no_update = []
    nyu_failed = []
    
    logger.info("\nStarting NYU Stern data scraper")
    name = 'equity_risk_premium'
    try:
        scraper = create_scraper(nyu_config)
        status = run_scraper(scraper, name, nyu_config)
        if status == 'updated':
            nyu_updated.append(name)
        elif status == 'no_update_needed':
            nyu_no_update.append(name)
        else:
            nyu_failed.append(name)
    except Exception as e:
        logger.exception(f"Error setting up {name}: {str(e)}")
        nyu_failed.append(name)
    
    # Combine results
    all_updated = edb_updated + fred_updated + nyu_updated
    all_no_update = edb_no_update + fred_no_update + nyu_no_update
    all_failed = edb_failed + fred_failed + nyu_failed
    
    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    
    # Create run summary
    summary = {
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration,
        "total_datasets": len(all_updated) + len(all_no_update) + len(all_failed),
        "updated": {
            "count": len(all_updated),
            "datasets": all_updated
        },
        "no_update_needed": {
            "count": len(all_no_update),
            "datasets": all_no_update
        },
        "failed": {
            "count": len(all_failed),
            "datasets": all_failed
        },
        "details": {
            "edb": {
                "updated": edb_updated,
                "no_update_needed": edb_no_update,
                "failed": edb_failed
            },
            "fred": {
                "updated": fred_updated,
                "no_update_needed": fred_no_update,
                "failed": fred_failed
            },
            "nyu": {
                "updated": nyu_updated,
                "no_update_needed": nyu_no_update,
                "failed": nyu_failed
            }
        }
    }
    
    # Save summary to blob storage
    try:
        # Convert summary to JSON
        summary_json = json.dumps(summary, indent=2, default=str)
        
        # Create a timestamp for the filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Upload to blob storage
        container_name = "logs"
        blob_name = f"run_summary_{timestamp}.json"
        
        # Ensure container exists
        azure.create_container(container_name)
        
        # Upload summary
        azure.upload_blob(container_name, blob_name, summary_json)
        logger.info(f"Saved run summary to {container_name}/{blob_name}")
    except Exception as e:
        logger.error(f"Error saving run summary: {e}")
    
    # Log summary
    logger.info("\n\n" + "="*50)
    logger.info(f"Scraping complete in {duration:.2f} seconds.")
    logger.info(f"EDB: Updated: {len(edb_updated)}, No update needed: {len(edb_no_update)}, Failed: {len(edb_failed)}")
    logger.info(f"FRED: Updated: {len(fred_updated)}, No update needed: {len(fred_no_update)}, Failed: {len(fred_failed)}")
    logger.info(f"NYU: Updated: {len(nyu_updated)}, No update needed: {len(nyu_no_update)}, Failed: {len(nyu_failed)}")
    logger.info(f"TOTAL: Updated: {len(all_updated)}, No update needed: {len(all_no_update)}, Failed: {len(all_failed)}")
    
    if all_updated:
        logger.info(f"Updated scrapers: {', '.join(all_updated)}")
    if all_no_update:
        logger.info(f"No update needed: {', '.join(all_no_update)}")
    if all_failed:
        logger.error(f"Failed scrapers: {', '.join(all_failed)}")
    
    return summary


# If this module is run directly
if __name__ == "__main__":
    # For local testing
    main(use_managed_identity=False)(f"Error fetching data from FRED API for {self.fred_series_id}: {e}")
            return None
    
    def process_data(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Process raw data.
        Note: For FRED data, we already get processed data from the API.
        
        Args:
            df: Optional DataFrame (not used for FRED as we fetch directly)
            
        Returns:
            Processed DataFrame
        """
        # If no DataFrame is provided, fetch from FRED
        if df is None:
            df = self.fetch_fred_data()
            
        if df is None or df.empty:
            return pd.DataFrame(columns=['Date', self.value_column])
        
        # Apply date adjustment for quarterly data
        if self.frequency == 'q':
            df['Date'] = df['Date'].apply(self._adjust_quarterly_date)
            
        # Apply value type conversion if needed
        if self.value_type == 'int':
            df[self.value_column] = df[self.value_column].round().astype(int)
            
        return df
    
    def _adjust_quarterly_date(self, date):
        """
        Adjust quarterly dates to first-of-month after quarter ends.
        
        FRED returns the first day of the quarter (e.g., 2025-01-01 for Q1 2025),
        but we want the first day of the month after the quarter ends:
        - Q1 (Jan-Mar) -> Apr 1
        - Q2 (Apr-Jun) -> Jul 1
        - Q3 (Jul-Sep) -> Oct 1
        - Q4 (Oct-Dec) -> Jan 1 (of next year)
        
        This aligns with the first-of-month pattern used in monthly data.
        
        Args:
            date: pandas Timestamp with the first day of a quarter
                
        Returns:
            pandas Timestamp with the first day of month after quarter end
        """
        # Get quarter number (1-4)
        quarter = (date.month - 1) // 3 + 1
        
        # Map quarters to first day of next month
        if quarter == 1:  # Q1 (Jan-Mar)
            return pd.Timestamp(date.year, 4, 1)  # Apr 1
        elif quarter == 2:  # Q2 (Apr-Jun)
            return pd.Timestamp(date.year, 7, 1)  # Jul 1
        elif quarter == 3:  # Q3 (Jul-Sep)
            return pd.Timestamp(date.year, 10, 1)  # Oct 1
        else:  # Q4 (Oct-Dec)
            return pd.Timestamp(date.year + 1, 1, 1)  # Jan 1 of next year
    
    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        if data.empty:
            logger.warning(f"No data to insert for {self.fred_series_id}")
            return
            
        # Convert column name to lowercase with underscore format
        column_name = ''.join(['_'+i.lower() if i.isupper() else i.lower() for i in self.value_column]).lstrip('_')
        
        # Rename columns to match database schema
        data = data.rename(columns={'Date': 'date', self.value_column: column_name})
        
        # Format date as string for database
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        # Use smart update
        smart_update(
            azure_connector=self.azure,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=[column_name]
        )
    
    def update_last_run(self, dataset_name: str) -> None:
        """Update timestamp of last scraper run"""
        self.azure.update_last_run(dataset_name)
    
    def get_last_run(self, dataset_name: str) -> Optional[datetime]:
        """Get timestamp of last scraper run"""
        return self.azure.get_last_run(dataset_name)
    
    def should_update(self, dataset_name: str, update_frequency_hours: int = 24) -> bool:
        """Check if dataset should be updated based on last update time"""
        return self.azure.should_update(dataset_name, update_frequency_hours)


class NYUSternScraper:
    """
    Scraper for NYU Stern Equity Risk Premium data.
    """
    
    def __init__(self, azure_connector: AzureConnector, config: Dict[str, Any]):
        """
        Initialize the NYU Stern scraper.
        
        Args:
            azure_connector: Azure connector instance
            config: Scraper configuration
        """
        self.azure = azure_connector
        self.table_name = config['table_name']
        self.url = config['url']
        self.sheet_name = config['sheet_name']
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        self.azure.create_table(self.table_name)

    def download_excel(self) -> Optional[bytes]:
        """Download Excel file from specified URL"""
        try:
            # First check if we have cached data in blob storage
            container_name = "raw-files"
            blob_name = "NYU_ERP.xlsx"
            
            # Ensure container exists
            self.azure.create_container(container_name)
            
            # Try to download from blob storage first
            excel_content = self.azure.download_blob(container_name, blob_name)
            
            if excel_content:
                logger.info("Retrieved NYU Stern data from blob storage")
                return excel_content
                
            # If not in blob storage, download from URL
            response = requests.get(self.url)
            response.raise_for_status()
            excel_content = response.content
            
            # Save to blob storage for future use
            self.azure.upload_blob(container_name, blob_name, excel_content)
            logger.info("Downloaded NYU Stern data from URL and saved to blob storage")
            
            return excel_content
        except Exception as e:
            logger.exception(f"Error downloading NYU Stern data: {e}")
            return None

    def process_data(self) -> pd.DataFrame:
        """
        Download and process the NYU Stern ERP data.
        
        Returns:
            Processed DataFrame with date and ERP values
        """
        # Download the Excel file
        excel_content = self.download_excel()
        if not excel_content:
            logger.error("Failed to download NYU Stern data")
            return pd.DataFrame()
            
        try:
            # Read the Excel file
            df = pd.read_excel(BytesIO(excel_content), sheet_name=self.sheet_name)
            
            # Clean column names
            df.columns = [str(col).strip() for col in df.columns]
            
            # Extract relevant columns
            relevant_cols = ['Start of month', 'T.Bond Rate', 'ERP (T12m)', 'Expected Return']
            
            # Check if all relevant columns exist
            missing_cols = [col for col in relevant_cols if col not in df.columns]
            if missing_cols:
                # Attempt to find similar column names
                for missing_col in missing_cols[:]:
                    for col in df.columns:
                        if missing_col.lower() in col.lower():
                            df.rename(columns={col: missing_col}, inplace=True)
                            missing_cols.remove(missing_col)
                            break
            
            # If we still have missing columns, log error and return empty dataframe
            if missing_cols:
                logger.error(f"Missing columns in NYU Stern data: {missing_cols}")
                logger.error(f"Available columns: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Keep only the relevant columns
            df = df[relevant_cols]
            
            # Rename columns to match database schema
            df.rename(columns={
                'Start of month': 'date',
                'T.Bond Rate': 'tbond_rate',
                'ERP (T12m)': 'erp_t12m',
                'Expected Return': 'expected_return'
            }, inplace=True)
            
            # Ensure date column is properly formatted as datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Process each column with percentage values individually by row
            for col in ['tbond_rate', 'erp_t12m', 'expected_return']:
                if col not in df.columns:
                    continue
                
                # Convert each value individually by row
                for idx, value in df[col].items():
                    # Convert to string for inspection
                    value_str = str(value)
                    
                    # Check if it has a % symbol
                    if '%' in value_str:
                        # Remove % and convert
                        df.at[idx, col] = float(value_str.replace('%', '')) / 100
                    else:
                        # Try to convert to float
                        try:
                            float_val = float(value)
                            # If it's a percentage value (e.g., 3.96 instead of 0.0396)
                            # Values in the data are typically in the 3-5% range as decimals
                            if float_val > 0.2:  # Threshold for identifying percentages
                                df.at[idx, col] = float_val / 100
                            else:
                                # Already in decimal form
                                df.at[idx, col] = float_val
                        except (ValueError, TypeError):
                            # Leave as is if conversion fails
                            pass
            
            # Sort by date
            df.sort_values('date', inplace=True)
            
            # Drop rows with NaN values
            df.dropna(inplace=True)
            
            return df
            
        except Exception as e:
            logger.exception(f"Error processing NYU Stern data: {e}")
            return pd.DataFrame()
    
    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        if data.empty:
            logger.warning("No data to insert for NYU Stern ERP")
            return
        
        # Format date as string for database
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        # Use smart update
        smart_update(
            azure_connector=self.azure,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=['tbond_rate', 'erp_t12m', 'expected_return']
        )
    
    def update_last_run(self, dataset_name: str) -> None:
        """Update timestamp of last scraper run"""
        self.azure.update_last_run(dataset_name)
    
    def get_last_run(self, dataset_name: str) -> Optional[datetime]:
        """Get timestamp of last scraper run"""
        return self.azure.get_last_run(dataset_name)
    
    def should_update(self, dataset_name: str, update_frequency_hours: int = 24) -> bool:
        """Check if dataset should be updated based on last update time"""
        return self.azure.should_update(dataset_name, update_frequency_hours)


#################################
# Main Data Collection Function #
#################################

def main(use_managed_identity: bool = True, storage_account: Optional[str] = None, 
         key_vault_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Main function to run all scrapers.
    
    Args:
        use_managed_identity: Whether to use managed identity for authentication
        storage_account: Optional storage account name
        key_vault_url: Optional Key Vault URL
    
    Returns:
        Dict containing summary of the run
    """
    start_time = datetime.utcnow()
    
    # Initialize Azure connector
    azure = AzureConnector(
        use_managed_identity=use_managed_identity,
        storage_account=storage_account,
        key_vault_url=key_vault_url
    )
    
    # Initialize tables and containers
    logger.info("Initializing Azure Storage tables and containers")
    azure.initialize_tables()
    azure.initialize_containers()
    
    # Configuration for all scrapers
    # This is a simplified version of the actual configuration
    # In a real implementation, you would import these from config modules
    
    # Base URL for all EDB data sources
    BASE_URL = "https://www.bde.pr.gov/BDE/PREDDOCS/"
    
    # Sample EDB scraper configs
    edb_configs = {
        'auto_sales': {
            'file_name': 'I_AUTO.XLS',
            'sheet_name': 'AS01',
            'data_location': 'A6:K18',
            'table_name': 'autosales',
            'value_column': 'Sales',
            'value_type': 'int',
            'type': 'monthly'
        },
        'bankruptcies': {
            'file_name': 'I_BANKRUPT.XLS',
            'sheet_name': 'BAN01',
            'data_location': 'A6:K18',
            'table_name': 'bankruptcies',
            'value_column': 'Filings',
            'value_type': 'int',
            'type': 'monthly'
        }
    }
    
    # Sample FRED scraper configs
    fred_configs = {
        'federal_funds_rate': {
            'table_name': 'federalfundsrate',
            'value_column': 'Rate',
            'value_type': 'float',
            'type': 'fred',
            'fred_series_id': 'DFF',  # Federal Funds Effective Rate
            'frequency': 'm'  # Monthly average
        },
        'real_gdp': {
            'table_name': 'realgdp',
            'value_column': 'Value',
            'value_type': 'float',
            'type': 'fred',
            'fred_series_id': 'GDPC1',  # Real Gross Domestic Product
            'frequency': 'q'  # Quarterly
        }
    }
    
    # NYU Stern config
    nyu_config = {
        'table_name': 'equityriskpremium',
        'url': 'https://pages.stern.nyu.edu/~adamodar/pc/implprem/ERPbymonth.xlsx',
        'sheet_name': 'Historical ERP',
        'type': 'nyu_stern'
    }
    
    # Function to create the appropriate scraper
    def create_scraper(config):
        if config['type'] == 'monthly':
            return MonthlyDataScraper(azure, config)
        elif config['type'] == 'quarterly':
            return QuarterlyDataScraper(azure, config)
        elif config['type'] == 'fred':
            return FREDScraper(azure, config)
        elif config['type'] == 'nyu_stern':
            return NYUSternScraper(azure, config)
        else:
            raise ValueError(f"Unknown scraper type: {config['type']}")
    
    # Function to run a scraper
    def run_scraper(scraper, name, config):
        try:
            # Check if tables need to be created
            scraper.create_table()
            
            # Handle FRED scrapers differently since they fetch data directly
            if config['type'] == 'fred':
                # Fetch and process data directly from FRED API
                processed_df = scraper.process_data()
                if processed_df.empty:
                    logger.warning(f"No data found for {name}")
                    return 'failed'
            # Handle NYU Stern scraper
            elif config['type'] == 'nyu_stern':
                # Process data directly
                processed_df = scraper.process_data()
                if processed_df.empty:
                    logger.warning(f"No data found for {name}")
                    return 'failed'
            else:
                # Always download and show latest data for EDB scrapers
                excel_content = scraper.download_excel(BASE_URL, config['file_name'])
                if not excel_content:
                    logger.error(f"Failed to download file for {name}")
                    return 'failed'

                # Extract data from specific sheet and location
                df = scraper.extract_data(
                    excel_content, 
                    config['sheet_name'], 
                    config['data_location']
                )
                if df is None:
                    logger.error(f"Failed to extract data for {name}")
                    return 'failed'

                # Process the data
                processed_df = scraper.process_data(df)
                if processed_df.empty:
                    logger.warning(f"No data found for {name}")
                    return 'failed'
                
            # Update data if needed
            if scraper.should_update(name):
                logger.info(f"\nUpdating {name}...")
                try:
                    scraper.insert_data(processed_df)
                    scraper.update_last_run(name)
                    logger.info(f"Successfully updated {name}")
                    return 'updated'
                except Exception as e:
                    logger.exception(f"Error updating {name}: {str(e)}")
                    return 'failed'
            else:
                logger.info(f"No update needed for {name} yet")
                return 'no_update_needed'
        except Exception as e:
            logger.exception