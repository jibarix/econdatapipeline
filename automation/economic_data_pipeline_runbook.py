"""
Economic Data Pipeline Runbook for Azure Automation.
This runbook collects economic data from multiple sources and stores it in Azure Storage.

Requirements:
- Python 3.8
- Required packages: pandas, requests, azure-identity, azure-storage-blob, azure-data-tables,
  azure-keyvault-secrets, python-dateutil, openpyxl
- System-assigned Managed Identity with proper permissions:
  - Storage Blob Data Contributor on the storage account
  - Storage Table Data Contributor on the storage account
  - Key Vault Secrets User on the key vault
- FRED API key stored in Key Vault as 'FRED-API-KEY'
"""

import os
import logging
import json
import traceback
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from io import BytesIO
from dateutil import parser

# Azure SDK imports
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('economic_data_pipeline')

# Function to write to output (for Azure Automation)
def write_to_output(message):
    """Write message to the output stream."""
    print(message)
    logger.info(message)

#########################
# Configuration Section #
#########################

# Base URL for all EDB data sources
BASE_URL = "https://www.bde.pr.gov/BDE/PREDDOCS/"

# Common settings for FRED API
FRED_START_DATE = "2014-01-01"  # Start date for all FRED data

# EDB scraper configurations
EDB_SCRAPER_CONFIGS = {
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
    },
    'cement_production': {
        'file_name': 'I_CEMENT.XLS',
        'sheet_name': 'CD01',
        'data_location': 'A6:K18',
        'table_name': 'cementproduction',
        'value_column': 'Production',
        'value_type': 'float',
        'type': 'monthly'
    },
    'electricity_consumption': {
        'file_name': 'I_ENERGY.XLS',
        'sheet_name': 'EEC01',
        'data_location': 'A6:K18',
        'table_name': 'electricityconsumption',
        'value_column': 'Consumption',
        'value_type': 'float',
        'type': 'monthly'
    },
    'gas_price': {
        'file_name': 'I_GAS.XLS',
        'sheet_name': 'GAS01',
        'data_location': 'A6:K18',
        'table_name': 'gasprice',
        'value_column': 'Price',
        'value_type': 'float',
        'type': 'monthly'
    },
    'gas_consumption': {
        'file_name': 'I_GAS.XLS',
        'sheet_name': 'GAS02',
        'data_location': 'A6:K18',
        'table_name': 'gasconsumption',
        'value_column': 'Consumption',
        'value_type': 'float',
        'type': 'monthly'
    },
    'labor_participation': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF03',
        'data_location': 'A6:K18',
        'table_name': 'laborparticipation',
        'value_column': 'Rate',
        'value_type': 'float',
        'type': 'monthly'
    },
    'unemployment_rate': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF08',
        'data_location': 'A6:K18',
        'table_name': 'unemploymentrate',
        'value_column': 'Rate',
        'value_type': 'float',
        'type': 'monthly'
    },
    'employment_rate': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF09',
        'data_location': 'A6:K18',
        'table_name': 'employmentrate',
        'value_column': 'Rate',
        'value_type': 'float',
        'type': 'monthly'
    },
    'unemployment_claims': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF10',
        'data_location': 'A6:K18',
        'table_name': 'unemploymentclaims',
        'value_column': 'Claims',
        'value_type': 'int',
        'type': 'monthly'
    },
    'trade_employment': {
        'file_name': 'I_PAYROLL.XLS',
        'sheet_name': 'PE05',
        'data_location': 'A6:K18',
        'table_name': 'tradeemployment',
        'value_column': 'Employment',
        'value_type': 'float',
        'type': 'monthly'
    },
    'consumer_price_index': {
        'file_name': 'I_PRICE.XLS',
        'sheet_name': 'CPI01',
        'data_location': 'A6:K18',
        'table_name': 'consumerpriceindex',
        'value_column': 'Index',
        'value_type': 'float',
        'type': 'monthly'
    },
    'transportation_price_index': {
        'file_name': 'I_PRICE.XLS',
        'sheet_name': 'CPI05',
        'data_location': 'A6:K18',
        'table_name': 'transportationpriceindex',
        'value_column': 'Index',
        'value_type': 'float',
        'type': 'monthly'
    },
    'retail_sales': {
        'file_name': 'I_RETAIL.XLS',
        'sheet_name': 'RS01',
        'data_location': 'A6:K18',
        'table_name': 'retailsales',
        'value_column': 'Sales',
        'value_type': 'float',
        'type': 'monthly'
    },
    'imports': {
        'file_name': 'I_TRADE.XLS',
        'sheet_name': 'ET05',
        'data_location': 'A6:K18',
        'table_name': 'imports',
        'value_column': 'Value',
        'value_type': 'float',
        'type': 'monthly'
    }
}

# FRED API scraper configurations
FRED_SCRAPER_CONFIGS = {
    'federal_funds_rate': {
        'table_name': 'federalfundsrate',
        'value_column': 'Rate',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'DFF',  # Federal Funds Effective Rate
        'frequency': 'm'  # Monthly average
    },
    'auto_manufacturing_orders': {
        'table_name': 'automanufacturingorders',
        'value_column': 'Orders',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'AMVPNO',  # Manufacturers' New Orders: Motor Vehicles and Parts
        'frequency': 'm'  # Monthly
    },
    'used_car_retail_sales': {
        'table_name': 'usedcarretailsales',
        'value_column': 'Sales',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'MRTSSM44112USN',  # Retail Sales: Used Car Dealers
        'frequency': 'm'  # Monthly
    },
    'domestic_auto_inventories': {
        'table_name': 'domesticautoinventories',
        'value_column': 'Inventories',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'AUINSA',  # Domestic Auto Inventories
        'frequency': 'm'  # Monthly
    },
    'domestic_auto_production': {
        'table_name': 'domesticautoproduction',
        'value_column': 'Production',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'DAUPSA',  # Domestic Auto Production
        'frequency': 'm'  # Monthly
    },
    'liquidity_credit_facilities': {
        'table_name': 'liquiditycreditfacilities',
        'value_column': 'Facilities',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'WLCFLL',  # Assets: Liquidity and Credit Facilities: Loans
        'frequency': 'm'  # Monthly
    },
    'semiconductor_manufacturing_units': {
        'table_name': 'semiconductormanufacturingunits',
        'value_column': 'Units',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'IPG3344S',  # Industrial Production: Semiconductor Components
        'frequency': 'm'  # Monthly
    },
    'aluminum_new_orders': {
        'table_name': 'aluminumneworders',
        'value_column': 'Orders',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'AANMNO',  # Manufacturers' New Orders: Aluminum Products
        'frequency': 'm'  # Monthly    
    },
    'real_gdp': {
        'table_name': 'realgdp',
        'value_column': 'Value',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'GDPC1',  # Real Gross Domestic Product
        'frequency': 'q'  # Quarterly
    },
    'gdp_now_forecast': {
        'table_name': 'gdpnowforecast',
        'value_column': 'Forecast',
        'value_type': 'float',
        'type': 'fred',
        'fred_series_id': 'GDPNOW',  # GDPNow Forecast
        'frequency': 'q'  # Quarterly
    }
}

# NYU Stern configuration
NYU_STERN_CONFIG = {
    'table_name': 'equityriskpremium',
    'url': 'https://pages.stern.nyu.edu/~adamodar/pc/implprem/ERPbymonth.xlsx',
    'sheet_name': 'Historical ERP',
    'type': 'nyu_stern'
}

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
                write_to_output("Using Managed Identity for authentication")
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
                write_to_output("Using connection string for authentication")
                conn_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
                if not conn_string:
                    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required when not using managed identity")
                
                # Create table service client using connection string
                self.table_service = TableServiceClient.from_connection_string(conn_string)
                # Create blob service client using connection string
                self.blob_service = BlobServiceClient.from_connection_string(conn_string)
                
            write_to_output("Azure Storage clients initialized successfully")
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
            write_to_output("Azure Key Vault client initialized successfully")
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

#############################
# Data Tracking Functions   #
#############################

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