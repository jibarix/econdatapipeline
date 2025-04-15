"""
Azure Automation runbook for economic data collection.
This runbook executes the data collection pipeline using managed identity.

This runbook should be uploaded to Azure Automation account and can be scheduled
to run at regular intervals (e.g., daily) to collect economic data.

Requirements:
1. Azure Automation account with System-assigned Managed Identity
2. Role assignments for the Managed Identity:
   - Storage Blob Data Contributor on the storage account
   - Storage Table Data Contributor on the storage account
   - Key Vault Secrets User on the key vault
3. Python modules imported into Azure Automation:
   - azure-identity
   - azure-storage-blob
   - azure-data-tables
   - azure-keyvault-secrets
   - pandas
   - requests
   - main_azure (custom module containing the data collection code)

Variables (can be set in Azure Automation variables):
- AZURE_STORAGE_ACCOUNT: Name of the storage account
- AZURE_KEY_VAULT_URL: URL of the key vault
"""
import os
import sys
import logging
import traceback
from datetime import datetime

# Azure Automation runbooks use Python 3, which doesn't have the azure namespace by default
# But the Azure modules are pre-installed or can be imported via the modules gallery
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient

def write_to_output(message):
    """Write message to the output stream."""
    print(message)

def setup_logging():
    """Set up logging to both console and Blob Storage."""
    # Configure logging
    logger = logging.getLogger('econ_data_collection')
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger

def run_data_collection():
    """
    Main function to run the data collection pipeline.
    
    This function is called by the Azure Automation runbook and executes
    the data collection pipeline.
    
    Returns:
        dict: Result summary or error information
    """
    start_time = datetime.utcnow()
    logger = setup_logging()
    
    try:
        write_to_output(f"Starting economic data collection at {start_time.isoformat()}")
        logger.info(f"Starting economic data collection at {start_time.isoformat()}")
        
        # Azure Automation uses the ManagedIdentity credential
        credential = ManagedIdentityCredential()
        
        # Get storage account name from environment variable or use default
        storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", "econdatastorage")
        key_vault_url = os.environ.get("AZURE_KEY_VAULT_URL", "https://econdatakeyvault.vault.azure.net/")
        
        # These parameters will be passed to the main module
        params = {
            "use_managed_identity": True,
            "storage_account": storage_account,
            "key_vault_url": key_vault_url
        }
        
        # Log connection attempt
        logger.info(f"Connecting to Azure Storage account: {storage_account}")
        logger.info(f"Using Key Vault URL: {key_vault_url}")
        
        # Test the connection
        try:
            # Test Blob Storage connection
            blob_service_url = f"https://{storage_account}.blob.core.windows.net/"
            blob_service = BlobServiceClient(account_url=blob_service_url, credential=credential)
            # List containers to test connection
            containers = list(blob_service.list_containers(max_results=1))
            logger.info(f"Successfully connected to Blob Storage. Found {len(containers)} containers.")
            
            # Test Table Storage connection
            table_service_url = f"https://{storage_account}.table.core.windows.net/"
            table_service = TableServiceClient(endpoint=table_service_url, credential=credential)
            # List tables to test connection
            tables = list(table_service.list_tables(results_per_page=1))
            logger.info(f"Successfully connected to Table Storage. Found {len(tables)} tables.")
            
            # If we made it here, both connections are working
            write_to_output("Successfully connected to Azure Storage services")
        except Exception as e:
            error_msg = f"Failed to connect to Azure Storage: {str(e)}"
            logger.error(error_msg)
            write_to_output(error_msg)
            raise
        
        # Import main module and run data collection
        # This is the azure-adapted main.py
        # In Azure Automation, we need to ensure the module is available in the Python modules section
        from main_azure import main
        
        # Run the data collection
        write_to_output("Executing data collection pipeline...")
        result = main()
        
        # Process the result
        if result:
            # Log a summary of the run
            updated_count = result["updated"]["count"]
            no_update_count = result["no_update_needed"]["count"]
            failed_count = result["failed"]["count"]
            
            write_to_output(f"Data collection complete: {updated_count} updated, {no_update_count} no update needed, {failed_count} failed")
            
            if failed_count > 0:
                failed_datasets = ", ".join(result["failed"]["datasets"])
                write_to_output(f"Failed datasets: {failed_datasets}")
                logger.warning(f"Failed datasets: {failed_datasets}")
        else:
            write_to_output("Data collection failed: No result returned")
            logger.error("Data collection failed: No result returned")
        
        # Calculate duration
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()
        
        write_to_output(f"Data collection completed in {duration_seconds:.2f} seconds")
        logger.info(f"Data collection completed in {duration_seconds:.2f} seconds")
        
        return result
        
    except Exception as e:
        error_message = f"Error in data collection: {str(e)}"
        error_traceback = traceback.format_exc()
        
        logger.error(error_message)
        logger.error(error_traceback)
        
        write_to_output(error_message)
        write_to_output("Error traceback:")
        write_to_output(error_traceback)
        
        # Return error information
        return {
            "error": str(e),
            "traceback": error_traceback,
            "timestamp": datetime.utcnow().isoformat()
        }

# Entry point for the runbook
if __name__ == "__main__":
    # When running in Azure Automation, this is the entry point
    run_data_collection()