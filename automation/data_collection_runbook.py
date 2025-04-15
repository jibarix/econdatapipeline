"""
Azure Automation runbook for economic data collection.
This runbook executes the data collection pipeline using managed identity.

Pre-requisites:
1. The economic_data_pipeline.py module must be uploaded to Azure Automation as a Python module
2. The Azure Automation account must have a System-assigned Managed Identity
3. The Managed Identity needs the following permissions:
   - Storage Blob Data Contributor on the storage account
   - Storage Table Data Contributor on the storage account
   - Key Vault Secrets User on the key vault
"""
import os
import sys
import traceback
import logging
from datetime import datetime

def write_to_output(message):
    """Write message to the output stream."""
    print(message)

def main():
    # Set up timestamp for the run
    start_time = datetime.utcnow()
    write_to_output(f"Economic data collection started at {start_time.isoformat()}")
    
    try:
        # Import the consolidated module
        # This assumes economic_data_pipeline.py has been uploaded to Azure Automation
        import economic_data_pipeline
        
        # Get variables from Azure Automation
        storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", "econdatastorage")
        key_vault_url = os.environ.get("AZURE_KEY_VAULT_URL", "https://econdatakeyvault.vault.azure.net/")
        
        write_to_output(f"Using storage account: {storage_account}")
        write_to_output(f"Using Key Vault URL: {key_vault_url}")
        
        # Execute the data collection with managed identity
        result = economic_data_pipeline.main(
            use_managed_identity=True,
            storage_account=storage_account,
            key_vault_url=key_vault_url
        )
        
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
        else:
            write_to_output("Data collection failed: No result returned")
        
        # Calculate duration
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()
        
        write_to_output(f"Data collection completed in {duration_seconds:.2f} seconds")
        
        return {
            "status": "success",
            "duration_seconds": duration_seconds,
            "updated": result["updated"]["count"] if result else 0,
            "no_update_needed": result["no_update_needed"]["count"] if result else 0,
            "failed": result["failed"]["count"] if result else 0
        }
        
    except Exception as e:
        error_message = f"Error in data collection: {str(e)}"
        stack_trace = traceback.format_exc()
        
        write_to_output(error_message)
        write_to_output("Stack trace:")
        write_to_output(stack_trace)
        
        # Return error information
        return {
            "status": "error",
            "error": str(e),
            "stack_trace": stack_trace,
            "timestamp": datetime.utcnow().isoformat()
        }

# Entry point for the runbook
if __name__ == "__main__":
    main()