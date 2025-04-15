"""
Setup Modules Runbook for Azure Automation.
This runbook verifies that all required modules are properly imported.
"""
import sys
import os
import datetime

def write_to_output(message):
    """Write message to the output stream."""
    print(message)

# Record start time
start_time = datetime.datetime.utcnow()
write_to_output(f"Module verification started at {start_time.isoformat()}")

try:
    # Log Python version
    write_to_output(f"Python version: {sys.version}")
    write_to_output(f"Python executable: {sys.executable}")
    
    # Verify core Azure modules
    write_to_output("\nVerifying Azure modules...")
    
    try:
        from azure.identity import ManagedIdentityCredential
        write_to_output("✓ azure.identity imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure.identity: {e}")
    
    try:
        from azure.storage.blob import BlobServiceClient
        write_to_output("✓ azure.storage.blob imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure.storage.blob: {e}")
    
    try:
        from azure.data.tables import TableServiceClient
        write_to_output("✓ azure.data.tables imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure.data.tables: {e}")
    
    try:
        from azure.keyvault.secrets import SecretClient
        write_to_output("✓ azure.keyvault.secrets imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure.keyvault.secrets: {e}")
    
    # Verify data processing modules
    write_to_output("\nVerifying data processing modules...")
    
    try:
        import pandas as pd
        write_to_output(f"✓ pandas {pd.__version__} imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import pandas: {e}")
    
    try:
        import numpy as np
        write_to_output(f"✓ numpy {np.__version__} imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import numpy: {e}")
    
    try:
        import requests
        write_to_output(f"✓ requests {requests.__version__} imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import requests: {e}")
    
    # Check if the economic_data_pipeline package is available
    write_to_output("\nVerifying custom modules...")
    
    # Test individual module imports - this works even if the package isn't installed
    try:
        import azure_connector
        write_to_output("✓ azure_connector module imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure_connector module: {e}")
        
    try:
        import azure_data_tracker
        write_to_output("✓ azure_data_tracker module imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure_data_tracker module: {e}")
        
    try:
        import azure_common_scrapers
        write_to_output("✓ azure_common_scrapers module imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure_common_scrapers module: {e}")
        
    try:
        import azure_fred_scraper
        write_to_output("✓ azure_fred_scraper module imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure_fred_scraper module: {e}")
        
    try:
        import azure_nyu_scraper
        write_to_output("✓ azure_nyu_scraper module imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import azure_nyu_scraper module: {e}")
        
    try:
        import main_azure
        write_to_output("✓ main_azure module imported successfully")
    except ImportError as e:
        write_to_output(f"✗ Failed to import main_azure module: {e}")
    
    # Check environment variables
    write_to_output("\nChecking environment variables...")
    expected_variables = [
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_KEY_VAULT_URL"
    ]
    
    for var in expected_variables:
        if var in os.environ:
            write_to_output(f"✓ {var} is set to: {os.environ[var]}")
        else:
            write_to_output(f"✗ {var} is not set")
    
    # Calculate duration
    end_time = datetime.datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    
    write_to_output(f"\nModule verification completed in {duration:.2f} seconds")
    
except Exception as e:
    import traceback
    write_to_output(f"Error during module verification: {str(e)}")
    write_to_output(traceback.format_exc())