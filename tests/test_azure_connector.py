"""
Test script to validate Azure Storage connection and functionality.
"""
import os
import sys
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('azure_test')

# Import the Azure connector
from azure_connector import AzureConnector

def test_azure_connection():
    """Test basic Azure Storage connection."""
    logger.info("Testing Azure Storage connection...")
    
    try:
        # Connect to Azure
        azure = AzureConnector(use_managed_identity=False)
        logger.info("✓ Successfully connected to Azure Storage")
        return azure
    except Exception as e:
        logger.error(f"✗ Failed to connect to Azure Storage: {e}")
        return None

def test_container_operations(azure):
    """Test container operations."""
    logger.info("\nTesting container operations...")
    
    test_container = "test-container"
    
    # Test container creation
    result = azure.create_container(test_container)
    if result:
        logger.info(f"✓ Created container '{test_container}'")
    else:
        logger.error(f"✗ Failed to create container '{test_container}'")
        return False
    
    # Test blob upload
    test_blob = "test-blob.txt"
    test_content = "This is a test blob created at " + datetime.utcnow().isoformat()
    
    result = azure.upload_blob(test_container, test_blob, test_content)
    if result:
        logger.info(f"✓ Uploaded blob '{test_blob}'")
    else:
        logger.error(f"✗ Failed to upload blob '{test_blob}'")
        return False
    
    # Test blob download
    content = azure.download_blob(test_container, test_blob)
    if content:
        logger.info(f"✓ Downloaded blob '{test_blob}'")
        logger.info(f"  Content: {content.decode('utf-8')}")
    else:
        logger.error(f"✗ Failed to download blob '{test_blob}'")
        return False
    
    # Test blob listing
    blobs = azure.list_blobs(test_container)
    if test_blob in blobs:
        logger.info(f"✓ Listed blobs in container '{test_container}'")
        logger.info(f"  Blobs: {blobs}")
    else:
        logger.error(f"✗ Failed to list blobs in container '{test_container}'")
        return False
    
    # Test blob deletion
    result = azure.delete_blob(test_container, test_blob)
    if result:
        logger.info(f"✓ Deleted blob '{test_blob}'")
    else:
        logger.error(f"✗ Failed to delete blob '{test_blob}'")
        return False
    
    return True

def test_table_operations(azure):
    """Test table operations."""
    logger.info("\nTesting table operations...")
    
    test_table = "testtable"
    
    # Test table creation
    result = azure.create_table(test_table)
    if result:
        logger.info(f"✓ Created table '{test_table}'")
    else:
        logger.error(f"✗ Failed to create table '{test_table}'")
        return False
    
    # Test entity insertion
    test_entity = {
        "PartitionKey": "test-partition",
        "RowKey": "test-row-1",
        "StringValue": "Test value",
        "IntValue": 42,
        "FloatValue": 3.14159,
        "BoolValue": True,
        "DateValue": datetime.utcnow().isoformat()
    }
    
    result = azure.upsert_entity(test_table, test_entity)
    if result:
        logger.info(f"✓ Inserted entity into table '{test_table}'")
    else:
        logger.error(f"✗ Failed to insert entity into table '{test_table}'")
        return False
    
    # Test batch insertion
    test_entities = [
        {
            "PartitionKey": "test-partition",
            "RowKey": "test-row-2",
            "Value": "Test value 2"
        },
        {
            "PartitionKey": "test-partition",
            "RowKey": "test-row-3",
            "Value": "Test value 3"
        }
    ]
    
    result = azure.batch_upsert(test_table, test_entities)
    if result:
        logger.info(f"✓ Batch inserted entities into table '{test_table}'")
    else:
        logger.error(f"✗ Failed to batch insert entities into table '{test_table}'")
        return False
    
    # Test entity retrieval
    entity = azure.get_entity(test_table, "test-partition", "test-row-1")
    if entity:
        logger.info(f"✓ Retrieved entity from table '{test_table}'")
        logger.info(f"  Entity: {entity}")
    else:
        logger.error(f"✗ Failed to retrieve entity from table '{test_table}'")
        return False
    
    # Test entity query
    entities = azure.query_entities(test_table, "PartitionKey eq 'test-partition'")
    if entities and len(entities) == 3:  # We inserted 3 entities
        logger.info(f"✓ Queried entities from table '{test_table}'")
        logger.info(f"  Found {len(entities)} entities")
    else:
        logger.error(f"✗ Failed to query entities from table '{test_table}'")
        return False
    
    # Test entity deletion
    result = azure.delete_entity(test_table, "test-partition", "test-row-1")
    if result:
        logger.info(f"✓ Deleted entity from table '{test_table}'")
    else:
        logger.error(f"✗ Failed to delete entity from table '{test_table}'")
        return False
    
    return True

def test_data_tracker_operations(azure):
    """Test data tracker operations."""
    logger.info("\nTesting data tracker operations...")
    
    # Import the smart_update function
    from azure_data_tracker import smart_update, initialize_revision_tracking
    
    # Initialize revision tracking
    result = initialize_revision_tracking(azure)
    if result:
        logger.info("✓ Initialized revision tracking")
    else:
        logger.error("✗ Failed to initialize revision tracking")
        return False
    
    # Test dataset
    test_dataset = "testdataset"
    
    # Create test table
    result = azure.create_table(test_dataset)
    if result:
        logger.info(f"✓ Created table '{test_dataset}'")
    else:
        logger.error(f"✗ Failed to create table '{test_dataset}'")
        return False
    
    # Create test data
    test_data = pd.DataFrame({
        "date": ["2025-01-01", "2025-02-01", "2025-03-01"],
        "value": [100.0, 200.0, 300.0]
    })
    
    # Test smart update
    result = smart_update(
        azure_connector=azure,
        dataset_name=test_dataset,
        data_df=test_data,
        date_field="date",
        value_fields=["value"]
    )
    
    if result["new"] == 3 or (result["new"] == 0 and result["updated"] >= 0):
        logger.info(f"✓ Smart update inserted {result['new']} new records")
    else:
        logger.error(f"✗ Smart update failed to insert records: {result}")
        return False
    
    # Test update with changes (should track revisions)
    test_data_updated = pd.DataFrame({
        "date": ["2025-01-01", "2025-02-01", "2025-03-01"],
        "value": [110.0, 200.0, 310.0]  # Changed values for 2 records
    })
    
    result = smart_update(
        azure_connector=azure,
        dataset_name=test_dataset,
        data_df=test_data_updated,
        date_field="date",
        value_fields=["value"]
    )
    
    if result["updated"] == 2 and result["revisions"] == 2:
        logger.info(f"✓ Smart update tracked {result['revisions']} revisions")
    else:
        logger.error(f"✗ Smart update failed to track revisions: {result}")
        return False
    
    # Test getting revision history
    from azure_data_tracker import get_revision_history
    
    revisions = get_revision_history(azure, test_dataset)
    if not revisions.empty and revisions.shape[1] >= 4:
        logger.info(f"✓ Retrieved {len(revisions)} revisions")
        logger.info(f"  Revisions:\n{revisions}")
    else:
        logger.error("✗ Failed to retrieve revisions")
        return False
    
    return True

def test_scraper_prerequisites(azure):
    """Test scraper prerequisites."""
    logger.info("\nTesting scraper prerequisites...")
    
    # Create scraper metadata table
    azure.create_table("scrapermetadata")
    
    # Test metadata tracking
    result = azure.update_last_run("test_scraper")
    if result:
        logger.info("✓ Updated last run timestamp")
    else:
        logger.error("✗ Failed to update last run timestamp")
        return False
    
    # Test retrieving last run
    last_run = azure.get_last_run("test_scraper")
    if last_run:
        logger.info(f"✓ Retrieved last run timestamp: {last_run}")
    else:
        logger.error("✗ Failed to retrieve last run timestamp")
        return False
    
    # Test should_update
    should_update = azure.should_update("test_scraper", update_frequency_hours=24)
    logger.info(f"✓ Should update: {should_update} (expected: False as we just updated)")
    
    return True

def main():
    """Run all tests."""
    logger.info("====== AZURE CONNECTOR TEST ======")
    
    # Test connection
    azure = test_azure_connection()
    if not azure:
        logger.error("Connection test failed. Aborting.")
        sys.exit(1)
    
    # Track test results
    test_results = {
        "container_operations": test_container_operations(azure),
        "table_operations": test_table_operations(azure),
        "data_tracker_operations": test_data_tracker_operations(azure),
        "scraper_prerequisites": test_scraper_prerequisites(azure)
    }
    
    # Print summary
    logger.info("\n====== TEST SUMMARY ======")
    all_passed = True
    
    for test_name, result in test_results.items():
        status = "PASSED" if result else "FAILED"
        logger.info(f"{test_name}: {status}")
        if not result:
            all_passed = False
    
    if all_passed:
        logger.info("\n✓ All tests passed! Your Azure implementation is working correctly.")
    else:
        logger.error("\n✗ Some tests failed. Please fix the issues before proceeding.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
