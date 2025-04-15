"""
Module for tracking data revisions and performing smart updates using Azure storage.
This keeps revision tracking separate from the main scraper logic and replaces
the original Supabase-based implementation.
"""
from datetime import datetime
import pandas as pd
import logging
from typing import List, Dict, Any, Optional

from azure_connector import AzureConnector

logger = logging.getLogger(__name__)

def initialize_revision_tracking(azure_connector: AzureConnector) -> bool:
    """
    Initialize the data revision tracking table in Azure.
    
    Args:
        azure_connector: AzureConnector instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    return azure_connector.create_table("data_revisions")

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
        if azure_connector.batch_upsert("data_revisions", revisions):
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
    result = azure_connector.query_entities("data_revisions", query_filter)
    
    if not result:
        # Return empty dataframe with expected columns
        return pd.DataFrame(columns=[
            'dataset', 'data_date', 'value_field', 
            'old_value', 'new_value', 'revision_date'
        ])
    
    # Convert to DataFrame
    df = pd.DataFrame(result)
    
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