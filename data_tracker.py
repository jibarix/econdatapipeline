"""
Module for tracking data revisions and performing smart updates.
This keeps revision tracking separate from the main scraper logic.
"""
from datetime import datetime
import pandas as pd

def initialize_revision_table(supabase):
    """Create the data_revisions table if it doesn't exist."""
    sql = """
    CREATE TABLE IF NOT EXISTS data_revisions (
        id SERIAL PRIMARY KEY,
        dataset VARCHAR NOT NULL,
        data_date DATE NOT NULL,
        value_field VARCHAR NOT NULL,
        old_value DECIMAL(12,2),
        new_value DECIMAL(12,2),
        revision_date TIMESTAMP WITH TIME ZONE NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_data_revisions_dataset_date ON data_revisions (dataset, data_date);
    """
    for statement in sql.split(';'):
        if statement.strip():
            supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()

def smart_update(supabase, dataset_name, data_df, date_field, value_fields):
    """
    Smart insert/update that tracks revisions.
    
    Args:
        supabase: Supabase client
        dataset_name: Name of the dataset (table)
        data_df: DataFrame with processed data
        date_field: Name of the date column
        value_fields: List of value columns to track
    """
    # Ensure date column is properly formatted
    data_df[date_field] = pd.to_datetime(data_df[date_field]).dt.strftime('%Y-%m-%d')
    
    # Get existing data for comparison
    result = supabase.table(dataset_name).select('*').execute()
    existing_df = pd.DataFrame(result.data) if result.data else pd.DataFrame()
    
    if existing_df.empty:
        # No existing data, just insert all
        print(f"No existing data found. Inserting {len(data_df)} new records.")
        records = data_df.to_dict('records')
        supabase.table(dataset_name).insert(records).execute()
        return
    
    # Prepare for comparison - create date indexed dictionaries
    existing_data = {}
    for _, row in existing_df.iterrows():
        existing_data[row[date_field]] = row
    
    # Track new, updated, and unchanged records
    new_records = []
    updates = []
    revisions = []
    
    # Compare each record
    for _, row in data_df.iterrows():
        record_date = row[date_field]
        
        if record_date not in existing_data:
            # New record
            new_records.append(row.to_dict())
            continue
            
        # Check for value changes
        existing_row = existing_data[record_date]
        record_changed = False
        
        for field in value_fields:
            # Skip if either value is NaN
            if pd.isna(row[field]) or pd.isna(existing_row[field]):
                continue
                
            # Check if value changed
            if abs(float(row[field]) - float(existing_row[field])) > 0.001:  # Allow small float precision diffs
                record_changed = True
                
                # Track revision
                revisions.append({
                    'dataset': dataset_name,
                    'data_date': record_date,
                    'value_field': field,
                    'old_value': float(existing_row[field]),
                    'new_value': float(row[field]),
                    'revision_date': datetime.utcnow().isoformat()
                })
        
        if record_changed:
            updates.append({**row.to_dict(), 'id': existing_row['id']})
    
    # Execute database operations
    if new_records:
        print(f"Inserting {len(new_records)} new records")
        supabase.table(dataset_name).insert(new_records).execute()
    
    if updates:
        print(f"Updating {len(updates)} changed records")
        for update in updates:
            supabase.table(dataset_name).update(update).eq('id', update['id']).execute()
    
    if revisions:
        print(f"Recording {len(revisions)} data revisions")
        supabase.table('data_revisions').insert(revisions).execute()
    
    print(f"Smart update complete: {len(new_records)} new, {len(updates)} updated, {len(revisions)} revisions tracked")

def get_revision_history(supabase, dataset, date=None, field=None, limit=None):
    """
    Get revision history for a dataset or specific data point.
    
    Args:
        supabase: Supabase client
        dataset: The dataset name
        date: Optional specific date to filter by
        field: Optional specific field to filter by
        limit: Optional maximum number of revisions to return
    
    Returns:
        List of revision records
    """
    query = supabase.table('data_revisions').select('*').eq('dataset', dataset)
    
    if date:
        query = query.eq('data_date', date)
    
    if field:
        query = query.eq('value_field', field)
    
    query = query.order('revision_date', desc=True)
    
    if limit:
        query = query.limit(limit)
        
    result = query.execute()
    return result.data