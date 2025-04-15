"""
Utility script to view latest data from the database.
"""
import argparse
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import io
from models import get_supabase_client
import data_tracker

def get_all_tables(supabase):
    """
    Get all data tables from the database.
    Uses the config module as a fallback if SQL queries fail.
    """
    try:
        # Try direct approach with known tables from config first
        from config import SCRAPER_CONFIGS
        from fred_config import FRED_SCRAPER_CONFIGS
        from nyu_config import NYU_STERN_CONFIG
        
        tables = []
        
        # Add EDB tables
        for name, config in SCRAPER_CONFIGS.items():
            table_name = config['table_name']
            tables.append(table_name)
            
        # Add FRED tables
        for name, config in FRED_SCRAPER_CONFIGS.items():
            table_name = config['table_name']
            tables.append(table_name)
            
        # Add NYU table
        tables.append(NYU_STERN_CONFIG['table_name'])
        
        # Test if tables exist
        existing_tables = []
        for table in tables:
            try:
                # Just try to get one record to verify table exists
                test = supabase.table(table).select('*').limit(1).execute()
                existing_tables.append(table)
            except Exception:
                # Skip tables that can't be accessed
                pass
                
        if existing_tables:
            return existing_tables
            
        # If no tables found, try SQL approach
        print("No tables found in configuration, trying database query...")
        sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name NOT IN ('scraper_metadata', 'data_revisions') 
        AND table_name NOT LIKE 'pg_%'
        ORDER BY table_name;
        """
        
        result = supabase.postgrest.rpc('exec_sql', {'query': sql}).execute()
        
        if result.data:
            return [item['table_name'] for item in result.data]
        else:
            print("No tables found using SQL query.")
            return []
    
    except Exception as e:
        print(f"Error retrieving tables: {e}")
        print("Check your database connection parameters in .env file.")
        return []

def get_table_summary(supabase, table_name):
    """
    Get a summary of the data in a table.
    
    Returns:
        dict: Summary statistics including:
            - count: Number of records
            - date_range: (min_date, max_date)
            - value_columns: List of data columns
            - latest_values: Most recent values
            - frequency: Estimated data frequency
    """
    # Get column names first
    sql_columns = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'public' 
    AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    
    columns_result = supabase.postgrest.rpc('exec_sql', {'query': sql_columns}).execute()
    if not columns_result.data:
        return None
    
    columns = {item['column_name']: item['data_type'] for item in columns_result.data}
    
    # Skip id and date columns to get value columns
    value_columns = [col for col in columns.keys() if col not in ['id', 'date']]
    value_types = {col: columns[col] for col in value_columns}
    
    # Get count, min and max dates
    sql_stats = f"""
    SELECT 
        COUNT(*) as record_count,
        MIN(date) as min_date,
        MAX(date) as max_date
    FROM {table_name};
    """
    
    stats_result = supabase.postgrest.rpc('exec_sql', {'query': sql_stats}).execute()
    if not stats_result.data:
        return None
    
    stats = stats_result.data[0]
    
    # Get latest values
    latest_values = None
    if stats['record_count'] > 0:
        values_sql = f"""
        SELECT * FROM {table_name}
        WHERE date = '{stats['max_date']}'
        LIMIT 1;
        """
        values_result = supabase.postgrest.rpc('exec_sql', {'query': values_sql}).execute()
        if values_result.data:
            latest_values = {col: values_result.data[0][col] for col in value_columns}
    
    # Estimate frequency
    frequency = "unknown"
    if stats['record_count'] > 1:
        freq_sql = f"""
        WITH date_diffs AS (
            SELECT 
                date,
                date - LAG(date) OVER (ORDER BY date) AS diff
            FROM {table_name}
            ORDER BY date
            LIMIT 100
        )
        SELECT 
            AVG(diff) as avg_diff,
            MODE() WITHIN GROUP (ORDER BY diff) as mode_diff
        FROM date_diffs
        WHERE diff IS NOT NULL;
        """
        
        freq_result = supabase.postgrest.rpc('exec_sql', {'query': freq_sql}).execute()
        if freq_result.data:
            avg_days = freq_result.data[0]['avg_diff']
            mode_days = freq_result.data[0]['mode_diff']
            
            # Determine frequency based on approximate days
            if mode_days is not None:
                if mode_days <= 1:
                    frequency = "daily"
                elif 5 <= mode_days <= 10:
                    frequency = "weekly"
                elif 25 <= mode_days <= 35:
                    frequency = "monthly"
                elif 85 <= mode_days <= 95:
                    frequency = "quarterly"
                elif 350 <= mode_days <= 380:
                    frequency = "yearly"
                else:
                    frequency = f"approximately every {int(mode_days)} days"
            elif avg_days is not None:
                if avg_days <= 1:
                    frequency = "daily"
                elif 5 <= avg_days <= 10:
                    frequency = "weekly"
                elif 25 <= avg_days <= 35:
                    frequency = "monthly"
                elif 85 <= avg_days <= 95:
                    frequency = "quarterly"
                elif 350 <= avg_days <= 380:
                    frequency = "yearly"
                else:
                    frequency = f"approximately every {int(avg_days)} days"
    
    # Get revisions
    revisions = data_tracker.get_revision_history(supabase, table_name, limit=5)
    revision_count = 0
    
    if revisions:
        # Get total revision count
        rev_count_sql = f"""
        SELECT COUNT(*) as total_revisions
        FROM data_revisions
        WHERE dataset = '{table_name}';
        """
        rev_count_result = supabase.postgrest.rpc('exec_sql', {'query': rev_count_sql}).execute()
        if rev_count_result.data:
            revision_count = rev_count_result.data[0]['total_revisions']
    
    return {
        'count': stats['record_count'],
        'date_range': (stats['min_date'], stats['max_date']),
        'value_columns': value_columns,
        'value_types': value_types,
        'latest_values': latest_values,
        'frequency': frequency,
        'revision_count': revision_count
    }

def view_latest_data(supabase, dataset_name, limit=10, plot=False):
    """
    View the latest data for a specific dataset.
    """
    # Get latest data
    result = supabase.table(dataset_name) \
        .select('*') \
        .order('date', desc=True) \
        .limit(limit) \
        .execute()
        
    if not result.data:
        print(f"No data found for {dataset_name}")
        return
        
    # Convert to dataframe and display
    df = pd.DataFrame(result.data)
    print(f"\nLatest {limit} records for {dataset_name}:")
    print(df)
    
    # Get revision history - get raw data as count(*) won't work
    revisions = []
    try:
        rev_result = supabase.table('data_revisions')\
            .select('*')\
            .eq('dataset', dataset_name)\
            .order('revision_date', desc=True)\
            .limit(5)\
            .execute()
        
        if rev_result.data:
            revisions = rev_result.data
    except Exception as e:
        print(f"\nCouldn't get revision history: {e}")
    
    if revisions:
        print(f"\nRecent revisions for {dataset_name}:")
        for rev in revisions:
            print(f"Date: {rev['data_date']}, Field: {rev['value_field']}, " 
                  f"Old: {rev['old_value']}, New: {rev['new_value']}, "
                  f"Revision Date: {rev['revision_date']}")
    else:
        print(f"\nNo revisions found for {dataset_name}")
    
    # Plot data if requested
    if plot:
        # Get all data for plotting
        plot_result = supabase.table(dataset_name) \
            .select('*') \
            .order('date') \
            .execute()
            
        if plot_result.data:
            plot_df = pd.DataFrame(plot_result.data)
            plot_df['date'] = pd.to_datetime(plot_df['date'])
            
            # Plot all value columns
            value_cols = [col for col in plot_df.columns if col not in ['id', 'date']]
            
            if value_cols:
                plt.figure(figsize=(12, 6))
                for col in value_cols:
                    plt.plot(plot_df['date'], plot_df[col], label=col)
                
                plt.title(f"{dataset_name} Data")
                plt.xlabel("Date")
                plt.ylabel("Value")
                plt.legend()
                plt.grid(True)
                plt.tight_layout()
                plt.show()

def list_datasets(supabase):
    """
    List all available datasets with detailed summaries.
    """
    tables = get_all_tables(supabase)
    
    if not tables:
        print("No datasets found in the database.")
        return
    
    print(f"Found {len(tables)} datasets:")
    
    # Table for summary display
    summary_data = []
    
    for table in tables:
        print(f"\nGetting information for {table}...")
        
        try:
            # Get data using individual queries - PostgREST doesn't allow aggregate functions
            # Get all records to analyze
            result = supabase.table(table).select('*').execute()
            
            if result.data and len(result.data) > 0:
                # Process the data in Python
                df = pd.DataFrame(result.data)
                
                # Calculate statistics
                count = len(df)
                
                # Convert date strings to datetime objects
                df['date'] = pd.to_datetime(df['date'])
                
                min_date = df['date'].min().strftime('%Y-%m-%d')
                max_date = df['date'].max().strftime('%Y-%m-%d')
                
                # Get column names
                columns = df.columns.tolist()
                value_columns = [c for c in columns if c not in ['id', 'date']]
                
                # Get latest values
                latest_row = df.loc[df['date'] == df['date'].max()]
                latest_values = {col: latest_row[col].iloc[0] for col in value_columns}
                
                # Get revision count
                rev_count = 0
                try:
                    rev_result = supabase.table('data_revisions').select('id').eq('dataset', table).execute()
                    if rev_result.data:
                        rev_count = len(rev_result.data)
                except:
                    # Ignore errors with revisions table
                    pass
                
                # Estimate frequency by calculating average days between dates
                frequency = "unknown"
                if count > 1:
                    # Sort dates and get differences
                    sorted_dates = sorted(df['date'].unique())
                    if len(sorted_dates) > 1:
                        # Calculate differences between consecutive dates
                        diffs = [(sorted_dates[i+1] - sorted_dates[i]).days 
                                for i in range(len(sorted_dates)-1)]
                        avg_diff = sum(diffs) / len(diffs)
                        
                        # Determine frequency based on average difference
                        if avg_diff <= 1.5:
                            frequency = "daily"
                        elif 6 <= avg_diff <= 8:
                            frequency = "weekly"
                        elif 28 <= avg_diff <= 31:
                            frequency = "monthly"
                        elif 89 <= avg_diff <= 92:
                            frequency = "quarterly"
                        elif 350 <= avg_diff <= 380:
                            frequency = "yearly"
                        else:
                            frequency = f"~{int(avg_diff)} days"
                
                # Add to summary data
                summary_data.append({
                    'Dataset': table,
                    'Records': count,
                    'Date Range': f"{min_date} to {max_date}",
                    'Frequency': frequency,
                    'Columns': ', '.join(value_columns),
                    'Revisions': rev_count
                })
                
                print(f"  - {table}: {count} records from {min_date} to {max_date}")
                print(f"    Frequency: {frequency}")
                print(f"    Value columns: {', '.join(value_columns)}")
                
                if latest_values:
                    print(f"    Latest values: {latest_values}")
                
                print(f"    Revision history: {rev_count} revisions tracked")
            else:
                print(f"  - {table}: No data found")
        except Exception as e:
            print(f"  - {table}: Error retrieving information: {e}")
    
    if summary_data:
        # Create a summary DataFrame
        summary_df = pd.DataFrame(summary_data)
        print("\nDatasets Summary:")
        print(summary_df[['Dataset', 'Records', 'Date Range', 'Frequency', 'Revisions']])

def main():
    parser = argparse.ArgumentParser(description='View Economic Data')
    parser.add_argument('--dataset', '-d', help='Dataset name to view')
    parser.add_argument('--limit', '-l', type=int, default=10, help='Number of records to show')
    parser.add_argument('--list', '-ls', action='store_true', help='List all available datasets')
    parser.add_argument('--plot', '-p', action='store_true', help='Plot the dataset')
    parser.add_argument('--initialize', '-i', action='store_true', help='Initialize database tables')
    
    args = parser.parse_args()
    
    try:
        # Get Supabase client
        supabase = get_supabase_client()
        
        # Initialize tables if requested
        if args.initialize:
            print("Initializing database tables...")
            from models import initialize_tables
            initialize_tables(supabase)
            print("Tables initialized. Run again with --list to see available datasets.")
            return
            
        if args.list:
            list_datasets(supabase)
        elif args.dataset:
            # Check if dataset exists
            tables = get_all_tables(supabase)
            if args.dataset not in tables:
                print(f"Error: Dataset '{args.dataset}' not found.")
                if tables:
                    print("Available datasets:")
                    for name in tables:
                        print(f"  - {name}")
                else:
                    print("No datasets found. Run with --initialize to set up database tables.")
                return
                
            view_latest_data(supabase, args.dataset, args.limit, args.plot)
        else:
            parser.print_help()
            
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check that your .env file exists with correct SUPABASE_URL and SUPABASE_KEY")
        print("2. Ensure you have an active internet connection")
        print("3. Run with --initialize to create necessary database tables")
        print("4. Run 'python main.py' to populate tables with data first")

if __name__ == '__main__':
    main()