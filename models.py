from supabase import create_client, Client
import os
from dotenv import load_dotenv
import data_tracker
from config import TABLES_TO_CREATE
from fred_config import FRED_TABLES_TO_CREATE
from nyu_config import NYU_TABLES_TO_CREATE

def get_supabase_client() -> Client:
    """Initialize and return a Supabase client."""
    load_dotenv()
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def initialize_tables(supabase: Client) -> None:
    """Initialize all required database tables."""
    # Create metadata tracking table
    metadata_sql = """
        CREATE TABLE IF NOT EXISTS scraper_metadata (
            dataset VARCHAR PRIMARY KEY,
            last_run TIMESTAMP WITH TIME ZONE NOT NULL
        );
    """

    # Execute metadata table creation
    for statement in metadata_sql.split(';'):
        if statement.strip():
            supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()
    
    # Initialize revision tracking table
    data_tracker.initialize_revision_table(supabase)
    
    # Create EDB tables
    for table_sql in TABLES_TO_CREATE:
        for statement in table_sql.split(';'):
            if statement.strip():
                supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()
    
    # Create FRED tables
    for table_sql in FRED_TABLES_TO_CREATE:
        for statement in table_sql.split(';'):
            if statement.strip():
                supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()
                
    # Create NYU Stern tables
    for table_sql in NYU_TABLES_TO_CREATE:
        for statement in table_sql.split(';'):
            if statement.strip():
                supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()