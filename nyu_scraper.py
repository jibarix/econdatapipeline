"""
Module for fetching NYU Stern Equity Risk Premium data.
"""
import logging
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from typing import Optional, Dict, Any
from supabase import Client
import data_tracker

logger = logging.getLogger('nyu_scraper')

class NYUSternScraper:
    """
    Scraper for NYU Stern Equity Risk Premium data.
    """
    
    def __init__(self, supabase: Client, config: Dict[str, Any]):
        """
        Initialize the NYU Stern scraper.
        
        Args:
            supabase: Supabase client
            config: Scraper configuration
        """
        self.supabase = supabase
        self.table_name = config['table_name']
        self.url = config['url']
        self.sheet_name = config['sheet_name']
        self.create_table_sql = config['create_table_sql']
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        for statement in self.create_table_sql.split(';'):
            if statement.strip():
                self.supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()

    def download_excel(self) -> Optional[bytes]:
        """Download Excel file from specified URL"""
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            return response.content
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
            
            # Add debug log for the first few rows after conversion
            logger.info("Sample of processed data:")
            logger.info(df.head().to_string())
            
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
        
        # Use the data tracker's smart update
        data_tracker.smart_update(
            supabase=self.supabase,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=['tbond_rate', 'erp_t12m', 'expected_return']
        )
    
    def update_last_run(self, dataset_name: str) -> None:
        """Update timestamp of last scraper run"""
        timestamp = datetime.utcnow().isoformat()
        self.supabase.table('scraper_metadata').upsert({
            'dataset': dataset_name,
            'last_run': timestamp
        }).execute()
    
    def get_last_run(self, dataset_name: str) -> Optional[datetime]:
        """Get timestamp of last scraper run"""
        result = self.supabase.table('scraper_metadata')\
            .select('last_run')\
            .eq('dataset', dataset_name)\
            .execute()
        if result.data:
            # Use dateutil parser which is more flexible with ISO formats
            from dateutil import parser
            last_run = parser.parse(result.data[0]['last_run'])
            return last_run.replace(tzinfo=None)  # Strip timezone for comparison
        return None
    
    def should_update(self, dataset_name: str, update_frequency_hours: int = 24) -> bool:
        """Check if dataset should be updated based on last update time"""
        last_run = self.get_last_run(dataset_name)
        if not last_run:
            return True
        now = datetime.utcnow()
        hours_since_update = (now - last_run).total_seconds() / 3600
        return hours_since_update >= update_frequency_hours