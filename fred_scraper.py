"""
Module for fetching economic data from the FRED API with proper quarterly date handling.
"""
import os
import logging
import pandas as pd
import requests
from datetime import datetime
from typing import Optional, Dict, Any, List
from supabase import Client
import data_tracker

logger = logging.getLogger('fred_scraper')

class FREDScraper:
    """
    Scraper for FRED (Federal Reserve Economic Data) API.
    """
    
    def __init__(self, supabase: Client, config: Dict[str, Any]):
        """
        Initialize the FRED scraper.
        
        Args:
            supabase: Supabase client
            config: Scraper configuration
        """
        self.supabase = supabase
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        self.create_table_sql = config['create_table_sql']
        self.fred_series_id = config['fred_series_id']
        self.frequency = config.get('frequency', 'm')  # Default to monthly
        self.api_key = os.environ.get("FRED_API_KEY")
        
        # Import the start date from fred_config
        from fred_config import FRED_START_DATE
        self.start_date = FRED_START_DATE
        
        if not self.api_key:
            raise ValueError("FRED_API_KEY environment variable not set")
    
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        for statement in self.create_table_sql.split(';'):
            if statement.strip():
                self.supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()
    
    def fetch_fred_data(self, start_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetch data from FRED API.
        
        Args:
            start_date: Optional start date in YYYY-MM-DD format
            
        Returns:
            DataFrame with date and value columns or None if failed
        """
        base_url = "https://api.stlouisfed.org/fred/series/observations"
        
        params = {
            "series_id": self.fred_series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "frequency": self.frequency,  # Set from config
            "sort_order": "desc",
            "limit": 1000  # Get more historical data
        }
        
        # Use the provided start date, or fall back to the default one
        params["observation_start"] = start_date if start_date else self.start_date
            
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'observations' not in data:
                logger.error(f"No observations in FRED API response for {self.fred_series_id}")
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(data['observations'])
            
            # Rename columns and convert types
            df = df.rename(columns={'date': 'Date', 'value': self.value_column})
            df['Date'] = pd.to_datetime(df['Date'])
            
            # Handle cases where value is "." (missing data)
            df[self.value_column] = df[self.value_column].replace('.', None)
            df[self.value_column] = pd.to_numeric(df[self.value_column], errors='coerce')
            
            # Drop rows with missing values
            df = df.dropna(subset=[self.value_column])
            
            # Sort by date
            df = df.sort_values('Date').reset_index(drop=True)
            
            # Keep only essential columns
            return df[['Date', self.value_column]]
            
        except Exception as e:
            logger.exception(f"Error fetching data from FRED API for {self.fred_series_id}: {e}")
            return None
    
    def process_data(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Process raw data.
        Note: For FRED data, we already get processed data from the API.
        
        Args:
            df: Optional DataFrame (not used for FRED as we fetch directly)
            
        Returns:
            Processed DataFrame
        """
        # If no DataFrame is provided, fetch from FRED
        if df is None:
            df = self.fetch_fred_data()
            
        if df is None or df.empty:
            return pd.DataFrame(columns=['Date', self.value_column])
        
        # Apply date adjustment for quarterly data
        if self.frequency == 'q':
            df['Date'] = df['Date'].apply(self._adjust_quarterly_date)
            
        # Apply value type conversion if needed
        if self.value_type == 'int':
            df[self.value_column] = df[self.value_column].round().astype(int)
            
        return df
    
    def _adjust_quarterly_date(self, date):
        """
        Adjust quarterly dates to first-of-month after quarter ends.
        
        FRED returns the first day of the quarter (e.g., 2025-01-01 for Q1 2025),
        but we want the first day of the month after the quarter ends:
        - Q1 (Jan-Mar) -> Apr 1
        - Q2 (Apr-Jun) -> Jul 1
        - Q3 (Jul-Sep) -> Oct 1
        - Q4 (Oct-Dec) -> Jan 1 (of next year)
        
        This aligns with the first-of-month pattern used in monthly data.
        
        Args:
            date: pandas Timestamp with the first day of a quarter
                
        Returns:
            pandas Timestamp with the first day of month after quarter end
        """
        # Get quarter number (1-4)
        quarter = (date.month - 1) // 3 + 1
        
        # Map quarters to first day of next month
        if quarter == 1:  # Q1 (Jan-Mar)
            return pd.Timestamp(date.year, 4, 1)  # Apr 1
        elif quarter == 2:  # Q2 (Apr-Jun)
            return pd.Timestamp(date.year, 7, 1)  # Jul 1
        elif quarter == 3:  # Q3 (Jul-Sep)
            return pd.Timestamp(date.year, 10, 1)  # Oct 1
        else:  # Q4 (Oct-Dec)
            return pd.Timestamp(date.year + 1, 1, 1)  # Jan 1 of next year
    
    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        if data.empty:
            logger.warning(f"No data to insert for {self.fred_series_id}")
            return
            
        # Convert column name to lowercase with underscore format
        column_name = ''.join(['_'+i.lower() if i.isupper() else i.lower() for i in self.value_column]).lstrip('_')
        
        # Rename columns to match database schema
        data = data.rename(columns={'Date': 'date', self.value_column: column_name})
        
        # Format date as string for database
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        # Use the data tracker's smart update
        data_tracker.smart_update(
            supabase=self.supabase,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=[column_name]
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