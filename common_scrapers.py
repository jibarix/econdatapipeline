"""
Common scraper implementations for Economic Development Bank data.
Provides base classes for monthly and quarterly data patterns.
"""
from typing import Optional, Dict, Any, List
import pandas as pd
from datetime import datetime
import requests
from io import BytesIO
from supabase import Client
import data_tracker

class BaseEDBScraper:
    """Base class for Economic Development Bank scrapers"""
    def __init__(self, supabase: Client):
        self.supabase = supabase

    def create_table(self) -> None:
        """Create database table if it doesn't exist"""
        pass

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw data into a standardized format"""
        pass

    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        pass

    def download_excel(self, url: str, file_name: str) -> Optional[bytes]:
        """Download Excel file from specified URL"""
        try:
            response = requests.get(url + file_name)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"Download error: {e}")
            return None

    def extract_data(self, excel_content: bytes, sheet_name: str, 
                     data_location: str) -> Optional[pd.DataFrame]:
        """Extract data from specific location in Excel file"""
        try:
            df = pd.read_excel(BytesIO(excel_content), sheet_name=sheet_name, header=None)
            start_cell, end_cell = data_location.split(":")
            start_row = int(start_cell[1:]) - 1
            start_col = ord(start_cell[0].upper()) - ord('A')
            end_row = int(end_cell[1:]) - 1
            end_col = ord(end_cell[0].upper()) - ord('A')
            return df.iloc[start_row:end_row + 1, start_col:end_col + 1]
        except Exception as e:
            print(f"Extraction error: {e}")
            return None

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


class MonthlyDataScraper(BaseEDBScraper):
    """
    Generic scraper for monthly data that follows the common EDB pattern.
    
    This handles data where:
    - Data is organized by months (rows) and fiscal years (columns)
    - First row contains fiscal year headers
    - First column contains month names
    - Data follows the fiscal year pattern (July-June)
    """
    
    def __init__(self, supabase: Client, config: Dict[str, Any]):
        super().__init__(supabase)
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        self.create_table_sql = config['create_table_sql']
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        for statement in self.create_table_sql.split(';'):
            if statement.strip():
                self.supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw data into standardized format"""
        # Set the fiscal years as column headers
        df.columns = ['Month'] + [int(year) for year in df.iloc[0, 1:]]
        df = df.iloc[1:].reset_index(drop=True)
        
        # Melt the dataframe to transform from wide to long format
        df_melted = pd.melt(df, id_vars=['Month'], var_name='Year', value_name=self.value_column)
        
        # Create dates from month names and fiscal years
        df_melted['Date'] = df_melted.apply(self._create_date, axis=1)
        df_melted = df_melted.dropna(subset=['Date'])
        df_melted = df_melted.sort_values(by='Date').reset_index(drop=True)
        
        # Convert values to the appropriate type
        if self.value_type == 'int':
            df_melted[self.value_column] = pd.to_numeric(df_melted[self.value_column], errors='coerce')
            df_melted = df_melted.dropna(subset=[self.value_column])
            df_melted[self.value_column] = df_melted[self.value_column].round().astype(int)
        else:  # float
            df_melted[self.value_column] = pd.to_numeric(df_melted[self.value_column], errors='coerce')
            df_melted = df_melted.dropna(subset=[self.value_column])
        
        return df_melted[['Date', self.value_column]]
    
    def _create_date(self, row: pd.Series) -> Optional[pd.Timestamp]:
        """
        Create proper dates based on month name and fiscal year.
        
        For Economic Development Bank Puerto Rico data:
        - July-December: use the year before fiscal year
        - January-June: use the same year as fiscal year
        """
        month_mapping = {
            'July': 7, 'August': 8, 'September': 9, 'October': 10,
            'November': 11, 'December': 12, 'January': 1, 'February': 2,
            'March': 3, 'April': 4, 'May': 5, 'June': 6
        }
        month_num = month_mapping.get(row['Month'])
        if not month_num:
            return None
            
        year = int(row['Year'])
        if month_num >= 7:  # July through December
            return pd.to_datetime(f'{year - 1}-{month_num}-01')  # Use year BEFORE fiscal year
        else:  # January through June
            return pd.to_datetime(f'{year}-{month_num}-01')  # Use same year as fiscal year
    
    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
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


class QuarterlyDataScraper(BaseEDBScraper):
    """
    Generic scraper for quarterly data that follows the common EDB pattern.
    
    This handles data where:
    - Data is organized by quarters (rows) and fiscal years (columns)
    - First row contains fiscal year headers
    - First column contains quarter names (e.g., "Jul-Sep")
    - Data follows the fiscal year pattern (July-June)
    """
    
    def __init__(self, supabase: Client, config: Dict[str, Any]):
        super().__init__(supabase)
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        self.create_table_sql = config['create_table_sql']
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        for statement in self.create_table_sql.split(';'):
            if statement.strip():
                self.supabase.postgrest.rpc('exec_sql', {'query': statement}).execute()

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw data into standardized format"""
        # Set the fiscal year as column headers
        fiscal_years = df.iloc[0, 1:].astype(int)
        df.columns = ['Quarter'] + list(fiscal_years)
        
        # Clean up the quarters data
        df = df.iloc[1:].copy()
        df['Quarter'] = df['Quarter'].str.strip()
        
        # Melt the dataframe
        df_melted = pd.melt(df, id_vars=['Quarter'], var_name='Year', value_name=self.value_column)
        
        # Create proper dates
        df_melted['Date'] = df_melted.apply(self._create_date, axis=1)
        
        # Clean and sort
        df_melted = df_melted.dropna(subset=['Date'])
        df_melted[self.value_column] = pd.to_numeric(df_melted[self.value_column], errors='coerce')
        df_melted = df_melted.dropna(subset=[self.value_column])
        df_melted = df_melted.sort_values(by='Date').reset_index(drop=True)
        
        return df_melted[['Date', self.value_column]]
    
    def _create_date(self, row: pd.Series) -> Optional[pd.Timestamp]:
        """
        Create proper dates for quarterly data based on quarter and fiscal year.
        
        For Economic Development Bank Puerto Rico data:
        - Jul-Sep: 1st quarter (Q1) of fiscal year - use first day of next month (Oct 1)
        - Oct-Dec: 2nd quarter (Q2) of fiscal year - use first day of next month (Jan 1)
        - Jan-Mar: 3rd quarter (Q3) of fiscal year - use first day of next month (Apr 1)
        - Apr-Jun: 4th quarter (Q4) of fiscal year - use first day of next month (Jul 1)
        
        This aligns with the first-of-month pattern used in monthly data.
        """
        quarter_map = {
            'Jul-Sep': ('10-01', -1),  # (month-day, year offset from fiscal year)
            'Oct-Dec': ('01-01', 0),   # Note: Jan 1 of the fiscal year
            'Jan-Mar': ('04-01', 0),
            'Apr-Jun': ('07-01', 0)
        }
        
        if row['Quarter'] not in quarter_map:
            return None
        
        month_day, year_offset = quarter_map[row['Quarter']]
        fiscal_year = int(row['Year'])
        calendar_year = fiscal_year + year_offset
        
        return pd.to_datetime(f'{calendar_year}-{month_day}')
        
    def insert_data(self, data: pd.DataFrame) -> None:
        """Insert processed data into database"""
        # Convert column name to lowercase with underscore format
        # Fix: ensure proper camelCase to snake_case conversion
        column_name = ''.join(['_'+i.lower() if i.isupper() else i.lower() for i in self.value_column]).lstrip('_')
        
        # For IndividualLoans specifically, ensure it becomes individual_loans
        if self.value_column == 'IndividualLoans':
            column_name = 'individual_loans'
        
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