"""
Common scraper implementations for Economic Development Bank data.
Provides base classes for monthly and quarterly data patterns.
Azure-adapted version.
"""
from typing import Optional, Dict, Any, List
import pandas as pd
from datetime import datetime
import requests
from io import BytesIO
import logging

# Import Azure connector instead of Supabase
from azure_connector import AzureConnector

logger = logging.getLogger(__name__)

class BaseEDBScraper:
    """Base class for Economic Development Bank scrapers"""
    def __init__(self, azure_connector: AzureConnector):
        self.azure = azure_connector

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
            # First check if the file is already in blob storage
            blob_name = file_name
            container_name = "raw-files"
            
            # Ensure container exists
            self.azure.create_container(container_name)
            
            # Try to download from blob storage first
            excel_content = self.azure.download_blob(container_name, blob_name)
            
            if excel_content:
                logger.info(f"Retrieved {file_name} from blob storage")
                return excel_content
            
            # If not in blob storage, download from URL
            response = requests.get(url + file_name)
            response.raise_for_status()
            excel_content = response.content
            
            # Save to blob storage for future use
            self.azure.upload_blob(container_name, blob_name, excel_content)
            logger.info(f"Downloaded {file_name} from URL and saved to blob storage")
            
            return excel_content
        except Exception as e:
            logger.error(f"Download error: {e}")
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
            logger.error(f"Extraction error: {e}")
            return None

    def update_last_run(self, dataset_name: str) -> None:
        """Update timestamp of last scraper run"""
        self.azure.update_last_run(dataset_name)

    def get_last_run(self, dataset_name: str) -> Optional[datetime]:
        """Get timestamp of last scraper run"""
        return self.azure.get_last_run(dataset_name)

    def should_update(self, dataset_name: str, update_frequency_hours: int = 24) -> bool:
        """Check if dataset should be updated based on last update time"""
        return self.azure.should_update(dataset_name, update_frequency_hours)


class MonthlyDataScraper(BaseEDBScraper):
    """
    Generic scraper for monthly data that follows the common EDB pattern.
    
    This handles data where:
    - Data is organized by months (rows) and fiscal years (columns)
    - First row contains fiscal year headers
    - First column contains month names
    - Data follows the fiscal year pattern (July-June)
    """
    
    def __init__(self, azure_connector: AzureConnector, config: Dict[str, Any]):
        super().__init__(azure_connector)
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        # No need for create_table_sql in Azure implementation
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        # Use Azure connector to create table
        self.azure.create_table(self.table_name)

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
        if data.empty:
            logger.warning(f"No data to insert for {self.table_name}")
            return
            
        # Convert column name to lowercase with underscore format
        column_name = ''.join(['_'+i.lower() if i.isupper() else i.lower() for i in self.value_column]).lstrip('_')
        
        # Rename columns to match database schema
        data = data.rename(columns={'Date': 'date', self.value_column: column_name})
        
        # Format date as string for database
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        # Use Azure data tracker's smart update instead of Supabase
        from azure_data_tracker import smart_update
        
        smart_update(
            azure_connector=self.azure,
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
    
    def __init__(self, azure_connector: AzureConnector, config: Dict[str, Any]):
        super().__init__(azure_connector)
        self.table_name = config['table_name']
        self.value_column = config['value_column']
        self.value_type = config.get('value_type', 'float')
        # No need for create_table_sql in Azure implementation
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        # Use Azure connector to create table
        self.azure.create_table(self.table_name)

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
        if data.empty:
            logger.warning(f"No data to insert for {self.table_name}")
            return
            
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
        
        # Use Azure data tracker's smart update instead of Supabase
        from azure_data_tracker import smart_update
        
        smart_update(
            azure_connector=self.azure,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=[column_name]
        )