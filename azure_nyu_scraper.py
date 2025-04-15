"""
Module for fetching NYU Stern Equity Risk Premium data.
Azure-adapted version.
"""
import logging
import pandas as pd
import requests
import json
from io import BytesIO
from datetime import datetime
from typing import Optional, Dict, Any

from azure_connector import AzureConnector
from azure_data_tracker import smart_update

logger = logging.getLogger('nyu_scraper')

class NYUSternScraper:
    """
    Scraper for NYU Stern Equity Risk Premium data.
    """
    
    def __init__(self, azure_connector: AzureConnector, config: Dict[str, Any]):
        """
        Initialize the NYU Stern scraper.
        
        Args:
            azure_connector: Azure connector instance
            config: Scraper configuration
        """
        self.azure = azure_connector
        self.table_name = config['table_name']
        self.url = config['url']
        self.sheet_name = config['sheet_name']
        
    def create_table(self) -> None:
        """Create the database table if it doesn't exist"""
        self.azure.create_table(self.table_name)

    def download_excel(self) -> Optional[bytes]:
        """Download Excel file from specified URL"""
        try:
            # First check if we have cached data in blob storage
            container_name = "raw-files"
            blob_name = "NYU_ERP.xlsx"
            
            # Ensure container exists
            self.azure.create_container(container_name)
            
            # Try to download from blob storage first
            excel_content = self.azure.download_blob(container_name, blob_name)
            
            if excel_content:
                logger.info("Retrieved NYU Stern data from blob storage")
                return excel_content
                
            # If not in blob storage, download from URL
            response = requests.get(self.url)
            response.raise_for_status()
            excel_content = response.content
            
            # Save to blob storage for future use
            self.azure.upload_blob(container_name, blob_name, excel_content)
            logger.info("Downloaded NYU Stern data from URL and saved to blob storage")
            
            return excel_content
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
        
        # Use Azure data tracker's smart update instead of Supabase
        smart_update(
            azure_connector=self.azure,
            dataset_name=self.table_name,
            data_df=data,
            date_field='date',
            value_fields=['tbond_rate', 'erp_t12m', 'expected_return']
        )
    
    def update_last_run(self, dataset_name: str) -> None:
        """Update timestamp of last scraper run"""
        self.azure.update_last_run(dataset_name)
    
    def get_last_run(self, dataset_name: str) -> Optional[datetime]:
        """Get timestamp of last scraper run"""
        return self.azure.get_last_run(dataset_name)
    
    def should_update(self, dataset_name: str, update_frequency_hours: int = 24) -> bool:
        """Check if dataset should be updated based on last update time"""
        return self.azure.should_update(dataset_name, update_frequency_hours)