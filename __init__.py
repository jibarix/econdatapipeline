"""
Economic Data Pipeline for Azure Automation.
This package contains modules for collecting economic data from various sources.
"""

__version__ = "0.1.0"

# Import main classes and functions for easy access
from .azure_connector import AzureConnector
from .azure_data_tracker import smart_update, get_revision_history
from .azure_common_scrapers import BaseEDBScraper, MonthlyDataScraper, QuarterlyDataScraper
from .azure_fred_scraper import FREDScraper
from .azure_nyu_scraper import NYUSternScraper
from .main_azure import main