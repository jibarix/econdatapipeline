"""
Test script to verify the quarterly date adjustment in the FRED scraper.
Run this script to validate the fix before deploying to production.
"""
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import logging

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_fred_quarterly')

# Load environment variables
load_dotenv()

# Import the modified FREDScraper
from fred_scraper import FREDScraper
from models import get_supabase_client

def test_quarterly_date_adjustment():
    """Test the quarterly date adjustment functionality"""
    # Create a sample dataframe with quarterly dates
    test_dates = [
        '2024-01-01',  # Q1
        '2024-04-01',  # Q2
        '2024-07-01',  # Q3
        '2024-10-01',  # Q4
        '2025-01-01',  # Q1
    ]
    test_values = [100, 110, 120, 130, 140]
    
    test_df = pd.DataFrame({
        'Date': pd.to_datetime(test_dates),
        'Value': test_values
    })
    
    # Create a mock config
    mock_config = {
        'table_name': 'test_quarterly',
        'value_column': 'Value',
        'value_type': 'float',
        'create_table_sql': '',
        'fred_series_id': 'TEST',
        'frequency': 'q'  # Quarterly
    }
    
    # Initialize the FREDScraper with a None supabase client (we won't use it)
    scraper = FREDScraper(None, mock_config)
    
    # Apply the quarterly date adjustment
    adjusted_dates = test_df['Date'].apply(scraper._adjust_quarterly_date)
    
    # Expected end-of-quarter dates
    expected_dates = [
        '2024-03-31',  # Q1 end
        '2024-06-30',  # Q2 end
        '2024-09-30',  # Q3 end
        '2024-12-31',  # Q4 end
        '2025-03-31',  # Q1 end
    ]
    expected_dates = pd.to_datetime(expected_dates)
    
    # Compare the adjusted dates with expected
    comparison = adjusted_dates == expected_dates
    
    if comparison.all():
        logger.info("✓ Quarterly date adjustment works correctly!")
        logger.info("Original dates vs Adjusted dates:")
        for i, (orig, adj) in enumerate(zip(test_df['Date'], adjusted_dates)):
            logger.info(f"  {orig.strftime('%Y-%m-%d')} -> {adj.strftime('%Y-%m-%d')}")
    else:
        logger.error("✗ Quarterly date adjustment failed!")
        logger.error("Comparison:")
        for i, (orig, adj, exp) in enumerate(zip(test_df['Date'], adjusted_dates, expected_dates)):
            status = "✓" if adj == exp else "✗"
            logger.error(f"  {status} {orig.strftime('%Y-%m-%d')} -> {adj.strftime('%Y-%m-%d')} (expected: {exp.strftime('%Y-%m-%d')})")

def test_real_fred_data():
    """Test with real FRED data series that is known to be quarterly"""
    # Initialize Supabase client
    supabase = get_supabase_client()
    
    # Quarterly FRED series - GDP (GDPC1)
    config = {
        'table_name': 'real_gdp',
        'value_column': 'Value',
        'value_type': 'float',
        'create_table_sql': '',
        'fred_series_id': 'GDPC1',  # Real Gross Domestic Product
        'frequency': 'q'  # Quarterly
    }
    
    scraper = FREDScraper(supabase, config)
    
    # Fetch data without processing first
    raw_df = scraper.fetch_fred_data()
    
    if raw_df is None or raw_df.empty:
        logger.error("Failed to fetch FRED data for testing")
        return
    
    # Get a few rows for demonstration
    sample_raw = raw_df.head(5).copy()
    
    # Now process with date adjustment
    processed_df = scraper.process_data(raw_df.copy())
    
    # Extract the same sample rows
    sample_dates = sample_raw['Date'].tolist()
    sample_processed = processed_df[processed_df['Date'].dt.year.isin([d.year for d in sample_dates])].head(5)
    
    # Display comparison
    logger.info("\nReal FRED Quarterly Data Test:")
    logger.info("Raw FRED dates vs Adjusted dates:")
    
    for i, (raw_date, raw_val) in enumerate(zip(sample_raw['Date'], sample_raw[config['value_column']])):
        if i < len(sample_processed):
            proc_date = sample_processed['Date'].iloc[i]
            proc_val = sample_processed[config['value_column']].iloc[i]
            logger.info(f"  {raw_date.strftime('%Y-%m-%d')} -> {proc_date.strftime('%Y-%m-%d')}")
            
            # Calculate days difference to verify it's roughly a quarter end
            days_diff = (proc_date - raw_date).days
            if 88 <= days_diff <= 92:  # Approximately 3 months (quarter)
                logger.info(f"    ✓ Properly adjusted by ~90 days ({days_diff} days)")
            else:
                logger.info(f"    ✗ Unexpected adjustment ({days_diff} days)")

if __name__ == "__main__":
    logger.info("===== Testing FRED Quarterly Date Adjustment =====")
    test_quarterly_date_adjustment()
    
    logger.info("\n===== Testing with Real FRED Quarterly Data =====")
    test_real_fred_data()