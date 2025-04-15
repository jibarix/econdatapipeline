"""
Main application for scraping Economic Development Bank data and FRED API data.
Azure-adapted version that uses Azure Storage instead of Supabase.
"""
import os
import logging
import json
from datetime import datetime
from typing import Dict, List, Tuple, Any
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import Azure connector and scrapers
from azure_connector import AzureConnector
from azure_common_scrapers import MonthlyDataScraper, QuarterlyDataScraper
from azure_fred_scraper import FREDScraper
from azure_nyu_scraper import NYUSternScraper

# Import configurations
from config import SCRAPER_CONFIGS, BASE_URL
from fred_config import FRED_SCRAPER_CONFIGS
from nyu_config import NYU_STERN_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger('data_scraper')

def create_scraper(azure: AzureConnector, config: Dict[str, Any]):
    """
    Create the appropriate scraper instance based on the configuration.
    """
    if config['type'] == 'monthly':
        return MonthlyDataScraper(azure, config)
    elif config['type'] == 'quarterly':
        return QuarterlyDataScraper(azure, config)
    elif config['type'] == 'fred':
        return FREDScraper(azure, config)
    elif config['type'] == 'nyu_stern':
        return NYUSternScraper(azure, config)
    else:
        raise ValueError(f"Unknown scraper type: {config['type']}")

def run_scraper(scraper, name: str, config: Dict[str, Any]) -> str:
    """
    Run a scraper and handle any errors.
    
    Returns:
        str: Status of the scraper run - 'updated', 'no_update_needed', or 'failed'
    """
    try:
        # Check if tables need to be created
        scraper.create_table()
        
        # Handle FRED scrapers differently since they fetch data directly
        if config['type'] == 'fred':
            # Fetch and process data directly from FRED API
            processed_df = scraper.process_data()
            if processed_df.empty:
                logger.warning(f"No data found for {name}")
                return 'failed'
                
            logger.info(f"\nLatest available data for {name}:")
            logger.info(processed_df.head())
        # Handle NYU Stern scraper
        elif config['type'] == 'nyu_stern':
            # Process data directly
            processed_df = scraper.process_data()
            if processed_df.empty:
                logger.warning(f"No data found for {name}")
                return 'failed'
                
            logger.info(f"\nLatest available data for {name}:")
            logger.info(processed_df.head())
        else:
            # Always download and show latest data for EDB scrapers
            excel_content = scraper.download_excel(BASE_URL, config['file_name'])
            if not excel_content:
                logger.error(f"Failed to download file for {name}")
                return 'failed'

            # Extract data from specific sheet and location
            df = scraper.extract_data(
                excel_content, 
                config['sheet_name'], 
                config['data_location']
            )
            if df is None:
                logger.error(f"Failed to extract data for {name}")
                return 'failed'

            # Process the data
            processed_df = scraper.process_data(df)
            if processed_df.empty:
                logger.warning(f"No data found for {name}")
                return 'failed'

            logger.info(f"\nLatest available data for {name}:")
            logger.info(processed_df.tail())
            
        # Update data if needed
        if scraper.should_update(name):
            logger.info(f"\nUpdating {name}...")
            try:
                scraper.insert_data(processed_df)
                scraper.update_last_run(name)
                logger.info(f"Successfully updated {name}")
                return 'updated'
            except Exception as e:
                logger.exception(f"Error updating {name}: {str(e)}")
                return 'failed'
        else:
            logger.info(f"No update needed for {name} yet")
            return 'no_update_needed'
    except Exception as e:
        logger.exception(f"Error processing {name}: {str(e)}")
        return 'failed'

def run_edb_scrapers(azure: AzureConnector) -> Tuple[List[str], List[str], List[str]]:
    """Run Economic Development Bank scrapers"""
    updated = []
    no_update_needed = []
    failed = []
    
    logger.info("Starting EDB data scraper")
    
    for name, config in SCRAPER_CONFIGS.items():
        logger.info(f"\n{'='*50}\nProcessing {name}...")
        
        try:
            scraper = create_scraper(azure, config)
            
            status = run_scraper(scraper, name, config)
            if status == 'updated':
                updated.append(name)
            elif status == 'no_update_needed':
                no_update_needed.append(name)
            else:
                failed.append(name)
                
        except Exception as e:
            logger.exception(f"Error setting up {name}: {str(e)}")
            failed.append(name)
    
    return updated, no_update_needed, failed

def run_fred_scrapers(azure: AzureConnector) -> Tuple[List[str], List[str], List[str]]:
    """Run FRED API scrapers"""
    updated = []
    no_update_needed = []
    failed = []
    
    logger.info("Starting FRED data scraper")
    
    for name, config in FRED_SCRAPER_CONFIGS.items():
        logger.info(f"\n{'='*50}\nProcessing {name}...")
        
        try:
            scraper = create_scraper(azure, config)
            
            status = run_scraper(scraper, name, config)
            if status == 'updated':
                updated.append(name)
            elif status == 'no_update_needed':
                no_update_needed.append(name)
            else:
                failed.append(name)
                
        except Exception as e:
            logger.exception(f"Error setting up {name}: {str(e)}")
            failed.append(name)
    
    return updated, no_update_needed, failed

def run_nyu_stern_scraper(azure: AzureConnector) -> Tuple[List[str], List[str], List[str]]:
    """Run NYU Stern ERP scraper"""
    updated = []
    no_update_needed = []
    failed = []
    
    logger.info("Starting NYU Stern ERP data scraper")
    
    config = NYU_STERN_CONFIG
    name = 'equity_risk_premium'
    
    logger.info(f"\n{'='*50}\nProcessing {name}...")
    
    try:
        scraper = create_scraper(azure, config)
        
        status = run_scraper(scraper, name, config)
        if status == 'updated':
            updated.append(name)
        elif status == 'no_update_needed':
            no_update_needed.append(name)
        else:
            failed.append(name)
            
    except Exception as e:
        logger.exception(f"Error processing {name}: {str(e)}")
        failed.append(name)
    
    return updated, no_update_needed, failed

def save_run_summary(azure: AzureConnector, summary: Dict[str, Any]) -> None:
    """Save the run summary to blob storage"""
    try:
        # Convert summary to JSON
        summary_json = json.dumps(summary, indent=2, default=str)
        
        # Create a timestamp for the filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Upload to blob storage
        container_name = "logs"
        blob_name = f"run_summary_{timestamp}.json"
        
        # Ensure container exists
        azure.create_container(container_name)
        
        # Upload summary
        azure.upload_blob(container_name, blob_name, summary_json)
        logger.info(f"Saved run summary to {container_name}/{blob_name}")
    except Exception as e:
        logger.error(f"Error saving run summary: {e}")

def main() -> Dict[str, Any]:
    """
    Main function to run all scrapers.
    
    Returns:
        Dict containing summary of the run
    """
    start_time = datetime.utcnow()
    
    # Initialize Azure connector
    # In production with Managed Identity:
    # azure = AzureConnector(use_managed_identity=True, key_vault_url=os.environ.get("AZURE_KEY_VAULT_URL"))
    
    # For local development:
    azure = AzureConnector(use_managed_identity=False, key_vault_url=os.environ.get("AZURE_KEY_VAULT_URL"))
    
    # Initialize tables and containers
    logger.info("Initializing Azure Storage tables and containers")
    azure.initialize_tables()
    azure.initialize_containers()
    
    # Run EDB scrapers
    edb_updated, edb_no_update, edb_failed = run_edb_scrapers(azure)
    
    # Run FRED scrapers
    fred_updated, fred_no_update, fred_failed = run_fred_scrapers(azure)
    
    # Run NYU Stern ERP scraper
    nyu_updated, nyu_no_update, nyu_failed = run_nyu_stern_scraper(azure)
    
    # Combine results
    all_updated = edb_updated + fred_updated + nyu_updated
    all_no_update = edb_no_update + fred_no_update + nyu_no_update
    all_failed = edb_failed + fred_failed + nyu_failed
    
    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    
    # Create run summary
    summary = {
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration,
        "total_datasets": len(all_updated) + len(all_no_update) + len(all_failed),
        "updated": {
            "count": len(all_updated),
            "datasets": all_updated
        },
        "no_update_needed": {
            "count": len(all_no_update),
            "datasets": all_no_update
        },
        "failed": {
            "count": len(all_failed),
            "datasets": all_failed
        },
        "details": {
            "edb": {
                "updated": edb_updated,
                "no_update_needed": edb_no_update,
                "failed": edb_failed
            },
            "fred": {
                "updated": fred_updated,
                "no_update_needed": fred_no_update,
                "failed": fred_failed
            },
            "nyu": {
                "updated": nyu_updated,
                "no_update_needed": nyu_no_update,
                "failed": nyu_failed
            }
        }
    }
    
    # Log summary
    logger.info("\n\n" + "="*50)
    logger.info(f"Scraping complete in {duration:.2f} seconds.")
    logger.info(f"EDB: Updated: {len(edb_updated)}, No update needed: {len(edb_no_update)}, Failed: {len(edb_failed)}")
    logger.info(f"FRED: Updated: {len(fred_updated)}, No update needed: {len(fred_no_update)}, Failed: {len(fred_failed)}")
    logger.info(f"NYU: Updated: {len(nyu_updated)}, No update needed: {len(nyu_no_update)}, Failed: {len(nyu_failed)}")
    logger.info(f"TOTAL: Updated: {len(all_updated)}, No update needed: {len(all_no_update)}, Failed: {len(all_failed)}")
    
    if all_updated:
        logger.info(f"Updated scrapers: {', '.join(all_updated)}")
    if all_no_update:
        logger.info(f"No update needed: {', '.join(all_no_update)}")
    if all_failed:
        logger.error(f"Failed scrapers: {', '.join(all_failed)}")
    
    # Save summary to blob storage
    save_run_summary(azure, summary)
    
    return summary

if __name__ == '__main__':
    main()