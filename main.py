"""
Main application for scraping Economic Development Bank data and FRED API data.
"""
import logging
from models import get_supabase_client, initialize_tables
from common_scrapers import MonthlyDataScraper, QuarterlyDataScraper
from fred_scraper import FREDScraper
from config import SCRAPER_CONFIGS, BASE_URL, TABLES_TO_CREATE
from fred_config import FRED_SCRAPER_CONFIGS, FRED_TABLES_TO_CREATE
from nyu_scraper import NYUSternScraper
from nyu_config import NYU_STERN_CONFIG, NYU_TABLES_TO_CREATE

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

def create_scraper(supabase, config):
    """
    Create the appropriate scraper instance based on the configuration.
    """
    if config['type'] == 'monthly':
        return MonthlyDataScraper(supabase, config)
    elif config['type'] == 'quarterly':
        return QuarterlyDataScraper(supabase, config)
    elif config['type'] == 'fred':
        return FREDScraper(supabase, config)
    else:
        raise ValueError(f"Unknown scraper type: {config['type']}")

def run_scraper(scraper, name, config):
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
                # If we get a column error, try to drop and recreate the table
                if "column" in str(e).lower() and "not found" in str(e).lower():
                    logger.warning(f"Column mismatch detected for {name}, recreating table...")
                    # Drop table
                    drop_sql = f"DROP TABLE IF EXISTS {config['table_name']};"
                    scraper.supabase.postgrest.rpc('exec_sql', {'query': drop_sql}).execute()
                    # Recreate table
                    scraper.create_table()
                    # Try insert again
                    scraper.insert_data(processed_df)
                    scraper.update_last_run(name)
                    logger.info(f"Successfully updated {name} after table recreation")
                    return 'updated'
                else:
                    raise
        else:
            logger.info(f"No update needed for {name} yet")
            return 'no_update_needed'
    except Exception as e:
        logger.exception(f"Error processing {name}: {str(e)}")
        return 'failed'

def run_edb_scrapers(supabase):
    """Run Economic Development Bank scrapers"""
    updated = []
    no_update_needed = []
    failed = []
    
    logger.info("Starting EDB data scraper")
    
    for name, config in SCRAPER_CONFIGS.items():
        logger.info(f"\n{'='*50}\nProcessing {name}...")
        
        try:
            scraper = create_scraper(supabase, config)
            
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

def run_fred_scrapers(supabase):
    """Run FRED API scrapers"""
    updated = []
    no_update_needed = []
    failed = []
    
    logger.info("Starting FRED data scraper")
    
    for name, config in FRED_SCRAPER_CONFIGS.items():
        logger.info(f"\n{'='*50}\nProcessing {name}...")
        
        try:
            scraper = create_scraper(supabase, config)
            
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

def run_nyu_stern_scraper(supabase):
    """Run NYU Stern ERP scraper"""
    updated = []
    no_update_needed = []
    failed = []
    
    logger.info("Starting NYU Stern ERP data scraper")
    
    config = NYU_STERN_CONFIG
    name = 'equity_risk_premium'
    
    logger.info(f"\n{'='*50}\nProcessing {name}...")
    
    try:
        scraper = NYUSternScraper(supabase, config)
        
        # Create the table if it doesn't exist
        scraper.create_table()
        
        # Process the data
        processed_df = scraper.process_data()
        if processed_df.empty:
            logger.warning(f"No data found for {name}")
            failed.append(name)
            return updated, no_update_needed, failed
            
        logger.info(f"\nLatest available data for {name}:")
        logger.info(processed_df.tail())
        
        # Update data if needed
        if scraper.should_update(name):
            logger.info(f"\nUpdating {name}...")
            try:
                scraper.insert_data(processed_df)
                scraper.update_last_run(name)
                logger.info(f"Successfully updated {name}")
                updated.append(name)
            except Exception as e:
                logger.exception(f"Error updating {name}: {str(e)}")
                failed.append(name)
        else:
            logger.info(f"No update needed for {name} yet")
            no_update_needed.append(name)
            
    except Exception as e:
        logger.exception(f"Error processing {name}: {str(e)}")
        failed.append(name)
    
    return updated, no_update_needed, failed

def main():
    """
    Main function to run all scrapers.
    """
    # Initialize Supabase client and database tables
    supabase = get_supabase_client()
    initialize_tables(supabase)
    
    # Add NYU tables to create
    global TABLES_TO_CREATE
    TABLES_TO_CREATE.extend(NYU_TABLES_TO_CREATE)
    
    # Run EDB scrapers
    edb_updated, edb_no_update, edb_failed = run_edb_scrapers(supabase)
    
    # Run FRED scrapers
    fred_updated, fred_no_update, fred_failed = run_fred_scrapers(supabase)
    
    # Run NYU Stern ERP scraper
    nyu_updated, nyu_no_update, nyu_failed = run_nyu_stern_scraper(supabase)
    
    # Combine results
    all_updated = edb_updated + fred_updated + nyu_updated
    all_no_update = edb_no_update + fred_no_update + nyu_no_update
    all_failed = edb_failed + fred_failed + nyu_failed
    
    # Log summary
    logger.info("\n\n" + "="*50)
    logger.info(f"Scraping complete.")
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

if __name__ == '__main__':
    main()