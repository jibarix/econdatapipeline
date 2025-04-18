"""
Configuration for NYU Stern ERP data scraper.
"""

# SQL template for NYU Stern ERP data table
NYU_STERN_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS equityriskpremium (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    tbond_rate DECIMAL(6,4) NOT NULL,
    erp_t12m DECIMAL(6,4) NOT NULL,
    expected_return DECIMAL(6,4) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_equityriskpremium_date ON equityriskpremium (date);
"""

# Configuration for NYU Stern ERP scraper
NYU_STERN_CONFIG = {
    'table_name': 'equityriskpremium',
    'url': 'https://pages.stern.nyu.edu/~adamodar/pc/implprem/ERPbymonth.xlsx',
    'sheet_name': 'Historical ERP',
    'create_table_sql': NYU_STERN_TABLE_SQL,
    'type': 'nyu_stern'
}

# Tables to create for NYU Stern data
NYU_TABLES_TO_CREATE = [NYU_STERN_TABLE_SQL]