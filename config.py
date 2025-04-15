"""
Configuration for all EDB data scrapers.
"""

# Base URL for all data sources
BASE_URL = "https://www.bde.pr.gov/BDE/PREDDOCS/"

# Common SQL template for creating monthly data tables
MONTHLY_TABLE_SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    {value_column} {value_type} NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date);
"""

# Common SQL template for creating percentage tables
PERCENT_TABLE_SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    {value_column} DECIMAL(6,2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date);
"""

# Definition of all scrapers
SCRAPER_CONFIGS = {
    # Monthly data scrapers
    'auto_sales': {
        'file_name': 'I_AUTO.XLS',
        'sheet_name': 'AS01',
        'data_location': 'A6:K18',
        'table_name': 'auto_sales',
        'value_column': 'Sales',
        'value_type': 'int',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='auto_sales',
            value_column='sales',
            value_type='INTEGER'
        ),
        'type': 'monthly'
    },
    'bankruptcies': {
        'file_name': 'I_BANKRUPT.XLS',
        'sheet_name': 'BAN01',
        'data_location': 'A6:K18',
        'table_name': 'bankruptcies',
        'value_column': 'Filings',
        'value_type': 'int',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='bankruptcies',
            value_column='filings',
            value_type='INTEGER'
        ),
        'type': 'monthly'
    },
    'cement_production': {
        'file_name': 'I_CEMENT.XLS',
        'sheet_name': 'CD01',
        'data_location': 'A6:K18',
        'table_name': 'cement_production',
        'value_column': 'Production',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='cement_production',
            value_column='production',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'electricity_consumption': {
        'file_name': 'I_ENERGY.XLS',
        'sheet_name': 'EEC01',
        'data_location': 'A6:K18',
        'table_name': 'electricity_consumption',
        'value_column': 'Consumption',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='electricity_consumption',
            value_column='consumption',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'gas_price': {
        'file_name': 'I_GAS.XLS',
        'sheet_name': 'GAS01',
        'data_location': 'A6:K18',
        'table_name': 'gas_price',
        'value_column': 'Price',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='gas_price',
            value_column='price',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'gas_consumption': {
        'file_name': 'I_GAS.XLS',
        'sheet_name': 'GAS02',
        'data_location': 'A6:K18',
        'table_name': 'gas_consumption',
        'value_column': 'Consumption',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='gas_consumption',
            value_column='consumption',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'labor_participation': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF03',
        'data_location': 'A6:K18',
        'table_name': 'labor_participation',
        'value_column': 'Rate',
        'value_type': 'float',
        'create_table_sql': PERCENT_TABLE_SQL_TEMPLATE.format(
            table_name='labor_participation',
            value_column='rate'
        ),
        'type': 'monthly'
    },
    'unemployment_rate': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF08',
        'data_location': 'A6:K18',
        'table_name': 'unemployment_rate',
        'value_column': 'Rate',
        'value_type': 'float',
        'create_table_sql': PERCENT_TABLE_SQL_TEMPLATE.format(
            table_name='unemployment_rate',
            value_column='rate'
        ),
        'type': 'monthly'
    },
    'employment_rate': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF09',
        'data_location': 'A6:K18',
        'table_name': 'employment_rate',
        'value_column': 'Rate',
        'value_type': 'float',
        'create_table_sql': PERCENT_TABLE_SQL_TEMPLATE.format(
            table_name='employment_rate',
            value_column='rate'
        ),
        'type': 'monthly'
    },
    'unemployment_claims': {
        'file_name': 'I_LABOR.XLS',
        'sheet_name': 'LF10',
        'data_location': 'A6:K18',
        'table_name': 'unemployment_claims',
        'value_column': 'Claims',
        'value_type': 'int',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='unemployment_claims',
            value_column='claims',
            value_type='INTEGER'
        ),
        'type': 'monthly'
    },
    'trade_employment': {
        'file_name': 'I_PAYROLL.XLS',
        'sheet_name': 'PE05',
        'data_location': 'A6:K18',
        'table_name': 'trade_employment',
        'value_column': 'Employment',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='trade_employment',
            value_column='employment',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'consumer_price_index': {
        'file_name': 'I_PRICE.XLS',
        'sheet_name': 'CPI01',
        'data_location': 'A6:K18',
        'table_name': 'consumer_price_index',
        'value_column': 'Index',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='consumer_price_index',
            value_column='index',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'transportation_price_index': {
        'file_name': 'I_PRICE.XLS',
        'sheet_name': 'CPI05',
        'data_location': 'A6:K18',
        'table_name': 'transportation_price_index',
        'value_column': 'Index',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='transportation_price_index',
            value_column='index',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'retail_sales': {
        'file_name': 'I_RETAIL.XLS',
        'sheet_name': 'RS01',
        'data_location': 'A6:K18',
        'table_name': 'retail_sales',
        'value_column': 'Sales',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='retail_sales',
            value_column='sales',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    },
    'imports': {
        'file_name': 'I_TRADE.XLS',
        'sheet_name': 'ET05',
        'data_location': 'A6:K18',
        'table_name': 'imports',
        'value_column': 'Value',
        'value_type': 'float',
        'create_table_sql': MONTHLY_TABLE_SQL_TEMPLATE.format(
            table_name='imports',
            value_column='value',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'monthly'
    }
}

# Define which tables need to be created in Supabase
TABLES_TO_CREATE = [config['create_table_sql'] for config in SCRAPER_CONFIGS.values()]