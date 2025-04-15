"""
Enhanced configuration for FRED API data scrapers.
Includes additional automotive industry indicators and GDP metrics.
"""

# Common settings
FRED_START_DATE = "2014-01-01"  # Start date for all FRED data

# SQL template for FRED data tables
FRED_TABLE_SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    {value_column} {value_type} NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date);
"""

# Configuration for FRED series
FRED_SCRAPER_CONFIGS = {
    'federal_funds_rate': {
        'table_name': 'federal_funds_rate',
        'value_column': 'Rate',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='federal_funds_rate',
            value_column='rate',
            value_type='DECIMAL(12,3)'
        ),
        'type': 'fred',
        'fred_series_id': 'DFF',  # Federal Funds Effective Rate
        'frequency': 'm'  # Monthly average
    },
    
    # New automotive industry indicators
    'auto_manufacturing_orders': {
        'table_name': 'auto_manufacturing_orders',
        'value_column': 'Orders',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='auto_manufacturing_orders',
            value_column='orders',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'fred',
        'fred_series_id': 'AMVPNO',  # Manufacturers' New Orders: Motor Vehicles and Parts
        'frequency': 'm'  # Monthly
    },
    'used_car_retail_sales': {
        'table_name': 'used_car_retail_sales',
        'value_column': 'Sales',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='used_car_retail_sales',
            value_column='sales',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'fred',
        'fred_series_id': 'MRTSSM44112USN',  # Retail Sales: Used Car Dealers
        'frequency': 'm'  # Monthly
    },
    'domestic_auto_inventories': {
        'table_name': 'domestic_auto_inventories',
        'value_column': 'Inventories',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='domestic_auto_inventories',
            value_column='inventories',
            value_type='DECIMAL(12,3)'
        ),
        'type': 'fred',
        'fred_series_id': 'AUINSA',  # Domestic Auto Inventories
        'frequency': 'm'  # Monthly
    },
    'domestic_auto_production': {
        'table_name': 'domestic_auto_production',
        'value_column': 'Production',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='domestic_auto_production',
            value_column='production',
            value_type='DECIMAL(12,1)'
        ),
        'type': 'fred',
        'fred_series_id': 'DAUPSA',  # Domestic Auto Production
        'frequency': 'm'  # Monthly
    },
    'liquidity_credit_facilities': {
        'table_name': 'liquidity_credit_facilities',
        'value_column': 'Facilities',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='liquidity_credit_facilities',
            value_column='facilities',
            value_type='DECIMAL(12,1)'
        ),
        'type': 'fred',
        'fred_series_id': 'WLCFLL',  # Assets: Liquidity and Credit Facilities: Loans: Wednesday Level
        'frequency': 'm'  # Monthly
    },
    'semiconductor_manufacturing_units': {
        'table_name': 'semiconductor_manufacturing_units',
        'value_column': 'Units',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='semiconductor_manufacturing_units',
            value_column='units',
            value_type='DECIMAL(12,4)'
        ),
        'type': 'fred',
        'fred_series_id': 'IPG3344S',  # Industrial Production: Manufacturing: Durable Goods: Semiconductor and Other Electronic Component (NAICS = 3344)
        'frequency': 'm'  # Monthly
    },
    'aluminum_new_orders': {
        'table_name': 'aluminum_new_orders',
        'value_column': 'Orders',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='aluminum_new_orders',
            value_column='orders',
            value_type='DECIMAL(12,1)'
        ),
        'type': 'fred',
        'fred_series_id': 'AANMNO',  # Manufacturers' New Orders: Aluminum and Nonferrous Metal Products
        'frequency': 'm'  # Monthly    
    },
    # New GDP indicators
    'real_gdp': {
        'table_name': 'real_gdp',
        'value_column': 'Value',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='real_gdp',
            value_column='value',
            value_type='DECIMAL(12,2)'
        ),
        'type': 'fred',
        'fred_series_id': 'GDPC1',  # Real Gross Domestic Product
        'frequency': 'q'  # Quarterly
    },
    'gdp_now_forecast': {
        'table_name': 'gdp_now_forecast',
        'value_column': 'Forecast',
        'value_type': 'float',
        'create_table_sql': FRED_TABLE_SQL_TEMPLATE.format(
            table_name='gdp_now_forecast',
            value_column='forecast',
            value_type='DECIMAL(12,4)'
        ),
        'type': 'fred',
        'fred_series_id': 'GDPNOW',  # GDPNow Forecast
        'frequency': 'q'  # Quarterly
    }
}

# Tables to create for FRED data
FRED_TABLES_TO_CREATE = [config['create_table_sql'] for config in FRED_SCRAPER_CONFIGS.values()]