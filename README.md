# Economic Data Collection System

A comprehensive data collection system for economic indicators from multiple sources, with revision tracking and smart updates.

## Overview

This project automatically collects, processes, and stores economic data from:

1. **Economic Development Bank (EDB) of Puerto Rico** - Local economic indicators
2. **Federal Reserve Economic Data (FRED)** - US national economic indicators
3. **NYU Stern School of Business** - Equity Risk Premium data

The system handles data in different formats, manages revisions intelligently, and maintains consistent database schemas.

**Notice**: This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.

## Features

- **Multi-source data collection**: Handles Excel files and API data
- **Smart revision tracking**: Detects and logs changes in data values over time
- **Consistent data processing**: Transforms diverse formats into standardized time series
- **Robust error handling**: Gracefully handles connection issues and format changes
- **Configurable scrapers**: Easy to add new data sources

## Available Datasets

### Puerto Rico Economic Indicators
- Auto sales
- Bankruptcies
- Cement production
- Electricity consumption
- Gas prices and consumption
- Labor market indicators (unemployment, participation)
- Consumer price indices
- Retail sales
- Import/export data
- Commercial banking metrics

### US Economic Indicators (FRED)
- Federal funds rate
- Auto manufacturing orders
- Used car retail sales
- Auto inventories and production
- Auto loan rates

### Financial Market Data
- NYU Stern Equity Risk Premium data

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd economic-data-system

# Set up virtual environment
py -3.11 -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\Activate.ps1

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials
```

## Configuration

Create a `.env` file with the following:

```
AZURE_STORAGE_CONNECTION_STRING=your_supabase_azure_storage_connection_string
AZURE_KEY_VAULT_URL=your_azure_key_vault
FRED_API_KEY=your_fred_api_key
```

## Usage

### Run Data Collection

```bash
python main_azure.py
```

## Database Schema

The system uses Supabase (PostgreSQL) with the following schema:

- **Data Tables**: One table per dataset with standardized schema
- **scraper_metadata**: Tracks when each dataset was last updated
- **data_revisions**: Records all data changes with historical values

## Project Structure

```
economicdatapipeline/
├── core/
│   ├── __init__.py
│   ├── azure_connector.py
│   ├── azure_data_tracker.py
│   ├── azure_common_scrapers.py
│   ├── azure_fred_scraper.py
│   ├── azure_nyu_scraper.py
│   ├── main_azure.py
│   ├── config.py
│   ├── fred_config.py
│   └── nyu_config.py
├── automation/
│   ├── data_collection_runbook.py
│   └── setup_modules_runbook.py
├── tests/
│   ├── test_azure_connector.py
│   ├── test_data_tables.py
│   └── test_specific_tables.py
├── .gitignore
├── README.md
├── setup.py
├── requirements.txt
└── .env.example
```

## Custom Scraper Development

To add a new data source:

1. Create a specific scraper class extending `BaseEDBScraper`
2. Add configuration details to the relevant config file
3. Include the scraper in `main.py`

See existing scrapers for implementation examples.

## License

[Add your license information here]

## Contributors

[Add contributors information here]

## Acknowledgements

- The Equity Risk Premium data is sourced from Professor Aswath Damodaran's website at NYU Stern School of Business.
- Economic Development Bank for Puerto Rico (EDB) provides the Puerto Rico economic indicators data.
- Federal Reserve Economic Data (FRED) API provides the US national economic indicators.