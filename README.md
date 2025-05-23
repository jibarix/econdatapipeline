# Economic Data Pipeline

A comprehensive data collection and processing pipeline for economic indicators, with a focus on Puerto Rico's economy. The system collects data from multiple sources, tracks revisions, and stores standardized datasets for analysis.

## Overview

This project collects economic indicators from:
- **Economic Development Bank (EDB) of Puerto Rico** - Local economic indicators
- **Federal Reserve Economic Data (FRED)** - US national economic indicators
- **NYU Stern School of Business** - Equity Risk Premium data

The pipeline handles different data formats, scrapes from Excel files and APIs, and standardizes everything into a consistent structure.

## Features

- **Multi-source data collection**: Handles Excel files, APIs, and web data
- **Standardized data schemas**: Consistent formatting across all datasets
- **Data revision tracking**: Records all changes to data points over time
- **Fiscal-to-calendar year conversion**: Properly handles Puerto Rico's fiscal year data (July-June)
- **Automated scheduling**: Daily/weekly data collection via Azure Automation
- **Azure-native deployment**: Uses Azure Storage Tables and Blob Storage for data persistence
- **Secure credential management**: Azure Key Vault integration

## Available Datasets

### Puerto Rico Economic Indicators
- `auto_sales`: Automobile and light truck sales
- `bankruptcies`: Bankruptcy filings
- `cement_production`: Cement production (94lb. bags)
- `electricity_consumption`: Electric energy consumption (mm kWh)
- `gas_price`: Gasoline average retail price (dollars per gallon)
- `gas_consumption`: Gasoline consumption (million gallons)
- `labor_participation`: Labor force participation rate (%)
- `unemployment_rate`: Unemployment rate (%)
- `employment_rate`: Employment rate (%)
- `unemployment_claims`: Unemployment insurance initial file claims
- `trade_employment`: Payroll employment in trade sector (000's)
- `consumer_price_index`: Consumer price index (Dec. 2006=100)
- `transportation_price_index`: Transportation price index (Dec. 2006=100)
- `retail_sales`: Total retail store sales (million $)
- `imports`: External trade imports (million $)

### US Economic Indicators from FRED
- `federal_funds_rate`: Federal funds effective rate (%)
- `auto_manufacturing_orders`: Manufacturers' new orders for motor vehicles and parts (millions $)
- `used_car_retail_sales`: Retail sales from used car dealers (millions $)
- `domestic_auto_inventories`: Domestic auto inventories (thousands of units)
- `domestic_auto_production`: Domestic auto production (thousands of units)
- `liquidity_credit_facilities`: Assets: Liquidity and credit facilities loans
- `semiconductor_manufacturing_units`: Industrial production of semiconductor components (index)
- `aluminum_new_orders`: Manufacturers' new orders for aluminum and nonferrous metal products
- `real_gdp`: Real Gross Domestic Product (billions of chained 2017 dollars)
- `gdp_now_forecast`: GDPNow forecast from Federal Reserve Bank of Atlanta (% change at annual rate)

### Financial Market Data
- `equity_risk_premium`: NYU Stern data on Equity Risk Premium with T-bond rates

## Project Structure

```
├── automation/                      # Azure Automation runbooks
│   └── economic_data_pipeline_runbook.py  # Consolidated runbook for Azure Automation
├── core_local/                      # Core implementation for local development
│   ├── azure_connector.py           # Azure Storage and KeyVault integration
│   ├── azure_data_tracker.py        # Data revision tracking functionality
│   ├── azure_common_scrapers.py     # Base classes for data scrapers
│   ├── azure_fred_scraper.py        # FRED API data collection
│   ├── azure_nyu_scraper.py         # NYU Stern data collection
│   ├── config.py                    # EDB scraper configurations
│   ├── fred_config.py               # FRED API configurations
│   ├── nyu_config.py                # NYU Stern configurations
│   └── main_azure.py                # Main application entry point
├── documentation/                   # Documentation files
│   ├── data_source_documentation.txt  # Overview of data sources
│   ├── economic_indicators.md       # Detailed dataset information
│   ├── deployment_guide.md          # Azure deployment instructions
│   └── automation_checklist.md      # Implementation checklist
├── tests/                           # Test scripts
│   ├── test_azure_connector.py      # Test Azure connection and functionality
│   ├── test_data_tables.py          # Test table storage operations
│   └── test_specific_tables.py      # Test specific data tables
├── .env.example                     # Example environment variables file
├── .gitignore                       # Git ignore file
├── requirements.txt                 # Project dependencies
└── setup_managed_identity_permissions.ps1  # PowerShell script for setting up Azure permissions
```

## Setup and Installation

### Local Development Setup

1. Clone the repository
2. Create a virtual environment and install dependencies:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in the required variables:
   ```
   # Azure Storage Configuration
   AZURE_STORAGE_CONNECTION_STRING=your_connection_string
   
   # Azure Key Vault (for secure credential storage)
   AZURE_KEY_VAULT_URL=https://your-vault.vault.azure.net/
   
   # FRED API Key (required for Federal Reserve data)
   FRED_API_KEY=your_fred_api_key
   ```
4. Run the tests to verify your setup:
   ```
   python tests/test_azure_connector.py
   ```

### Azure Deployment

For deploying to Azure Automation, follow the detailed instructions in `documentation/deployment_guide.md`. The key steps are:

1. Create the required Azure resources (Resource Group, Automation Account, Storage Account, Key Vault)
2. Enable System-assigned Managed Identity for the Automation Account
3. Set up the necessary permissions using the PowerShell script
4. Store the FRED API key in Key Vault
5. Install the required Python packages in Azure Automation
6. Create and publish the data collection runbook
7. Set up a schedule for automated execution

## Running the Data Collection Pipeline

### Local Execution

To run the data collection pipeline locally:

```python
python core_local/main_azure.py
```

This will collect data from all configured sources and store it in Azure Storage.

### Azure Automation Execution

The data collection pipeline is designed to run in Azure Automation using the consolidated runbook. The runbook will:

1. Connect to Azure services using Managed Identity
2. Initialize the necessary tables and containers
3. Collect data from all configured sources
4. Track any data revisions
5. Log the results to blob storage

## Monitoring and Troubleshooting

- Check the job output in Azure Automation for detailed information
- Review run summaries in the `logs` container in blob storage
- Monitor data revisions in the `datarevisions` table
- Set up Azure Monitor alerts for failed jobs

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.