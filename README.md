# Economic Data Pipeline - Azure Edition

A comprehensive economic data collection and processing pipeline built on Azure infrastructure.

## Project Overview

This project automates the collection, processing, and storage of economic indicators from multiple sources, leveraging Azure cloud services for reliability and scalability. The pipeline migrates from a previously Supabase-based implementation to a fully Azure-managed solution.

### Data Sources

The pipeline collects data from:

- **Economic Development Bank (EDB) of Puerto Rico** - Local economic indicators
- **Federal Reserve Economic Data (FRED)** - US national economic indicators
- **NYU Stern School of Business** - Equity Risk Premium data

### Azure Services Used

- **Azure Automation** - For scheduled execution of data collection pipelines
- **Azure Storage (Table & Blob)** - For data persistence and revision tracking
- **Azure Key Vault** - For secure storage of API keys and credentials
- **Azure Managed Identity** - For secure service-to-service authentication

## Repository Structure

```
economic_data_pipeline/
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

### File Descriptions

#### Core Components

- **`__init__.py`** - Package initialization file that exposes core functionality.
- **`azure_connector.py`** - Handles all interactions with Azure Storage (Blob and Table) and Key Vault. Serves as the central access point for Azure resources.
- **`azure_data_tracker.py`** - Tracks data revisions and changes over time. Implements smart update logic to detect and log modifications to economic data points.
- **`azure_common_scrapers.py`** - Base classes for scrapers, including common functionality for monthly and quarterly data extraction from Economic Development Bank sources.
- **`azure_fred_scraper.py`** - Specialized scraper for the Federal Reserve Economic Data (FRED) API. Handles retrieval and processing of US economic indicators.
- **`azure_nyu_scraper.py`** - Specialized scraper for NYU Stern School of Business data. Extracts equity risk premium information.
- **`main_azure.py`** - Main orchestration script that coordinates the data collection process across all sources.
- **`config.py`** - Configuration for Economic Development Bank data sources, including table structures and data locations.
- **`fred_config.py`** - Configuration for FRED API data sources, including series IDs and frequency settings.
- **`nyu_config.py`** - Configuration for NYU Stern data sources.

#### Automation Runbooks

- **`data_collection_runbook.py`** - Azure Automation runbook that executes the data collection pipeline using managed identity. Designed to run on a schedule.
- **`setup_modules_runbook.py`** - Diagnostic runbook that verifies the Azure Automation environment, including module imports and environment variables.

#### Tests

- **`test_azure_connector.py`** - Tests for the Azure connector functionality, including storage operations.
- **`test_data_tables.py`** - Tests for Azure Table Storage interactions.
- **`test_specific_tables.py`** - Tests for specific economic datasets.

#### Root Files

- **`.gitignore`** - Specifies intentionally untracked files to ignore.
- **`README.md`** - Project documentation and setup instructions (this file).
- **`setup.py`** - Python package setup configuration for installation.
- **`requirements.txt`** - Lists all Python dependencies required by the project.
- **`.env.example`** - Template for environment variables needed by the application.

## Data Flow Architecture

1. **Data Collection**
   - Azure Automation initiates the data collection runbook
   - Runbook uses Managed Identity to securely access Azure resources
   - Scrapers download data from respective sources

2. **Data Processing**
   - Raw data is parsed and transformed into a standardized format
   - Data is validated and cleaned
   - Historical data is preserved for time series analysis

3. **Data Storage**
   - Processed data is stored in Azure Table Storage
   - Raw files are cached in Azure Blob Storage
   - Data revisions are tracked with timestamps

4. **Data Access**
   - Consumers can access data through Azure Table Storage
   - Historical revisions can be retrieved for audit purposes
   - Data can be exported to CSV or other formats for analysis

## Setup Instructions

### Prerequisites

- Azure subscription
- Azure CLI installed (for deployment scripts)
- Python 3.8 or higher
- FRED API key (for Federal Reserve data)

### Local Development Setup

1. Clone the repository
   ```bash
   git clone https://github.com/your-username/economic-data-pipeline.git
   cd economic-data-pipeline
   ```

2. Create and activate a virtual environment
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

4. Copy the environment template and configure
   ```bash
   cp .env.example .env
   # Edit .env with your configuration values
   ```

5. Run tests to verify setup
   ```bash
   python -m tests.test_azure_connector
   ```

### Azure Deployment

1. Create required Azure resources
   ```bash
   # Ensure you're logged in to Azure CLI
   az login
   
   # Create resource group, storage account, key vault, and automation account
   az group create --name EconDataPipelineRG --location eastus
   az storage account create --name econdatastorage --resource-group EconDataPipelineRG --location eastus --sku Standard_LRS
   az keyvault create --name EconDataKeyVault --resource-group EconDataPipelineRG --location eastus
   az automation account create --name EconDataAutomation --resource-group EconDataPipelineRG --location eastus --sku Basic
   ```

2. Package the core modules
   ```bash
   # From project root
   cd core
   zip -r ../economic_data_pipeline.zip .
   cd ..
   ```

3. Upload module package to Azure Automation
   - Navigate to your Automation Account in Azure Portal
   - Go to "Python packages" under "Shared Resources"
   - Upload the zip package created in the previous step

4. Upload runbooks
   - Go to "Runbooks" under "Process Automation"
   - Upload both `setup_modules_runbook.py` and `data_collection_runbook.py`
   - Publish the runbooks after uploading

5. Configure managed identity and permissions
   - Enable system-assigned managed identity for the Automation Account
   - Grant appropriate permissions to storage account and key vault:
     - Storage Blob Data Contributor
     - Storage Table Data Contributor
     - Key Vault Secrets User

6. Set environment variables in Azure Automation
   - AZURE_STORAGE_ACCOUNT: econdatastorage
   - AZURE_KEY_VAULT_URL: https://econdatakeyvault.vault.azure.net/

7. Store secrets in Key Vault
   - FRED-API-KEY: Your Federal Reserve API key

8. Test the setup
   - Run the `setup_modules_runbook` to verify all modules are properly loaded
   - Run the `data_collection_runbook` to start collecting data

9. Configure scheduling
   - Create daily schedule for data collection
   - Link schedule to the `data_collection_runbook`

## Available Datasets

### Puerto Rico Economic Indicators
- **Auto Sales (`autosales`)** - Automobile and light truck sales
- **Bankruptcies (`bankruptcies`)** - Bankruptcy filings
- **Cement Production (`cementproduction`)** - Cement production in 94lb. bags
- **Electricity Consumption (`electricityconsumption`)** - Electric energy consumption in mm kWh
- **Gas Price (`gasprice`)** - Gasoline average retail price in dollars per gallon
- **Gas Consumption (`gasconsumption`)** - Gasoline consumption in millions of gallons
- **Labor Participation (`laborparticipation`)** - Labor force participation rate as percentage
- **Unemployment Rate (`unemploymentrate`)** - Unemployment rate as percentage
- **Employment Rate (`employmentrate`)** - Employment rate as percentage
- **Unemployment Claims (`unemploymentclaims`)** - Unemployment insurance initial file claims
- **Trade Employment (`tradeemployment`)** - Payroll employment in trade sector (thousands)
- **Consumer Price Index (`consumerpriceindex`)** - Consumer price index (Dec. 2006=100)
- **Transportation Price Index (`transportationpriceindex`)** - Transportation price index (Dec. 2006=100)
- **Retail Sales (`retailsales`)** - Total retail store sales in millions of dollars
- **Imports (`imports`)** - External trade imports in millions of dollars

### US Economic Indicators (FRED)
- **Federal Funds Rate (`federalfundsrate`)** - Federal funds effective rate as percentage
- **Auto Manufacturing Orders (`automanufacturingorders`)** - Manufacturers' new orders for motor vehicles and parts
- **Used Car Retail Sales (`usedcarretailsales`)** - Retail sales from used car dealers
- **Domestic Auto Inventories (`domesticautoinventories`)** - Domestic auto inventories in thousands of units
- **Domestic Auto Production (`domesticautoproduction`)** - Domestic auto production in thousands of units
- **Liquidity Credit Facilities (`liquiditycreditfacilities`)** - Assets in liquidity and credit facilities loans
- **Semiconductor Manufacturing (`semiconductormanufacturingunits`)** - Industrial production of semiconductor components
- **Aluminum New Orders (`aluminumneworders`)** - Manufacturers' new orders for aluminum and nonferrous metal products
- **Real GDP (`realgdp`)** - Real Gross Domestic Product in billions of chained 2017 dollars
- **GDPNow Forecast (`gdpnowforecast`)** - GDPNow forecast from Federal Reserve Bank of Atlanta

### Financial Market Data (NYU Stern)
- **Equity Risk Premium (`equityriskpremium`)** - Includes T-bond rates, sustainable ERP, and other equity risk premium metrics

## Data Organization

### Database Schema

All data is stored in Azure Table Storage with consistent schema across datasets:

- **PartitionKey** - Used to identify the dataset
- **RowKey** - Usually the date in YYYY-MM-DD format
- **Value columns** - Named after the economic indicator (e.g., rate, price, sales)
- **Timestamp** - Azure Table automatically tracks the last update time

### Data Revisions

Data revisions are tracked in a dedicated `datarevisions` table with the following structure:

- **PartitionKey** - Dataset name
- **RowKey** - Unique identifier for the revision
- **dataset** - Name of the dataset
- **data_date** - Date of the data point that was revised
- **value_field** - Name of the field that was revised
- **old_value** - Previous value
- **new_value** - New value
- **revision_date** - When the revision was detected

### Raw Data Storage

Raw files downloaded from sources are cached in Azure Blob Storage:

- **Container**: raw-files
- **Blob naming convention**: Original filename (e.g., `I_AUTO.XLS`) or source identifier (e.g., `fred_DFF.json`)

## Development Guidelines

### Adding a New Data Source

1. Create a new scraper class inheriting from the appropriate base class
2. Add configuration to the relevant config file
3. Update the main processing loop in `main_azure.py`
4. Add appropriate tests

### Modifying Existing Data Sources

1. Update the configuration in the appropriate config file
2. Test the changes locally using the test scripts
3. Monitor the first few runs in Azure Automation to ensure data quality

### Error Handling

The pipeline includes robust error handling:

- Individual scraper failures don't affect other data sources
- All errors are logged for investigation
- The data revision tracking system ensures data integrity even when sources change

## Monitoring and Maintenance

### Monitoring

- Review Azure Automation job logs regularly
- Check the run summary JSON files in the `logs` container
- Monitor data revision frequency for unusual patterns

### Troubleshooting

- If a runbook fails, run the `setup_modules_runbook` to verify module imports
- Check for API rate limiting issues with FRED
- Verify source website structure hasn't changed for scraped sources

## License

[Your License]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.