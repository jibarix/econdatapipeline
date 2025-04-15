# Economic Data Pipeline

A comprehensive data pipeline for collecting, processing, and storing economic indicators from multiple sources, with a focus on Puerto Rico's economy.

## Project Overview

This project builds a robust data pipeline that automatically collects economic data from multiple authoritative sources, processes it into a standardized format, tracks data revisions, and stores it for analysis. The system is deployed on Azure using Azure Automation, Azure Storage Tables, and Azure Key Vault.

### Data Sources

1. **Economic Development Bank (EDB) of Puerto Rico**
   - Local economic indicators for Puerto Rico
   - Data organized in fiscal year format (July-June)
   - Monthly updates via Excel files

2. **Federal Reserve Economic Data (FRED)**
   - US national economic indicators via API
   - Various update frequencies
   - Includes GDP, automotive industry, and financial indicators

3. **NYU Stern School of Business**
   - Equity Risk Premium data
   - Monthly updates via Excel files

## Architecture

The pipeline leverages Azure cloud services for secure, scalable, and reliable operation:

- **Azure Automation**: Runs scheduled data collection scripts
- **Azure Storage Tables**: Stores standardized economic data
- **Azure Blob Storage**: Caches raw data files
- **Azure Key Vault**: Secures API keys and credentials
- **Managed Identity**: Provides secure authentication between services

## Available Datasets

The pipeline collects 30+ economic indicators including:

### Puerto Rico Economic Indicators
- Auto sales
- Bankruptcies
- Cement production
- Electricity consumption
- Gas price and consumption
- Labor participation rate
- Unemployment rate
- Employment rate
- Consumer price index
- Retail sales
- Imports
- And more...

### US Economic Indicators
- Federal funds rate
- Auto manufacturing orders
- Used car retail sales
- Domestic auto inventories and production
- Semiconductor manufacturing units
- Real GDP
- GDPNow forecast
- And more...

### Financial Market Data
- Treasury bond rates
- Equity risk premium
- Expected returns

## Key Features

- **Automated data collection** from multiple sources
- **Standardized data schema** for consistent analysis
- **Data revision tracking** to monitor source data changes
- **Smart updates** to minimize redundant operations
- **Secure credential management** using Azure Key Vault
- **Error handling and logging** for reliable operation

## Project Structure

```
economic-data-pipeline/
├── core_local/                 # Core functionality for local development
│   ├── azure_connector.py      # Azure Storage and Key Vault integration
│   ├── azure_data_tracker.py   # Data revision tracking
│   ├── azure_common_scrapers.py # Base scraper classes
│   ├── azure_fred_scraper.py   # FRED API scraper
│   ├── azure_nyu_scraper.py    # NYU Stern data scraper
│   ├── config.py               # EDB scraper configurations
│   ├── fred_config.py          # FRED API configurations
│   ├── nyu_config.py           # NYU Stern configurations
│   └── main_azure.py           # Main script for local execution
├── automation_local/           # Azure Automation scripts for local testing
│   ├── setup_modules_runbook.py # Validates module imports
│   └── data_collection_runbook.py # Data collection wrapper
├── automation/                 # Production Azure Automation scripts
│   └── economic_data_pipeline_runbook.py # Consolidated production runbook
├── tests/                      # Test scripts
│   ├── test_azure_connector.py # Tests Azure connector functionality
│   ├── test_data_tables.py     # Tests Azure Table integration
│   └── test_specific_tables.py # Tests specific table operations
├── documentation/              # Project documentation
│   ├── data_source_documentation.txt # Data source details
│   ├── economic_indicators.md  # Dataset documentation
│   ├── deployment_guide.md     # Azure deployment instructions
│   └── automation_checklist.md # Implementation checklist
├── requirements.txt            # Python dependencies
├── .env.example                # Example environment variables
├── setup_managed_identity_permissions.ps1 # PowerShell script for Azure setup
└── README.md                   # This file
```

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Azure subscription
- FRED API key
- Required Python packages (see `requirements.txt`)

### Local Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/economic-data-pipeline.git
   cd economic-data-pipeline
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file based on `.env.example`:
   ```
   # Azure Storage Configuration
   AZURE_STORAGE_CONNECTION_STRING=your_connection_string

   # Azure Key Vault
   AZURE_KEY_VAULT_URL=https://your-vault.vault.azure.net/

   # FRED API Key
   FRED_API_KEY=your_fred_api_key
   ```

5. Run the test script to validate Azure connection:
   ```bash
   python tests/test_azure_connector.py
   ```

6. Run the main script locally:
   ```bash
   python core_local/main_azure.py
   ```

### Azure Deployment

Please follow the detailed instructions in [documentation/deployment_guide.md](documentation/deployment_guide.md) for deploying the pipeline to Azure Automation. The guide includes:

1. Creating required Azure resources
2. Setting up Managed Identity permissions
3. Storing secrets in Key Vault
4. Uploading and configuring the runbook
5. Setting up schedules
6. Monitoring and troubleshooting

## Data Schema

Each dataset is stored in its own Azure Storage Table with a consistent schema:

- `PartitionKey`: Dataset name (e.g., "autosales", "federalfundsrate")
- `RowKey`: Date of the data point in YYYY-MM-DD format
- Value columns: Specific to each dataset (e.g., "sales", "rate", "index")
- `Timestamp`: System-generated timestamp for the entity

Data revisions are tracked in a separate "datarevisions" table that records all historical changes to data points.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Economic Development Bank of Puerto Rico for providing open economic data
- Federal Reserve Bank of St. Louis for the FRED API
- NYU Stern School of Business for Equity Risk Premium data
- Azure team for their comprehensive cloud platform