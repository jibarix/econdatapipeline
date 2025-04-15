# Azure Automation Implementation Checklist

## Phase 1: Environment Setup

- [x] Create Resource Group: `EconDataPipelineRG`
- [x] Create Azure Automation Account: `EconDataAutomation`
- [x] Create Azure Key Vault: `EconDataKeyVault`
- [x] Create Azure Storage Account: `econdatastorage`
- [x] Create `.gitignore` file for repository
- [x] Create `.env` file with required environment variables
- [x] Update dependencies in `requirements.txt`
- [x] Add FRED API Key to environment variables

## Phase 2: Code Development

- [x] Create Azure connector functionality
- [x] Implement data tracking with revision history
- [x] Build scrapers for EDB data
- [x] Build scrapers for FRED API data
- [x] Build scraper for NYU Stern data
- [x] Create test scripts for local validation
- [x] Create consolidated runbook (`economic_data_pipeline_runbook.py`)

## Phase 3: Local Testing

- [x] Test Azure connector functionality
- [x] Verify container operations
- [x] Verify table operations
- [x] Verify data tracker operations
- [x] Confirm scraper prerequisites work correctly

## Phase 4: Azure Automation Setup

- [x] Create consolidated runbook in Azure Automation
- [ ] Install required Python packages in Azure Automation:
  - [ ] Import `import_py3package_from_pypi.py` helper runbook
  - [ ] Install pandas
  - [ ] Install numpy
  - [ ] Install requests
  - [ ] Install azure-identity
  - [ ] Install azure-storage-blob
  - [ ] Install azure-data-tables
  - [ ] Install azure-keyvault-secrets
  - [ ] Install python-dateutil
  - [ ] Install openpyxl
- [ ] Test the runbook execution in Azure Automation environment

### Managed Identity Permissions

- [ ] Enable System-assigned Managed Identity for the Automation Account
- [ ] Run the PowerShell script for permission setup
- [ ] Assign Storage Blob Data Contributor role
- [ ] Assign Storage Table Data Contributor role
- [ ] Assign Key Vault Secrets User role
- [ ] Store FRED API key in Key Vault as 'FRED-API-KEY'
- [ ] Verify all permissions are properly set

### Scheduling

- [ ] Create daily schedule for main data collection runbook
- [ ] Create weekly schedule for less frequently updated data sources
- [ ] Test scheduled execution

## Phase 5: Monitoring and Operations

### Logging and Monitoring

- [ ] Set up Azure Monitor alerts for failed runbooks
- [ ] Configure diagnostic settings for Azure Automation account
- [ ] Create Power BI dashboard for pipeline health monitoring (optional)

### Data Access Solution

- [ ] Implement data export functionality
- [ ] Create Azure Function for API access (optional)
- [ ] Configure CORS and authentication (if implementing API)

### Documentation

- [ ] Update technical documentation with Azure implementation details
- [ ] Create operational runbook for managing the pipeline
- [ ] Document troubleshooting procedures

## Final Verification

- [ ] Run full end-to-end test of the pipeline
- [ ] Confirm all data sources are collected correctly
- [ ] Verify revision tracking functionality
- [ ] Ensure proper error handling and notifications
- [ ] Document the final solution architecture and components