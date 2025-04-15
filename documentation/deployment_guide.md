# Deployment Guide: Azure Automation for Economic Data Pipeline

This guide provides step-by-step instructions for deploying the Economic Data Pipeline to Azure Automation.

## Prerequisites

- Azure subscription with owner or contributor access
- PowerShell with Az modules installed
- Python 3.8 or later (for local testing)
- Administrative access to your workstation

## Step 1: Prepare Local Files

1. Save the consolidated Python module as `economic_data_pipeline.py`
2. Save the runbook script as `data_collection_runbook.py`
3. Save the PowerShell setup script as `setup_managed_identity_permissions.ps1`

## Step 2: Verify Azure Resources

Ensure that you have the following Azure resources already deployed:

- Resource Group: `EconDataPipelineRG`
- Automation Account: `EconDataAutomation`
- Storage Account: `econdatastorage`
- Key Vault: `EconDataKeyVault`

If not already created, you can create them using the Azure Portal or Azure CLI.

## Step 3: Enable System-assigned Managed Identity

1. In the Azure Portal, navigate to your Automation Account (`EconDataAutomation`)
2. Select **Identity** from the left menu
3. Under the **System assigned** tab, set **Status** to **On**
4. Click **Save**
5. Make note of the Object ID, as you'll need it later

## Step 4: Set Up Required Permissions

Run the PowerShell setup script to configure the necessary permissions:

```powershell
.\setup_managed_identity_permissions.ps1 -ResourceGroupName "EconDataPipelineRG" -AutomationAccountName "EconDataAutomation" -StorageAccountName "econdatastorage" -KeyVaultName "EconDataKeyVault"
```

This script will:
- Assign the "Storage Blob Data Contributor" role to the Managed Identity
- Assign the "Storage Table Data Contributor" role to the Managed Identity
- Assign the "Key Vault Secrets User" role to the Managed Identity
- Create Automation variables for storage account and key vault URL

## Step 5: Store FRED API Key in Key Vault

1. In the Azure Portal, navigate to your Key Vault (`EconDataKeyVault`)
2. Select **Secrets** from the left menu
3. Click **+ Generate/Import**
4. Set **Name** to `FRED-API-KEY`
5. Set **Value** to your FRED API key
6. Click **Create**

## Step 6: Upload Python Module to Azure Automation

1. In the Azure Portal, navigate to your Automation Account (`EconDataAutomation`)
2. Select **Modules** from the left menu
3. Click **+ Add a module**
4. Choose **Python module**
5. Browse to select your `economic_data_pipeline.py` file
6. Click **Import**

## Step 7: Create the Runbook

1. In the Azure Portal, navigate to your Automation Account (`EconDataAutomation`)
2. Select **Runbooks** from the left menu
3. Click **+ Create a runbook**
4. Set **Name** to `data_collection_runbook`
5. Set **Runbook type** to **Python**
6. Click **Create**
7. In the runbook editor, paste the contents of `data_collection_runbook.py`
8. Click **Save** and then **Publish**

## Step 8: Import Required Python Packages

1. In the Azure Portal, navigate to your Automation Account (`EconDataAutomation`)
2. Select **Python packages** from the left menu
3. Click **+ Add a package**
4. Import the following packages (one at a time):
   - `pandas`
   - `requests`
   - `azure-identity`
   - `azure-storage-blob`
   - `azure-data-tables`
   - `azure-keyvault-secrets`
   - `python-dateutil`

## Step 9: Test the Runbook

1. In the Azure Portal, navigate to your Automation Account (`EconDataAutomation`)
2. Select **Runbooks** from the left menu
3. Click on your `data_collection_runbook`
4. Click **Start** to run the runbook
5. Monitor the output to ensure it completes successfully

## Step 10: Schedule the Runbook

1. In the Azure Portal, navigate to your Automation Account (`EconDataAutomation`)
2. Select **Runbooks** from the left menu
3. Click on your `data_collection_runbook`
4. Select **Schedules** from the left menu
5. Click **+ Add a schedule**
6. Choose **Link a schedule to your runbook**
7. Click **Create a new schedule**
8. Set **Name** to `DailyDataCollection`
9. Set **Starts** to an appropriate start time
10. Set **Recurrence** to **Recurring**
11. Set **Recur every** to **1 Day**
12. Click **Create**

## Step 11: Monitor Execution

1. In the Azure Portal, navigate to your Automation Account (`EconDataAutomation`)
2. Select **Jobs** from the left menu
3. Review the status and output of completed jobs
4. To view logs, you can check the `logs` container in your storage account

## Troubleshooting

If you encounter issues:

1. **Module Import Errors**:
   - Ensure all required Python packages are imported in your Automation Account
   - Check that the `economic_data_pipeline.py` module is properly uploaded

2. **Permission Errors**:
   - Verify Managed Identity permissions using the Azure Portal
   - Check that the Key Vault has RBAC authorization enabled

3. **FRED API Key Issues**:
   - Ensure the key is correctly stored in Key Vault as `FRED-API-KEY`
   - Verify the Managed Identity has permission to read secrets

4. **Storage Account Issues**:
   - Ensure the storage account exists and is accessible
   - Verify the Managed Identity has both blob and table permissions

## Maintenance

- Periodically check the storage account for accumulated data
- Review job logs for any recurring errors
- Keep required Python packages updated in the Automation Account

---

By following these steps, you've successfully deployed the Economic Data Pipeline to Azure Automation. The system will now automatically collect and process economic indicators according to your schedule