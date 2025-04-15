# Deployment Guide: Azure Automation for Economic Data Pipeline

This guide provides step-by-step instructions for deploying the Economic Data Pipeline to Azure Automation using the consolidated runbook approach.

## Prerequisites

- Azure subscription with owner or contributor access
- PowerShell with Az modules installed
- Python 3.8 or later (for local testing)
- Administrative access to your workstation

## Step 1: Prepare Deployment Files

1. Save the consolidated runbook script as `economic_data_pipeline_runbook.py`
2. Save the PowerShell setup script as `setup_managed_identity_permissions.ps1`
3. Ensure you have the FRED API key available

## Step 2: Create Azure Resources

Ensure that you have the following Azure resources deployed:

1. **Create a Resource Group** (if not already created):
   ```
   az group create --name EconDataPipelineRG --location eastus
   ```

2. **Create an Automation Account**:
   ```
   az automation account create --name EconDataAutomation --resource-group EconDataPipelineRG --location eastus --sku Basic
   ```

3. **Create a Storage Account**:
   ```
   az storage account create --name econdatastorage --resource-group EconDataPipelineRG --location eastus --sku Standard_LRS --kind StorageV2
   ```

4. **Create a Key Vault**:
   ```
   az keyvault create --name EconDataKeyVault --resource-group EconDataPipelineRG --location eastus --enable-rbac-authorization true
   ```

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

## Step 6: Install Required Python Packages

Azure Automation requires compatible Python packages. The easiest way to install them is using a helper runbook:

1. **Import the PyPI Import Runbook**:
   - Download the `import_py3package_from_pypi.py` script from the [Azure Automation GitHub repository](https://github.com/azureautomation/runbooks/blob/master/Utility/Python/import_py3package_from_pypi.py)
   - In your Automation Account, go to **Runbooks** and click **+ Create a runbook**
   - Name it `import_py3package_from_pypi`, select **Python** type, and paste the script
   - Click **Save** and **Publish**

2. **Run the Helper Runbook** for each required package:
   - pandas
   - numpy
   - requests
   - azure-identity
   - azure-storage-blob
   - azure-data-tables
   - azure-keyvault-secrets
   - python-dateutil
   - openpyxl

   For each package, execute the helper runbook with parameters:
   ```
   {'module_name': 'package_name'}
   ```

3. **Verify Package Installation**:
   - Go to **Python packages** under **Shared Resources**
   - Check that all required packages are listed with status **Available**

## Step 7: Create the Data Collection Runbook

1. In the Azure Portal, navigate to your Automation Account
2. Select **Runbooks** from the left menu
3. Click **+ Create a runbook**
4. Configure as follows:
   - **Name**: `economic_data_pipeline_runbook`
   - **Runbook type**: Python
   - **Runtime version**: 3.8
5. Click **Create**
6. In the runbook editor, paste the entire content of your `economic_data_pipeline_runbook.py` file
7. Click **Save** and then **Publish**

## Step 8: Test the Runbook

1. In your Automation Account, navigate to your runbook
2. Click **Start** to run the runbook
3. Monitor the output for successful execution
4. Verify that data is being collected and stored in Azure Storage Tables

## Step 9: Schedule the Runbook

1. In your Automation Account, navigate to your runbook
2. Select **Schedules** from the left menu
3. Click **+ Add a schedule**
4. Choose **Link a schedule to your runbook**
5. Click **Create a new schedule**
6. Set:
   - **Name**: `DailyDataCollection`
   - **Description**: "Daily collection of economic data"
   - **Starts**: Choose a suitable start time (e.g., 2:00 AM)
   - **Time zone**: Your preferred time zone
   - **Recurrence**: Recurring
   - **Recur every**: 1 Day
7. Click **Create**
8. Confirm by clicking **OK**

## Step 10: Set Up Monitoring

1. In the Azure Portal, navigate to your Automation Account
2. Select **Monitoring** > **Alerts**
3. Click **+ New alert rule**
4. Add a condition for failed jobs
5. Add an action group for notifications
6. Configure diagnostic settings to log detailed information

## Step 11: Verify Data Collection

After the runbook has executed:

1. In the Azure Portal, navigate to your Storage Account
2. Select **Tables** under **Data storage**
3. Verify that tables have been created for each dataset
4. Check that data has been properly inserted
5. Verify the `datarevisions` table exists and contains revision tracking information

## Troubleshooting

If you encounter issues:

1. **Package Installation Problems**:
   - Check that the packages were installed with the correct version for Python 3.8
   - Try reinstalling problematic packages

2. **Permission Errors**:
   - Verify that the Managed Identity has all required roles assigned
   - Check that the Key Vault has RBAC authorization enabled

3. **Data Source Issues**:
   - Check if the source URLs or APIs are accessible
   - Verify the FRED API key is correct and has necessary permissions

4. **Execution Errors**:
   - Review the job output for detailed error messages
   - Check logs in the Storage Account's `logs` container
   - Test specific scrapers independently for troubleshooting

## Maintenance

- Periodically review run summaries in the `logs` container
- Monitor for failed jobs and address issues promptly
- Update the runbook as needed when data sources change

This deployment guide provides a comprehensive set of instructions for implementing the Economic Data Pipeline using the consolidated runbook approach in Azure Automation.