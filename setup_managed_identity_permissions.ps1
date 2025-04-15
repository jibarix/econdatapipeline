# Setup script for Azure Automation Managed Identity permissions
# This script sets up the necessary permissions for the System-assigned Managed Identity
# of the Azure Automation account to access Azure Storage and Key Vault

# Parameters
param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory=$true)]
    [string]$AutomationAccountName,

    [Parameter(Mandatory=$true)]
    [string]$StorageAccountName,

    [Parameter(Mandatory=$true)]
    [string]$KeyVaultName
)

# Ensure Azure PowerShell modules are installed
$requiredModules = @("Az.Accounts", "Az.Automation", "Az.Storage", "Az.KeyVault", "Az.Resources")
foreach ($module in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Host "Installing module: $module"
        Install-Module -Name $module -Scope CurrentUser -Force -AllowClobber
    }
}

# Connect to Azure (if not already connected)
$context = Get-AzContext
if (-not $context) {
    Connect-AzAccount
}

# Get the Automation Account
Write-Host "Getting Automation Account: $AutomationAccountName"
$automationAccount = Get-AzAutomationAccount -ResourceGroupName $ResourceGroupName -Name $AutomationAccountName
if (-not $automationAccount) {
    Write-Error "Automation Account not found: $AutomationAccountName"
    exit 1
}

# Get the Managed Identity Object ID
$identity = Get-AzADServicePrincipal -DisplayName $AutomationAccountName
if (-not $identity) {
    Write-Error "Managed Identity not found for Automation Account: $AutomationAccountName"
    Write-Host "Make sure you've enabled System-assigned Managed Identity for the Automation Account"
    exit 1
}

$objectId = $identity.Id
Write-Host "Managed Identity Object ID: $objectId"

# Get the Storage Account
Write-Host "Getting Storage Account: $StorageAccountName"
$storageAccount = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName
if (-not $storageAccount) {
    Write-Error "Storage Account not found: $StorageAccountName"
    exit 1
}

# Assign Storage Blob Data Contributor role to the Managed Identity
Write-Host "Assigning Storage Blob Data Contributor role to Managed Identity"
$blobContributorRole = "Storage Blob Data Contributor"
New-AzRoleAssignment -ObjectId $objectId -RoleDefinitionName $blobContributorRole -Scope $storageAccount.Id

# Assign Storage Table Data Contributor role to the Managed Identity
Write-Host "Assigning Storage Table Data Contributor role to Managed Identity"
$tableContributorRole = "Storage Table Data Contributor"
New-AzRoleAssignment -ObjectId $objectId -RoleDefinitionName $tableContributorRole -Scope $storageAccount.Id

# Get the Key Vault
Write-Host "Getting Key Vault: $KeyVaultName"
$keyVault = Get-AzKeyVault -ResourceGroupName $ResourceGroupName -VaultName $KeyVaultName
if (-not $keyVault) {
    Write-Error "Key Vault not found: $KeyVaultName"
    exit 1
}

# Check if RBAC authorization is enabled for Key Vault
if (-not $keyVault.EnableRbacAuthorization) {
    Write-Host "Enabling RBAC authorization for Key Vault: $KeyVaultName"
    Update-AzKeyVault -ResourceGroupName $ResourceGroupName -VaultName $KeyVaultName -EnableRbacAuthorization $true
}

# Assign Key Vault Secrets User role to the Managed Identity
Write-Host "Assigning Key Vault Secrets User role to Managed Identity"
$keyVaultSecretsUserRole = "Key Vault Secrets User"
New-AzRoleAssignment -ObjectId $objectId -RoleDefinitionName $keyVaultSecretsUserRole -Scope $keyVault.ResourceId

Write-Host "Permissions successfully configured for Managed Identity"
Write-Host "Roles assigned:"
Write-Host "- $blobContributorRole on $StorageAccountName"
Write-Host "- $tableContributorRole on $StorageAccountName"
Write-Host "- $keyVaultSecretsUserRole on $KeyVaultName"

# Creating Azure Automation variables
Write-Host "Creating Azure Automation variables"
New-AzAutomationVariable -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name "AZURE_STORAGE_ACCOUNT" -Value $StorageAccountName -Encrypted $false
New-AzAutomationVariable -ResourceGroupName $ResourceGroupName -AutomationAccountName $AutomationAccountName -Name "AZURE_KEY_VAULT_URL" -Value $keyVault.VaultUri -Encrypted $false

Write-Host "Setup complete!"