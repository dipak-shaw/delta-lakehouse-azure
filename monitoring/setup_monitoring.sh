#!/bin/bash
# Setup monitoring for Delta Lakehouse on Azure
# This script sets up monitoring resources and alerts for the Delta Lakehouse architecture

# Parse arguments
subscription_id=""
resource_group=""
environment="dev"

print_usage() {
    echo "Usage: ./setup_monitoring.sh --subscription <subscription_id> --resource-group <resource_group> [--environment <env>]"
}

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --subscription)
            subscription_id="$2"
            shift
            shift
            ;;
        --resource-group)
            resource_group="$2"
            shift
            shift
            ;;
        --environment)
            environment="$2"
            shift
            shift
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$subscription_id" ] || [ -z "$resource_group" ]; then
    echo "Error: Missing required parameters."
    print_usage
    exit 1
fi

echo "Setting up monitoring for Delta Lakehouse in subscription: $subscription_id, resource group: $resource_group, environment: $environment"

# Set Azure subscription
echo "Setting Azure subscription..."
az account set --subscription "$subscription_id"

if [ $? -ne 0 ]; then
    echo "Error: Failed to set Azure subscription. Please check the subscription ID and your login status."
    exit 1
fi

# Create Log Analytics workspace if it doesn't exist
workspace_name="law-deltalake-$environment"
echo "Checking for Log Analytics workspace: $workspace_name..."

workspace_exists=$(az monitor log-analytics workspace list --resource-group "$resource_group" --query "[?name=='$workspace_name'].name" -o tsv)

if [ -z "$workspace_exists" ]; then
    echo "Creating Log Analytics workspace: $workspace_name..."
    az monitor log-analytics workspace create \
        --resource-group "$resource_group" \
        --workspace-name "$workspace_name" \
        --location "$(az group show --name "$resource_group" --query "location" -o tsv)" \
        --sku "PerGB2018"
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create Log Analytics workspace."
        exit 1
    fi
else
    echo "Log Analytics workspace already exists."
fi

# Get workspace ID and primary key
workspace_id=$(az monitor log-analytics workspace show --resource-group "$resource_group" --workspace-name "$workspace_name" --query "customerId" -o tsv)
workspace_key=$(az monitor log-analytics workspace get-shared-keys --resource-group "$resource_group" --workspace-name "$workspace_name" --query "primarySharedKey" -o tsv)

echo "Log Analytics workspace ID: $workspace_id"

# Create Action Group for alerts
action_group_name="ag-deltalake-$environment"
echo "Creating Action Group: $action_group_name..."

az monitor action-group create \
    --resource-group "$resource_group" \
    --name "$action_group_name" \
    --short-name "DLakeAlert" \
    --action email -n "DataEngineeringTeam" --email-address "dataeng@example.com"

action_group_id=$(az monitor action-group show --resource-group "$resource_group" --name "$action_group_name" --query "id" -o tsv)

echo "Action Group ID: $action_group_id"

# Find Databricks workspace
databricks_workspace=$(az resource list --resource-group "$resource_group" --resource-type "Microsoft.Databricks/workspaces" --query "[0].name" -o tsv)

if [ -z "$databricks_workspace" ]; then
    echo "Warning: No Databricks workspace found in resource group $resource_group"
else
    echo "Found Databricks workspace: $databricks_workspace"
    
    # Set up Databricks diagnostic settings to send logs to Log Analytics
    databricks_id=$(az resource show --resource-group "$resource_group" --name "$databricks_workspace" --resource-type "Microsoft.Databricks/workspaces" --query "id" -o tsv)
    
    echo "Setting up diagnostic settings for Databricks..."
    az monitor diagnostic-settings create \
        --name "databricks-to-logs" \
        --resource "$databricks_id" \
        --workspace "$workspace_id" \
        --logs '[{"category": "dbfs", "enabled": true}, {"category": "clusters", "enabled": true}, {"category": "accounts", "enabled": true}, {"category": "jobs", "enabled": true}, {"category": "notebook", "enabled": true}, {"category": "sqlPermissions", "enabled": true}, {"category": "instancePools", "enabled": true}, {"category": "sqlAnalytics", "enabled": true}]' \
        --metrics '[{"category": "AllMetrics", "enabled": true}]'
fi

# Find Data Lake Storage account
storage_account=$(az resource list --resource-group "$resource_group" --resource-type "Microsoft.Storage/storageAccounts" --query "[0].name" -o tsv)

if [ -z "$storage_account" ]; then
    echo "Warning: No Storage account found in resource group $resource_group"
else
    echo "Found Storage account: $storage_account"
    
    # Set up Storage diagnostic settings
    storage_id=$(az resource show --resource-group "$resource_group" --name "$storage_account" --resource-type "Microsoft.Storage/storageAccounts" --query "id" -o tsv)
    
    echo "Setting up diagnostic settings for Storage..."
    az monitor diagnostic-settings create \
        --name "storage-to-logs" \
        --resource "$storage_id" \
        --workspace "$workspace_id" \
        --logs '[{"category": "StorageRead", "enabled": true}, {"category": "StorageWrite", "enabled": true}, {"category": "StorageDelete", "enabled": true}]' \
        --metrics '[{"category": "Transaction", "enabled": true}, {"category": "Capacity", "enabled": true}]'
fi

# Create alert rules
echo "Creating alert rules..."

# Alert for failed Databricks jobs
az monitor scheduled-query create \
    --resource-group "$resource_group" \
    --name "DatabricksJobFailure" \
    --scopes "/subscriptions/$subscription_id/resourceGroups/$resource_group" \
    --condition "count where AzureDiagnostics | where Category == 'jobs' and StatusCode >= 400 > 0" \
    --description "Alert when any Databricks job fails" \
    --location "$(az group show --name "$resource_group" --query "location" -o tsv)" \
    --action-group "$action_group_id" \
    --evaluation-frequency "15m" \
    --window-size "15m" \
    --severity 2

# Alert for high storage costs
az monitor scheduled-query create \
    --resource-group "$resource_group" \
    --name "StorageCostAlert" \
    --scopes "/subscriptions/$subscription_id/resourceGroups/$resource_group" \
    --condition "sum(UsageQuantity) by ResourceId > 5000" \
    --description "Alert when storage costs exceed threshold" \
    --location "$(az group show --name "$resource_group" --query "location" -o tsv)" \
    --action-group "$action_group_id" \
    --evaluation-frequency "1d" \
    --window-size "1d" \
    --severity 2

echo "Setting up custom queries for monitoring dashboard..."

# Create example queries for monitoring
queries=(
    "DatabricksClusterCost:AzureMetrics | where ResourceProvider == 'MICROSOFT.DATABRICKS' | summarize TotalCost = sum(Total) by bin(TimeGenerated, 1d), ClusterName | render timechart"
    "StorageUtilization:AzureMetrics | where ResourceProvider == 'MICROSOFT.STORAGE' | where MetricName == 'UsedCapacity' | summarize AvgUsedGB = avg(Total)/1024/1024/1024 by bin(TimeGenerated, 1h) | render timechart"
    "JobPerformance:AzureDiagnostics | where Category == 'jobs' | summarize AvgDuration = avg(DurationMs)/1000, SuccessRate = 100.0 * countif(StatusCode < 400) / count() by JobId | sort by AvgDuration desc"
)

for query in "${queries[@]}"; do
    IFS=":" read -r name query_text <<< "$query"
    echo "Creating query: $name"
    # In a real script, we would save these queries to Log Analytics
    echo "$query_text" > "./monitoring/$name.kql"
done

echo "Monitoring setup complete!"

echo "Next steps:"
echo "1. Set up your Databricks monitoring notebook using the provided workspace ID and key"
echo "2. Configure the monitoring dashboard in Azure Portal"
echo "3. Test the alert notifications"

exit 0
