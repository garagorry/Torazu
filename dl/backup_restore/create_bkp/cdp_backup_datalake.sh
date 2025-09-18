#!/bin/bash

# Enhanced CDP Datalake Backup Script
# This script automates the process of:
# 1. Getting the environment CRN from datalake describe
# 2. Getting the environment name from the CRN
# 3. Getting the backup location from the environment
# 4. Creating a backup with validation

set -e  # Exit on any error

# Function to display usage
usage() {
    echo "Usage: $0 --datalake-name <name> [--backup-name <name>] [--validation-only]"
    echo "   or: $0 -d <name> [-b <name>] [--validation-only]"
    echo ""
    echo "Arguments:"
    echo "  --datalake-name, -d    Name of the datalake to backup (required)"
    echo "  --backup-name, -b      Name for the backup (optional, defaults to datalake-name-backup-<timestamp>)"
    echo "  --validation-only      Flag to only validate the backup without creating it (optional)"
    echo "  --help, -h             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --datalake-name jdga-it1-aw-dl"
    echo "  $0 -d jdga-it1-aw-dl --backup-name my-backup-test"
    echo "  $0 -d jdga-it1-aw-dl -b my-backup-test --validation-only"
    echo "  $0 --datalake-name jdga-it1-aw-dl --validation-only"
    exit 1
}

# Initialize variables
DL_NAME=""
BACKUP_NAME=""
VALIDATION_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --datalake-name|-d)
            if [ -z "$2" ]; then
                echo "Error: --datalake-name requires a value"
                usage
            fi
            DL_NAME="$2"
            shift 2
            ;;
        --backup-name|-b)
            if [ -z "$2" ]; then
                echo "Error: --backup-name requires a value"
                usage
            fi
            BACKUP_NAME="$2"
            shift 2
            ;;
        --validation-only)
            VALIDATION_ONLY=true
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "Error: Unknown argument '$1'"
            usage
            ;;
    esac
done

# Check if datalake name is provided
if [ -z "$DL_NAME" ]; then
    echo "Error: Datalake name is required. Use --datalake-name or -d"
    usage
fi

# Set default backup name if not provided
if [ -z "$BACKUP_NAME" ]; then
    BACKUP_NAME="${DL_NAME}-backup-$(date +%Y%m%d-%H%M%S)"
fi

echo "=========================================="
echo "CDP Datalake Backup Script"
echo "=========================================="
echo "Datalake Name: $DL_NAME"
echo "Backup Name: $BACKUP_NAME"
echo "Validation Only: $VALIDATION_ONLY"
echo "=========================================="

# Step 1: Get the environment CRN from datalake describe
echo "Step 1: Getting environment CRN from datalake..."
ENV_CRN=$(cdp datalake describe-datalake --datalake-name "$DL_NAME" 2>/dev/null | jq -r '.datalake.environmentCrn')

if [ "$ENV_CRN" = "null" ] || [ -z "$ENV_CRN" ]; then
    echo "Error: Could not retrieve environment CRN for datalake $DL_NAME"
    exit 1
fi

echo "Environment CRN: $ENV_CRN"

# Step 2: Get the environment name from the CRN
echo "Step 2: Getting environment name from CRN..."
ENV_NAME=$(cdp environments list-environments 2>/dev/null | jq -r --arg crn "$ENV_CRN" '.environments[] | select(.crn == $crn) | .environmentName')

if [ "$ENV_NAME" = "null" ] || [ -z "$ENV_NAME" ]; then
    echo "Error: Could not find environment name for CRN $ENV_CRN"
    exit 1
fi

echo "Environment Name: $ENV_NAME"

# Step 3: Get the backup location
echo "Step 3: Getting backup location..."
BACKUP_LOCATION=$(cdp environments describe-environment --environment-name "$ENV_NAME" 2>/dev/null | jq -r '.environment.backupStorage.awsDetails.storageLocationBase')

if [ "$BACKUP_LOCATION" = "null" ] || [ -z "$BACKUP_LOCATION" ]; then
    echo "Error: Could not retrieve backup location for environment $ENV_NAME"
    exit 1
fi

echo "Backup Location: $BACKUP_LOCATION"

# Step 4: Create the backup
echo "Step 4: Creating backup..."

if [ "$VALIDATION_ONLY" = true ]; then
    echo "Creating validation-only backup..."
    BACKUP_ID=$(cdp datalake backup-datalake \
        --datalake-name "$DL_NAME" \
        --backup-name "$BACKUP_NAME" \
        --backup-location "$BACKUP_LOCATION" \
        --validation-only 2>/dev/null | jq -r '.backupId')
else
    echo "Creating actual backup..."
    BACKUP_ID=$(cdp datalake backup-datalake \
        --datalake-name "$DL_NAME" \
        --backup-name "$BACKUP_NAME" \
        --backup-location "$BACKUP_LOCATION" 2>/dev/null | jq -r '.backupId')
fi

if [ "$BACKUP_ID" = "null" ] || [ -z "$BACKUP_ID" ]; then
    echo "Error: Could not create backup for datalake $DL_NAME"
    exit 1
fi

echo "Backup ID: $BACKUP_ID"

# Step 5: Check backup status with polling
echo "Step 5: Monitoring backup status..."
echo "Backup ID: $BACKUP_ID"
echo ""

# Function to display predicted durations
show_predicted_durations() {
    local status_output="$1"
    echo "=========================================="
    echo "PREDICTED DURATION SUMMARY"
    echo "=========================================="
    
    # Get overall predicted duration
    local total_duration=$(echo "$status_output" | jq -r '.totalPredictedDurationInMinutes // "N/A"')
    echo "Total Predicted Duration: ${total_duration} minutes"
    echo ""
    
    # Get individual component durations
    echo "Component Breakdown:"
    echo "-------------------"
    
    # Admin Operations
    echo "Admin Operations:"
    local precheck_duration=$(echo "$status_output" | jq -r '.operationStates.adminOperations.precheckStoragePermission.durationInMinutes // .operationStates.adminOperations.precheckStoragePermission.predictedDurationInMinutes // "N/A"')
    local ranger_duration=$(echo "$status_output" | jq -r '.operationStates.adminOperations.rangerAuditCollectionValidation.durationInMinutes // .operationStates.adminOperations.rangerAuditCollectionValidation.predictedDurationInMinutes // "N/A"')
    local dryrun_duration=$(echo "$status_output" | jq -r '.operationStates.adminOperations.dryRunValidation.durationInMinutes // .operationStates.adminOperations.dryRunValidation.predictedDurationInMinutes // "N/A"')
    
    echo "  - Storage Permission Check: ${precheck_duration} minutes"
    echo "  - Ranger Audit Collection: ${ranger_duration} minutes"
    echo "  - Dry Run Validation: ${dryrun_duration} minutes"
    echo ""
    
    # HBase Operations
    echo "HBase Operations:"
    local atlas_audit_duration=$(echo "$status_output" | jq -r '.operationStates.hbase.atlasEntityAuditEventTable.predictedDurationInMinutes // "N/A"')
    local atlas_janus_duration=$(echo "$status_output" | jq -r '.operationStates.hbase.atlasJanusTable.predictedDurationInMinutes // "N/A"')
    
    echo "  - Atlas Entity Audit Events: ${atlas_audit_duration} minutes"
    echo "  - Atlas Janus Table: ${atlas_janus_duration} minutes"
    echo ""
    
    # Solr Operations
    echo "Solr Operations:"
    local edge_index_duration=$(echo "$status_output" | jq -r '.operationStates.solr.edgeIndexCollection.predictedDurationInMinutes // "N/A"')
    local fulltext_index_duration=$(echo "$status_output" | jq -r '.operationStates.solr.fulltextIndexCollection.predictedDurationInMinutes // "N/A"')
    local ranger_audits_duration=$(echo "$status_output" | jq -r '.operationStates.solr.rangerAuditsCollection.predictedDurationInMinutes // "N/A"')
    local vertex_index_duration=$(echo "$status_output" | jq -r '.operationStates.solr.vertexIndexCollection.predictedDurationInMinutes // "N/A"')
    
    echo "  - Edge Index Collection: ${edge_index_duration} minutes"
    echo "  - Fulltext Index Collection: ${fulltext_index_duration} minutes"
    echo "  - Ranger Audits Collection: ${ranger_audits_duration} minutes"
    echo "  - Vertex Index Collection: ${vertex_index_duration} minutes"
    echo ""
    
    # Database Operations
    echo "Database Operations:"
    local database_duration=$(echo "$status_output" | jq -r '.operationStates.database.database.predictedDurationInMinutes // "N/A"')
    echo "  - Database: ${database_duration} minutes"
    echo ""
}

# Poll for status until completion
MAX_ATTEMPTS=60  # Maximum 60 attempts (5 minutes with 5-second intervals)
ATTEMPT=0
STATUS=""

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    ATTEMPT=$((ATTEMPT + 1))
    
    echo "Checking status (attempt $ATTEMPT/$MAX_ATTEMPTS)..."
    STATUS_OUTPUT=$(cdp datalake backup-datalake-status --datalake-name "$DL_NAME" --backup-id "$BACKUP_ID" 2>/dev/null)
    STATUS=$(echo "$STATUS_OUTPUT" | jq -r '.status')
    
    echo "Current Status: $STATUS"
    
    case "$STATUS" in
        "VALIDATION_SUCCESSFUL")
            echo "✅ Validation completed successfully!"
            show_predicted_durations "$STATUS_OUTPUT"
            break
            ;;
        "VALIDATION_FAILED"|"FAILED")
            echo "❌ Validation failed!"
            echo "Failure Reason: $(echo "$STATUS_OUTPUT" | jq -r '.failureReason // "Unknown"')"
            exit 1
            ;;
        "RUNNING"|"IN_PROGRESS"|"PENDING")
            echo "⏳ Validation in progress..."
            if [ $ATTEMPT -eq 1 ]; then
                show_predicted_durations "$STATUS_OUTPUT"
            fi
            echo "Waiting 5 seconds before next check..."
            sleep 5
            ;;
        *)
            echo "⚠️  Unknown status: $STATUS"
            echo "Waiting 5 seconds before next check..."
            sleep 5
            ;;
    esac
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "❌ Timeout: Maximum attempts reached. Status: $STATUS"
    exit 1
fi

echo ""
echo "=========================================="
echo "Backup process completed!"
echo "Backup ID: $BACKUP_ID"
echo "Backup Name: $BACKUP_NAME"
echo "Backup Location: $BACKUP_LOCATION"
echo "=========================================="
