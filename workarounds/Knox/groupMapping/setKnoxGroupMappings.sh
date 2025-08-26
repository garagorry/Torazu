#!/usr/bin/env bash
#===============================================================================
# Script: setKnoxGroupMappings.sh
# Purpose: Set Knox group mappings in Cloudera Manager cluster configuration
# Author: Jimmy Garagorry
# Version: 2.0
#===============================================================================

# Exit on any error, undefined variable, or pipe failure
set -euo pipefail

# Script configuration
readonly SCRIPT_NAME="${0##*/}"
readonly SCRIPT_VERSION="2.0"
readonly REQUIRED_COMMANDS=("curl" "jq" "psql" "awk" "grep" "sed")

# Color codes for output formatting
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# Global variables
WORKLOAD_USER=""
WORKLOAD_USER_PASS=""
CM_SERVER=""
CM_CLUSTER_NAME=""
CM_API_VERSION=""
OUTPUT_DIR=""
OUT_CLUSTER_SETTINGS=""
CRED_VALIDATED=false
ENV_ASSIGNEES_GROUP_NAME=""

#===============================================================================
# Function: log_message - Print formatted log messages
# Parameters: $1 - Log level (INFO, WARN, ERROR, DEBUG)
#            $2 - Message to log
#===============================================================================
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "${level}" in
        "INFO")
            echo -e "${GREEN}[${timestamp}] [INFO]${NC} ${message}"
            ;;
        "WARN")
            echo -e "${YELLOW}[${timestamp}] [WARN]${NC} ${message}"
            ;;
        "ERROR")
            echo -e "${RED}[${timestamp}] [ERROR]${NC} ${message}"
            ;;
        "DEBUG")
            echo -e "${BLUE}[${timestamp}] [DEBUG]${NC} ${message}"
            ;;
        *)
            echo -e "[${timestamp}] [${level}] ${message}"
            ;;
    esac
}

#===============================================================================
# Function: check_dependencies - Verify required commands are available
#===============================================================================
check_dependencies() {
    log_message "INFO" "Checking required dependencies..."
    
    for cmd in "${REQUIRED_COMMANDS[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            log_message "ERROR" "Required command '$cmd' not found. Please install it and try again."
            exit 1
        fi
    done
    
    log_message "INFO" "All required dependencies are available."
}

#===============================================================================
# Function: run_as_root_check - Verify script is running as root
#===============================================================================
run_as_root_check() {
    if [[ $(id -u) -ne 0 ]]; then
        log_message "ERROR" "This script must be run as root user."
        log_message "INFO" "Please execute: ${GREEN}sudo -i${NC} then run ${GREEN}$0${NC} again"
        exit 1
    fi
    
    log_message "INFO" "Root privileges confirmed."
}

#===============================================================================
# Function: validate_input - Validate user input
# Parameters: $1 - Username to validate
#===============================================================================
validate_input() {
    local username="$1"
    
    if [[ -z "$username" ]]; then
        log_message "ERROR" "Username cannot be empty."
        return 1
    fi
    
    if [[ ! "$username" =~ ^[a-zA-Z0-9._-]+$ ]]; then
        log_message "ERROR" "Username contains invalid characters. Use only alphanumeric, dots, underscores, and hyphens."
        return 1
    fi
    
    return 0
}

#===============================================================================
# Function: validate_group_name - Validate environment assignees group name
# Parameters: $1 - Group name to validate
#===============================================================================
validate_group_name() {
    local group_name="$1"
    
    if [[ -z "$group_name" ]]; then
        log_message "ERROR" "Environment assignees group name is required as a command line argument."
        log_message "INFO" "Usage: $0 <group_name>"
        log_message "INFO" "Example: $0 _c_env_assignees_6bb5c9ab"
        exit 1
    fi
    
    if [[ ! "$group_name" =~ ^[a-zA-Z0-9._-]+$ ]]; then
        log_message "ERROR" "Group name contains invalid characters. Use only alphanumeric, dots, underscores, and hyphens."
        exit 1
    fi
    
    ENV_ASSIGNEES_GROUP_NAME="$group_name"
    log_message "INFO" "Group name validated: $ENV_ASSIGNEES_GROUP_NAME"
}

#===============================================================================
# Function: get_secure_password - Securely prompt for password input
#===============================================================================
get_secure_password() {
    local password=""
    local char_count=0
    
    log_message "INFO" "Enter your Workload user password: "
    
    # Save current terminal settings
    local old_stty=$(stty -g)
    
    # Disable echo and set terminal to raw mode
    stty -echo -icanon
    
    while IFS= read -r -n1 -s char; do
        case "${char}" in
            $'\0')  # Enter key
                break
                ;;
            $'\177') # Backspace key
                if [ ${#password} -gt 0 ]; then
                    echo -ne "\b \b"
                    password=${password::-1}
                    char_count=$((char_count - 1))
                fi
                ;;
            *)
                char_count=$((char_count + 1))
                echo -n '*'
                password+="${char}"
                ;;
        esac
    done
    
    echo  # New line after password input
    
    # Restore terminal settings
    stty "$old_stty"
    
    if [[ -z "$password" ]]; then
        log_message "ERROR" "Password cannot be empty."
        return 1
    fi
    
    WORKLOAD_USER_PASS="$password"
    log_message "INFO" "Password received successfully."
    return 0
}

#===============================================================================
# Function: load_cm_configuration - Load Cloudera Manager configuration
#===============================================================================
load_cm_configuration() {
    log_message "INFO" "Loading Cloudera Manager configuration..."
    
    local cm_server_db_file="/etc/cloudera-scm-server/db.properties"
    
    if [[ ! -f "$cm_server_db_file" ]]; then
        log_message "ERROR" "Cloudera Manager database properties file not found: $cm_server_db_file"
        exit 1
    fi
    
    # Extract database configuration
    export CM_DB_HOST=$(awk -F"=" '/^com\.cloudera\.cmf\.db\.host/ {print $NF}' "$cm_server_db_file" | tr -d ' ')
    export CM_DB_NAME=$(awk -F"=" '/^com\.cloudera\.cmf\.db\.name/ {print $NF}' "$cm_server_db_file" | tr -d ' ')
    export CM_DB_USER=$(awk -F"=" '/^com\.cloudera\.cmf\.db\.user/ {print $NF}' "$cm_server_db_file" | tr -d ' ')
    export PGPASSWORD=$(awk -F"=" '/^com\.cloudera\.cmf\.db\.password/ {print $NF}' "$cm_server_db_file" | tr -d ' ')
    
    # Validate extracted values
    if [[ -z "$CM_DB_HOST" || -z "$CM_DB_NAME" || -z "$CM_DB_USER" || -z "$PGPASSWORD" ]]; then
        log_message "ERROR" "Failed to extract database configuration from $cm_server_db_file"
        exit 1
    fi
    
    # Get cluster name from database
    CM_CLUSTER_NAME=$(echo "SELECT name FROM clusters;" | psql -h "$CM_DB_HOST" -U "$CM_DB_USER" -d "$CM_DB_NAME" 2>/dev/null | grep -v Proxy | tail -n 3 | head -n1 | sed 's| ||g')
    
    if [[ -z "$CM_CLUSTER_NAME" ]]; then
        log_message "ERROR" "Failed to retrieve cluster name from database"
        exit 1
    fi
    
    # Set CM server URL and output directory
    CM_SERVER="https://$(hostname -f):7183"
    OUTPUT_DIR="/tmp/$(hostname -f)/$(date +"%Y%m%d%H%M%S")"
    OUT_CLUSTER_SETTINGS="${OUTPUT_DIR}/${CM_CLUSTER_NAME}_$(date +"%Y%m%d%H%M%S")"
    
    log_message "INFO" "Configuration loaded:"
    log_message "INFO" "  Cluster: $CM_CLUSTER_NAME"
    log_message "INFO" "  Server: $CM_SERVER"
    log_message "INFO" "  Output: $OUTPUT_DIR"
}

#===============================================================================
# Function: test_credentials - Validate user credentials against CM API
#===============================================================================
test_credentials() {
    log_message "INFO" "Testing user credentials..."
    
    local temp_file="/tmp/cm_auth_test_$$"
    
    # Test API connection
    if curl -s -L -k -u "${WORKLOAD_USER}:${WORKLOAD_USER_PASS}" -X GET "${CM_SERVER}/api/version" > "$temp_file" 2>&1; then
        if grep -q "Bad credentials" "$temp_file" 2>/dev/null; then
            CRED_VALIDATED=false
            log_message "ERROR" "Invalid credentials provided. Please check username and password."
        else
            CRED_VALIDATED=true
            log_message "INFO" "Credentials validated successfully."
        fi
    else
        CRED_VALIDATED=false
        log_message "ERROR" "Failed to connect to Cloudera Manager API. Check server connectivity."
    fi
    
    # Clean up temporary file
    rm -f "$temp_file"
    
    if [[ "$CRED_VALIDATED" == false ]]; then
        return 1
    fi
    
    return 0
}

#===============================================================================
# Function: get_api_version - Retrieve CM API version
#===============================================================================
get_api_version() {
    log_message "INFO" "Retrieving Cloudera Manager API version..."
    
    CM_API_VERSION=$(curl -s -L -k -u "${WORKLOAD_USER}:${WORKLOAD_USER_PASS}" -X GET "${CM_SERVER}/api/version")
    
    if [[ -z "$CM_API_VERSION" ]]; then
        log_message "ERROR" "Failed to retrieve API version"
        exit 1
    fi
    
    log_message "INFO" "API Version: $CM_API_VERSION"
}

#===============================================================================
# Function: prepare_output_directory - Create and prepare output directory
#===============================================================================
prepare_output_directory() {
    log_message "INFO" "Preparing output directory..."
    
    if [[ -d "$OUTPUT_DIR" ]]; then
        log_message "WARN" "Output directory already exists. Removing and recreating..."
        rm -rf "$OUTPUT_DIR"
    fi
    
    if mkdir -p "$OUTPUT_DIR"; then
        log_message "INFO" "Output directory created: $OUTPUT_DIR"
    else
        log_message "ERROR" "Failed to create output directory: $OUTPUT_DIR"
        exit 1
    fi
}

#===============================================================================
# Function: update_knox_configuration - Update Knox gateway configuration
#===============================================================================
update_knox_configuration() {
    log_message "INFO" "Updating Knox gateway configuration..."
    
    local api_url="${CM_SERVER}/api/${CM_API_VERSION}/clusters/${CM_CLUSTER_NAME}/services/knox/roleConfigGroups/knox-KNOX_GATEWAY-BASE/config"
    local config_data="{
        \"items\": [
            {
                \"name\": \"conf/gateway-site.xml_role_safety_valve\",
                \"value\": \"<property><name>gateway.group.config.group.mapping.${ENV_ASSIGNEES_GROUP_NAME}</name><value>(!= 0 (size groups))</value></property>\"
            }
        ]
    }"
    
    log_message "INFO" "Applying Knox configuration update..."
    log_message "INFO" "  Group mapping: gateway.group.config.group.mapping.${ENV_ASSIGNEES_GROUP_NAME}"
    
    if curl -s -L -k -u "${WORKLOAD_USER}:${WORKLOAD_USER_PASS}" \
        -X PUT \
        -H "Content-Type:application/json" \
        "$api_url" \
        -d "$config_data"; then
        
        log_message "INFO" "Knox configuration updated successfully."
    else
        log_message "ERROR" "Failed to update Knox configuration."
        exit 1
    fi
}

#===============================================================================
# Function: restart_knox_service - Restart Knox service
#===============================================================================
restart_knox_service() {
    log_message "INFO" "Preparing to restart Knox service..."
    
    echo -e "\nPress Enter to restart Knox service..."
    read -r
    
    log_message "INFO" "Initiating Knox service restart..."
    
    if curl -s -L -k -u "${WORKLOAD_USER}:${WORKLOAD_USER_PASS}" \
        -X POST "${CM_SERVER}/api/${CM_API_VERSION}/clusters/${CM_CLUSTER_NAME}/services/knox/commands/restart"; then
        
        log_message "INFO" "Knox service restart command sent successfully."
    else
        log_message "ERROR" "Failed to send Knox restart command."
        exit 1
    fi
    
    log_message "INFO" "Waiting for restart to complete..."
    sleep 5
    
    # Check service status
    log_message "INFO" "Checking Knox service status..."
    if curl -s -L -k -u "${WORKLOAD_USER}:${WORKLOAD_USER_PASS}" \
        -X POST "${CM_SERVER}/api/${CM_API_VERSION}/clusters/${CM_CLUSTER_NAME}/services/knox/commands/status"; then
        
        log_message "INFO" "Knox service status retrieved."
    else
        log_message "WARN" "Failed to retrieve Knox service status."
    fi
    
    sleep 10
}

#===============================================================================
# Function: cleanup_supervisor - Cleanup supervisor processes
#===============================================================================
cleanup_supervisor() {
    log_message "INFO" "Cleaning up supervisor processes..."
    
    if echo "exit" | /opt/cloudera/cm-agent/bin/supervisorctl -c /var/run/cloudera-scm-agent/supervisor/supervisord.conf; then
        log_message "INFO" "Supervisor cleanup completed successfully."
    else
        log_message "WARN" "Supervisor cleanup may not have completed successfully."
    fi
}

#===============================================================================
# Function: cleanup - Cleanup function for script exit
#===============================================================================
cleanup() {
    log_message "INFO" "Script execution completed."
    log_message "INFO" "Knox group mapping has been updated for: $ENV_ASSIGNEES_GROUP_NAME"
}

#===============================================================================
# Function: main - Main execution function
#===============================================================================
main() {
    local group_name="$1"
    
    log_message "INFO" "Starting $SCRIPT_NAME v$SCRIPT_VERSION"
    
    # Validate command line argument
    validate_group_name "$group_name"
    
    # Set up cleanup trap
    trap cleanup EXIT
    
    # Check dependencies and privileges
    check_dependencies
    run_as_root_check
    
    # Clear screen and get user input
    clear
    log_message "INFO" "Knox Group Mapping Configuration Tool"
    log_message "INFO" "====================================="
    log_message "INFO" "Target Group: $ENV_ASSIGNEES_GROUP_NAME"
    
    # Get username
    while true; do
        read -p "Enter your Workload username: " WORKLOAD_USER
        if validate_input "$WORKLOAD_USER"; then
            break
        fi
    done
    
    # Get password securely
    while ! get_secure_password; do
        log_message "WARN" "Please try again."
    done
    
    # Load configuration and perform operations
    load_cm_configuration
    
    if test_credentials; then
        get_api_version
        prepare_output_directory
        update_knox_configuration
        restart_knox_service
        cleanup_supervisor
        log_message "INFO" "Knox group mapping configuration completed successfully!"
    else
        log_message "ERROR" "Credential validation failed. Exiting."
        exit 1
    fi
}

#===============================================================================
# Script execution
#===============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ $# -eq 0 ]]; then
        log_message "ERROR" "No arguments provided."
        log_message "INFO" "Usage: $0 <group_name>"
        log_message "INFO" "Example: $0 _c_env_assignees_6bb5c9ab"
        exit 1
    fi
    
    main "$1"
fi