# CDP Knox Group Mapping Management Scripts - Data Lake Resize Workaround

## Overview

This directory contains two essential scripts designed to support a **specific workaround** for the Data Lake resize flow at **(Customer)**. These scripts address a unique issue that occurs **only during Data Lake resize operations** where large POSIX groups in CDP cause the resize flow to fail.

**⚠️ IMPORTANT: This is NOT a regular practice or standard procedure. This workaround is designed specifically to facilitate the Data Lake resize flow that is expected to fail due to unique group membership settings.**

## Problem Context

### The Data Lake Resize Issue

- **Specific Scenario**: This workaround addresses a **unique issue that occurs ONLY during Data Lake resize operations** at the customer
- **Root Cause**: CDP creates two POSIX groups by default that include all synced users in an environment
- **Impact**: These large group memberships cause the **Data Lake resize flow to fail** due to group membership limitations
- **Solution**: Temporarily convert these groups to NON-POSIX to allow the resize flow to complete successfully
- **Scope**: This is **NOT a general scaling solution** - it's a targeted workaround for a specific resize operation

### When This Workaround Applies

**ONLY use this workaround when:**

- ✅ You are performing a **Data Lake resize operation** at customer
- ✅ The resize flow has **already failed** due to group membership issues
- ✅ You need to **complete the resize flow** that was interrupted

**DO NOT use this workaround for:**

- ❌ Regular cluster operations
- ❌ General scaling issues
- ❌ Preventive maintenance
- ❌ Other environments or customers

### Required Manual Process

To complete the **failed Data Lake resize flow**, the following steps must be executed:

1. **Find Group Name**: Discover the unique environment assignees group name for the specific environment
2. **Knox Gateway Changes**: Update the Knox gateway configuration to support the new group topology
3. **FreeIPA Changes**: Convert `cdp-usersync-internal` and `_c_env_assignees` groups to non-POSIX
4. **Retry Resize**: Use the management console to retry the latest failed resize event

## Scripts Overview

### 1. `getKnoxGroupMappings.sh` - Configuration Discovery Tool

**Purpose**: Extract and analyze Cloudera Manager cluster configuration data to discover current Knox group mappings and cluster settings **before applying the workaround**.

**What it does**:

- Connects to Cloudera Manager database to retrieve cluster information
- Authenticates with Cloudera Manager API using workload user credentials
- Extracts comprehensive cluster configuration data including:
  - Service configurations
  - Role configurations
  - Role config group configurations
- Outputs structured JSON data for analysis
- Helps identify current Knox gateway settings and group mappings

**Use Case**:

- **Documentation** of existing configuration before applying the workaround
- **Rollback preparation** in case the workaround needs to be reversed
- **Analysis** of current Knox gateway topology settings

### 2. `setKnoxGroupMappings.sh` - Knox Configuration Update Tool

**Purpose**: Apply Knox gateway configuration changes to support the temporary non-POSIX group topology **required for the Data Lake resize workaround**.

**What it does**:

- Takes a group name as a command line argument
- Updates Knox gateway configuration via Cloudera Manager API
- Applies the safety valve configuration for group mapping
- Restarts Knox service to apply changes
- Performs cleanup operations

**Use Case**:

- **Implementation** of the temporary Knox topology configuration
- **Automation** of the manual Knox gateway changes required for the workaround
- **Consistency** in applying the workaround across the affected cluster

## Prerequisites

### System Requirements

- **Operating System**: Linux (tested on RHEL/CentOS)
- **User**: Must be run as root user
- **Network**: Access to Cloudera Manager API and database

### Required Commands

The following commands must be available on the system:

- `curl` - For API communication
- `jq` - For JSON processing
- `psql` - For PostgreSQL database access
- `awk` - For text processing
- `grep` - For pattern matching
- `sed` - For text manipulation

### Access Requirements

- **Root privileges** on the Cloudera Manager server
- **Workload user credentials** with API access
- **Database access** to Cloudera Manager PostgreSQL database

## Installation & Setup

### 1. Download Scripts

```bash
# Ensure scripts are executable
chmod +x getKnoxGroupMappings.sh
chmod +x setKnoxGroupMappings.sh
```

### 2. Verify Dependencies

```bash
# Check if required commands are available
which curl jq psql awk grep sed
```

### 3. Verify Access

```bash
# Ensure you can access the Cloudera Manager database properties
ls -la /etc/cloudera-scm-server/db.properties

# Verify you can connect to the database
# (The scripts will test this automatically)
```

## Usage

### Getting Current Configuration

**Command**:

```bash
# ./getKnoxGroupMappings.sh
```

**Process**:

1. Script prompts for workload username and password
2. Automatically discovers cluster configuration
3. Extracts all service and role configurations
4. Outputs structured data to timestamped directory

**Output Location**:

```
/tmp/<hostname>/<timestamp>/<cluster_name>_<timestamp>
```

**Extracting Group Name**:

After running the script, use this command to extract the required group name:

```bash
grep -A1 "conf/gateway-site.xml_role_safety_valve" /tmp/$(hostname -f)/$(date +"%Y%m%d%H%M%S")/*
```

**Example Output**:

```
"name": "conf/gateway-site.xml_role_safety_valve",
"value": "<property><name>gateway.group.config.group.mapping._c_env_assignees_6bb5c9ab</name><value>(!= 0 (size groups))</value></property>"
```

**Note**: The group name (e.g., `_c_env_assignees_6bb5c9ab`) is what you'll need for the `setKnoxGroupMappings.sh` script.

### Setting Knox Group Mappings

**Command**:

```bash
# ./setKnoxGroupMappings.sh <group_name>
```

**Example**:

```bash
# ./setKnoxGroupMappings.sh _c_env_assignees_6bb5c9ab
```

**Process**:

1. Validates the provided group name
2. Prompts for workload username and password
3. Updates Knox gateway configuration
4. Prompts for confirmation before restarting Knox service
5. Applies changes and verifies status

## Workflow for Data Lake Resize Workaround

### Phase 1: Pre-Resize Preparation (BEFORE Starting Resize)

```bash
# 1. Document current state BEFORE starting any resize operation
./getKnoxGroupMappings.sh

# 2. Extract the group name from the output file
grep -A1 "conf/gateway-site.xml_role_safety_valve" /tmp/$(hostname -f)/$(date +"%Y%m%d%H%M%S")/*

# 3. Note the environment assignees group name for later use
# Example output might show: gateway.group.config.group.mapping._c_env_assignees_6bb5c9ab
```

### Phase 2: Start Data Lake Resize (Normal Process)

```bash
# 1. Initiate Data Lake resize through CDP CLI
# 2. Monitor the resize operation
# 3. Wait for the resize to fail (this is expected)
```

### Phase 3: Apply the Workaround

```bash
# 1. Apply Knox configuration changes using the group name discovered in Phase 1
./setKnoxGroupMappings.sh <discovered_group_name>

# 2. Apply FreeIPA changes to convert groups to non-POSIX
# 3. Verify all changes are applied successfully
```

### Phase 4: Complete the Resize Flow

```bash
# 1. Return to Cloudera Manager management console
# 2. Locate the latest failed resize event
# 3. Retry the failed event from the management console
# 4. Monitor the resize operation to completion
# 5. Verify the Data Lake resize completed successfully
```

### Phase 5: Post-Resize Verification

```bash
# 1. Verify the Data Lake resize completed successfully
# 2. Test basic cluster functionality
# 3. Document the workaround was applied and resize completed
```

## Output and Logging

### Log Format

Both scripts provide structured logging with:

- **Timestamps** for all operations
- **Color-coded** messages (INFO, WARN, ERROR, DEBUG)
- **Progress indicators** for long-running operations
- **Clear error messages** with actionable guidance

### Output Files

- **Configuration extracts** in JSON format
- **Log files** with detailed operation history
- **Timestamped directories** for easy tracking

## Error Handling

### Common Issues and Solutions

**Database Connection Failed**:

- Verify Cloudera Manager is running
- Check database properties file exists
- Ensure network connectivity to database

**Authentication Failed**:

- Verify workload user credentials
- Check user has API access permissions
- Ensure Cloudera Manager API is accessible

**Permission Denied**:

- Ensure script is run as root user
- Verify script has execute permissions
- Check file ownership and permissions

## Security Considerations

### Credential Handling

- **Password masking** during input
- **Secure credential validation**
- **Temporary file cleanup**
- **No credential storage** in scripts

### Access Control

- **Root privilege requirement** for system-level operations
- **API authentication** for Cloudera Manager access
- **Database access** limited to configuration retrieval

## Troubleshooting

### Debug Mode

Both scripts provide detailed logging to help diagnose issues:

- **Dependency checks** with clear error messages
- **Configuration validation** with specific failure points
- **API interaction** logging for troubleshooting
- **Database operation** status reporting

### Common Debug Steps

1. **Check script permissions**: `ls -la *.sh`
2. **Verify dependencies**: `which curl jq psql awk grep sed`
3. **Test database access**: Check `/etc/cloudera-scm-server/db.properties`
4. **Verify API access**: Test Cloudera Manager web interface
5. **Check logs**: Review script output for specific error messages

## Support and Maintenance

### Version Information

- **Current Version**: 2.0
- **Last Updated**: August 2025
- **Compatibility**: CDP 7.2.17.x, Cloudera Manager 7.1x
- **Use Case**: Data Lake resize workaround only

## Important Notes and Warnings

### ⚠️ Critical Warnings

1. **This is NOT a regular practice**: These scripts are designed for a specific workaround scenario only
2. **Customer-specific**: This workaround is designed specifically for a Customer Data Lake resize operations
3. **Temporary solution**: The group changes are meant to be temporary to allow the resize to complete
4. **Expected failure**: The Data Lake resize is expected to fail first, then the workaround is applied
5. **Management console retry**: After applying the workaround, the resize must be retried from the Cloudera Manager console

### 🔄 Workaround Flow Summary

```
1. Prepare (getKnoxGroupMappings.sh) → 2. Start resize → 3. Wait for failure → 4. Apply workaround → 5. Retry from console → 6. Complete resize
```

### 📋 Success Criteria

The workaround is successful when:

- ✅ The Data Lake resize operation completes successfully after retry
- ✅ All cluster services are functioning normally
- ✅ The temporary group changes allow the resize to proceed
- ✅ The resize operation reaches completion status

---

## Final Important Notes

**⚠️ CRITICAL REMINDERS:**

1. **This is a WORKAROUND, not a standard procedure**
2. **ONLY use for a Customer Data Lake resize operations**
3. **Run getKnoxGroupMappings.sh BEFORE starting the resize to identify the group name**
4. **The resize MUST fail first before applying this workaround**
5. **After applying the workaround, retry the resize from the Cloudera Manager console**
6. **This is NOT a general scaling solution or regular maintenance procedure**

**Always test in a non-production environment first and ensure proper backup procedures are in place before applying changes to production systems.**
