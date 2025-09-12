# Datahub Request Template Generator

This Python script generates DataHub request templates from either running Cloudera On Cloud DataHub clusters or JSON files containing cluster description data. The generated templates follow the `DistroXV1Request` structure and can be used to recreate or modify DataHub clusters with advanced customization options.

## Features

- **Dual Input Sources**: Generate templates from running clusters via CDP CLI or JSON files
- **Dynamic Template Generation**: Creates templates based on actual cluster configurations
- **Instance Group Overrides**: Customize node counts, instance types, volumes, and more per instance group
- **Volume Type Conversion**: Automatically converts gp2 volumes to gp3 while preserving ephemeral volumes
- **Subnet Configuration**: Apply single or multiple subnet IDs across all instance groups
- **Bucket Name Management**: Automatic S3 bucket name extraction from datalake CRN or manual override
- **Tag Management**: Comprehensive tag handling with CLI command integration
- **Load Balancer & Multi-AZ**: Configurable load balancer and multi-AZ settings
- **Timestamped Output**: Automatically adds timestamps to output directories and files
- **Flexible Output Location**: Customizable output directory with intelligent defaults
- **Comprehensive Logging**: Detailed logging for debugging and monitoring
- **Error Handling**: Robust error handling with informative error messages
- **CLI Command Parsing**: Parse DataHub create CLI commands to enrich templates with additional configuration

## Prerequisites

1. **Python 3.7+** installed on your system
2. **CDP CLI** installed and configured (for running cluster queries)
3. **Valid CDP credentials** configured in your environment

### Installing CDP CLI

```bash
# Install CDP CLI (follow official documentation)
# https://docs.cloudera.com/cdp-public-cloud/cloud/cli/topics/mc-cli-install.html

# Configure credentials
cdp configure
```

## Installation

1. Clone or download the script to your local machine
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Syntax

```bash
python generate_request_template.py [OPTIONS]
```

### Input Source Options (Required - Choose One)

- `--cluster-name CLUSTER_NAME` or `-c CLUSTER_NAME`: Generate template from a running cluster
- `--input-file FILE_PATH` or `-f FILE_PATH`: Generate template from a JSON file

### Output Options

- `--output DIRECTORY` or `-o DIRECTORY`: Specify output directory (optional)

### Override Options

- `--environment-name NAME` or `-e NAME`: Override environment name in template
- `--bucket-name NAME` or `-b NAME`: Override S3 bucket name in template
- `--datalake-name NAME` or `-d NAME`: Override datalake name in template
- `--dh-name NAME`: Override DataHub cluster name (highest priority for cluster name)

### Instance Group Override Options

- `--instance-groups CONFIG [CONFIG ...]`: Override instance group configurations
  - Format: `nodeCount=2,instanceGroupName=core,instanceGroupType=CORE,instanceType=m6i.4xlarge,attachedVolumeConfiguration=[{volumeSize=256,volumeCount=2,volumeType=gp3}],rootVolumeSize=200,recipeNames=recipe1,recipe2,recoveryMode=MANUAL`
  - Can specify multiple instance groups with separate quoted strings

### Network Configuration Options

- `--subnet-id SUBNET_ID`: Apply a single subnet ID to all instance groups
- `--subnet-ids SUBNET_ID [SUBNET_ID ...]`: Apply multiple subnet IDs to all instance groups

### CLI Command File Option (Optional)

- `--cli-command-file FILE_PATH` or `-l FILE_PATH`: Path to a file containing the CDP DataHub create command exported from the CDP UI. This augments the generated template with details not returned by the describe API (e.g., tags, `--subnet-id`, `--multi-az`, `--enable-load-balancer`, `--datahub-database`, and `--instance-groups` overrides).

## Execution Options

### **Option 1 – Using `--input-file` (Describe JSON Output)**

This option requires the JSON output from the Datahub describe API, typically exported from the CDP CLI.

- **`--input-file`**: Path to the Datahub describe JSON output file.
  (Example: `ENVIRONMENT_ENV_dhcore-de_20250718233637.json`)
- **`--environment-name`**: Name of the CDP environment where the request template should be updated.
- **`--cli-command-file`**: File exported from the CDP UI (per DataHub cluster). Used to extract tags and additional flags not included in the describe output.

Example command:

```bash
python generate_request_template.py \
  --input-file ENVIRONMENT_ENV_dhcore-de_20250718233637.json \
  --output /tmp/it4 \
  --environment-name jdga-it1-cdp-env \
  --bucket-name my-s3-bucket \
  --dh-name my-new-cluster \
  --cli-command-file /tmp/dhcore-de-cli.txt
```

### **Option 2 – Using `--cluster-name` (Dynamic Describe Call)**

This option dynamically retrieves the describe data using the cluster name.

- **`--cluster-name`**: Name of the DataHub cluster to retrieve describe information via the API.
- **`--environment-name`**: Name of the CDP environment for updating the request template.
- **`--cli-command-file`**: Same as in Option 1, exported from the CDP UI, to fetch tags and extra flags not returned by the API.

Example command:

```bash
python generate_request_template.py \
  --cluster-name dhcore-de \
  --output /tmp/it5 \
  --environment-name jdga-it1-cdp-env \
  --bucket-name my-s3-bucket \
  --dh-name my-new-cluster \
  --cli-command-file /tmp/dhcore-de-cli.txt
```

## Examples

### Basic Usage

```bash
# Generate from running cluster
python generate_request_template.py --cluster-name jdga-dm-01

# Generate from JSON file
python generate_request_template.py --input-file cluster_data.json

# With custom output directory
python generate_request_template.py --cluster-name jdga-dm-01 --output ./my-templates
```

### Advanced Configuration Examples

#### Instance Group Overrides

```bash
# Override specific instance groups
python generate_request_template.py \
  --cluster-name my-cluster \
  --instance-groups \
    "nodeCount=3,instanceGroupName=worker,instanceGroupType=CORE,instanceType=m6i.4xlarge,attachedVolumeConfiguration=[{volumeSize=512,volumeCount=2,volumeType=gp3}],rootVolumeSize=200" \
    "nodeCount=2,instanceGroupName=manager,instanceGroupType=GATEWAY,instanceType=m6i.2xlarge,attachedVolumeConfiguration=[{volumeSize=256,volumeCount=1,volumeType=gp3}],rootVolumeSize=150" \
  --output ./templates
```

#### Network Configuration

```bash
# Apply single subnet to all instance groups
python generate_request_template.py \
  --cluster-name my-cluster \
  --subnet-id subnet-1234567890abcdef0 \
  --output ./templates

# Apply multiple subnets to all instance groups
python generate_request_template.py \
  --cluster-name my-cluster \
  --subnet-ids subnet-1234567890abcdef0 subnet-0987654321fedcba0 \
  --output ./templates
```

#### Complete Configuration Example

```bash
python generate_request_template.py \
  --cluster-name jdga-dm-01 \
  --environment-name my-env \
  --bucket-name my-s3-bucket \
  --dh-name my-new-dh-cluster \
  --instance-groups \
    "nodeCount=4,instanceGroupName=worker,instanceGroupType=CORE,instanceType=r5d.4xlarge,attachedVolumeConfiguration=[{volumeSize=500,volumeCount=2,volumeType=ephemeral}],rootVolumeSize=200" \
    "nodeCount=1,instanceGroupName=compute,instanceGroupType=CORE,instanceType=m6i.8xlarge,attachedVolumeConfiguration=[{volumeSize=1000,volumeCount=1,volumeType=gp3}],rootVolumeSize=300" \
  --subnet-ids subnet-1234567890abcdef0 subnet-0987654321fedcba0 \
  --cli-command-file /tmp/my-cli-command.txt \
  --output ./templates
```

### Output Structure

The script creates a timestamped output directory structure:

```
output_directory/
└── request-template-20250812_213000/
    ├── cluster-name_running-cluster_template_20250812_213000.json
    └── cluster-name_json-file_template_20250812_213000.json
```

## Generated Template Structure

The generated template follows the `DistroXV1Request` structure with comprehensive configuration:

```json
{
  "environmentName": "environment-name",
  "name": "cluster-name",
  "instanceGroups": [
    {
      "name": "instance-group-name",
      "nodeCount": 1,
      "type": "GATEWAY|CORE",
      "recoveryMode": "MANUAL",
      "minimumNodeCount": 0,
      "scalabilityOption": "ALLOWED",
      "template": {
        "aws": {
          "encryption": { "type": "DEFAULT", "key": null },
          "placementGroup": { "strategy": "PARTITION" }
        },
        "instanceType": "instance-type",
        "rootVolume": { "size": 100 },
        "attachedVolumes": [
          {
            "size": 256,
            "count": 2,
            "type": "gp3"
          }
        ],
        "cloudPlatform": "AWS"
      },
      "recipeNames": [],
      "subnetIds": ["subnet-1234567890abcdef0"]
    }
  ],
  "image": {
    "id": "image-id",
    "catalog": "cdp-default"
  },
  "network": {
    "subnetId": "subnet-1234567890abcdef0",
    "networkId": null
  },
  "cluster": {
    "databases": [],
    "cloudStorage": {
      "locations": [
        {
          "type": "YARN_LOG",
          "value": "s3a://bucket-name/datalake/oplogs/yarn-app-logs"
        },
        {
          "type": "ZEPPELIN_NOTEBOOK",
          "value": "s3a://bucket-name/datalake/cluster-name/zeppelin/notebook"
        }
      ]
    },
    "exposedServices": ["ALL"],
    "blueprintName": "7.2.17 - Data Engineering: HA: Apache Spark, Apache Hive, Apache Oozie",
    "validateBlueprint": false
  },
  "externalDatabase": {
    "availabilityType": "HA"
  },
  "tags": {
    "application": null,
    "userDefined": {
      "generated-date": "20250912_123456",
      "source-cluster": "original-cluster-name",
      "dhname": "cluster-name"
    },
    "defaults": null
  },
  "inputs": {
    "ynlogd.dirs": "/hadoopfs/fs1/nodemanager/log,/hadoopfs/fs2/nodemanager/log",
    "ynld.dirs": "/hadoopfs/fs1/nodemanager,/hadoopfs/fs2/nodemanager",
    "dfs.dirs": "/hadoopfs/fs3/datanode,/hadoopfs/fs4/datanode",
    "query_data_hive_path": "s3a://bucket-name/warehouse/tablespace/external/cluster-name/hive/sys.db/query_data",
    "query_data_tez_path": "s3a://bucket-name/warehouse/tablespace/external/cluster-name/hive/sys.db"
  },
  "gatewayPort": null,
  "enableLoadBalancer": true,
  "variant": "CDP",
  "javaVersion": 8,
  "enableMultiAz": false,
  "architecture": "x86_64",
  "disableDbSslEnforcement": false,
  "security": {}
}
```

## Input JSON File Format

When using `--input-file`, the JSON should contain cluster description data in the format returned by:

```bash
cdp datahub describe-cluster --cluster-name CLUSTER_NAME
```

Example structure:

```json
{
  "cluster": {
    "clusterName": "cluster-name",
    "instanceGroups": [...],
    "imageDetails": {...},
    "environmentName": "env-name",
    "workloadType": "workload-type",
    "multiAz": false,
    "security": {...}
  }
}
```

## Error Handling

The script provides comprehensive error handling:

- **CLI Execution Errors**: Detailed error messages for CDP CLI failures
- **File I/O Errors**: Clear messages for file reading/writing issues
- **JSON Parsing Errors**: Helpful feedback for malformed JSON data
- **Validation Errors**: Checks for required data structures

## Logging

The script uses Python's built-in logging module with:

- **INFO level**: General progress information
- **ERROR level**: Error details and debugging information
- **Timestamped format**: Easy to track execution timeline

## Troubleshooting

### Common Issues

1. **CDP CLI Not Found**

   ```bash
   # Ensure CDP CLI is in PATH
   which cdp

   # Install if missing
   # Follow official installation guide
   ```

2. **Authentication Errors**

   ```bash
   # Reconfigure credentials
   cdp configure

   # Check environment variables
   echo $CDP_ACCESS_KEY_ID
   echo $CDP_PRIVATE_KEY
   ```

3. **Permission Denied**

   ```bash
   # Check file permissions
   ls -la generate_request_template.py

   # Make executable if needed
   chmod +x generate_request_template.py
   ```

### Debug Mode

For additional debugging information, you can modify the logging level in the script:

```python
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
```

## Key Features Explained

### Volume Type Conversion

- **gp2 → gp3**: Automatically converts gp2 volumes to gp3 for better performance and cost optimization
- **ephemeral preservation**: Ephemeral volumes remain unchanged as they serve a specific purpose
- **Manual override**: Use `attachedVolumeConfiguration` in `--instance-groups` to specify exact volume types

### Instance Group Overrides

- **Selective application**: Only instance groups specified in `--instance-groups` are modified
- **Preservation**: Other instance groups retain their original configuration
- **Comprehensive options**: Override node count, instance type, volumes, recipes, and more

### Subnet Configuration

- **Global application**: Subnet IDs from `--subnet-id` or `--subnet-ids` apply to all instance groups
- **Priority order**: `--subnet-ids` > `--subnet-id` > original template subnet IDs
- **Consistency**: Ensures all instance groups use the same network configuration

### Tag Management

- **Automatic generation**: Includes timestamp, source cluster, and dhname tags
- **CLI integration**: Merges tags from CLI command files
- **Consistency**: dhname tag always matches the final cluster name

## API Reference

### DistroXRequestTemplateGenerator Class

#### Core Methods

- `get_cluster_data_from_cli(cluster_name)`: Fetches cluster data using CDP CLI
- `get_cluster_data_from_file(file_path)`: Reads cluster data from JSON file
- `generate_request_template(...)`: Main template generation with comprehensive override support
- `save_template(template, output_dir, cluster_name, source_type)`: Saves template to file

#### Extraction Methods

- `extract_instance_group_details(group, skip_cli_overrides=False)`: Processes instance group information
- `extract_image_details(image_data)`: Extracts image configuration with id and catalog
- `extract_network_details(cluster_data)`: Processes network configuration with subnetId and networkId
- `extract_cluster_details(cluster_data)`: Extracts cluster configuration with blueprintName

#### Parsing Methods

- `parse_instance_groups_argument(instance_groups_arg)`: Parses complex instance group override strings
- `parse_cli_command_file(cli_file_path)`: Parses CLI command files for additional configuration
- `merge_instance_group_override(template_group, override_group)`: Merges instance group overrides

#### Utility Methods

- `_get_bucket_name_from_datalake_crn(datalake_crn)`: Extracts S3 bucket name from datalake CRN
- `_build_tags(cluster_info, cluster_name)`: Builds comprehensive tag structure
- `_get_load_balancer_setting(cluster_info)`: Gets load balancer configuration
- `_get_multi_az_setting(cluster_info)`: Gets multi-AZ configuration
