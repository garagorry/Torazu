# Datahub Request Template Generator

This Python script generates Datahub request templates from either running CDP DataHub clusters or JSON files containing cluster description data. The generated templates follow the `DistroXV1Request` structure and can be used to recreate or modify DataHub clusters.

## Features

- **Dual Input Sources**: Generate templates from running clusters or JSON files
- **Dynamic Template Generation**: Creates templates based on actual cluster configurations
- **Timestamped Output**: Automatically adds timestamps to output directories and files
- **Flexible Output Location**: Customizable output directory with intelligent defaults
- **Comprehensive Logging**: Detailed logging for debugging and monitoring
- **Error Handling**: Robust error handling with informative error messages
- **CLI Command Parsing (Optional)**: Parse a DataHub create CLI command exported from the UI to enrich templates with tags, subnet, multi-AZ, load balancer, database, and instance group overrides

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
  --cli-command-file /tmp/dhcore-de-cli.txt
```

## Examples

### Generate Template from Running Cluster

```bash
# Basic usage
python generate_request_template.py --cluster-name jdga-dm-01

# With custom output directory
python generate_request_template.py --cluster-name jdga-dm-01 --output ./my-templates

# With custom environment name
python generate_request_template.py --cluster-name jdga-dm-01 --environment-name my-env
```

### Generate Template from JSON File

```bash
# From a JSON file
python generate_request_template.py --input-file cluster_data.json

# With custom output directory
python generate_request_template.py --input-file cluster_data.json --output ./templates
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

The generated template follows the `DistroXV1Request` structure:

```json
{
  "environmentName": "environment-name",
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
          "encryption": {"type": "DEFAULT", "key": null},
          "placementGroup": {"strategy": "PARTITION"},
          "instanceType": "instance-type",
          "rootVolume": {"size": 100},
          "attachedVolumes": [...],
          "cloudPlatform": "AWS"
        }
      },
      "recipeNames": [],
      "subnetIds": [],
      "availabilityZones": []
    }
  ],
  "image": {...},
  "network": {...},
  "cluster": {...},
  "tags": {...},
  "security": {...}
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

## API Reference

### DistroXRequestTemplateGenerator Class

#### Methods

- `get_cluster_data_from_cli(cluster_name)`: Fetches cluster data using CDP CLI
- `get_cluster_data_from_file(file_path)`: Reads cluster data from JSON file
- `extract_instance_group_details(group)`: Processes instance group information
- `extract_image_details(image_data)`: Extracts image configuration
- `extract_network_details(cluster_data)`: Processes network configuration
- `extract_cluster_details(cluster_data)`: Extracts cluster configuration
- `generate_request_template(cluster_data, cluster_name, environment_name)`: Main template generation
- `save_template(template, output_dir, cluster_name, source_type)`: Saves template to file
