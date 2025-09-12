# RDS Performance and Cost Analysis Script

This script provides comprehensive analysis of RDS instance performance using AWS CloudWatch metrics and cost data using AWS Cost Explorer API.

## Features

- **Performance Analysis**: Collects and analyzes key RDS metrics including:

  - CPU Utilization
  - Database Connections
  - Freeable Memory
  - Free Storage Space
  - Read/Write IOPS
  - Read/Write Latency

- **Cost Analysis**:

  - Multiple cost metrics: Blended, Unblended, Amortized, Net Unblended, Net Amortized
  - Total cost for the specified period (filtered by Cloudera-Resource-Name tag)
  - **Detailed Daily Breakdown**: Day-by-day cost analysis with all metrics
  - **Monthly Breakdown**: Aggregated monthly cost analysis
  - **Usage Type Breakdown**: Detailed breakdown by RDS usage types
  - **Cost Trend Analysis**: Trend analysis and projections
  - **CSV Export**: Export detailed breakdowns to CSV files
  - Cost optimization recommendations
  - Automatic filtering by Service (RDS), Region, and Cloudera-Resource-Name tag
  - Fallback mechanisms for data availability issues

- **Comprehensive Reporting**:
  - Instance configuration details
  - Performance metrics with statistics (avg, max, min)
  - Performance insights and alerts
  - Cost analysis and recommendations
  - Formatted tables for easy reading

## Prerequisites

- Python 3.7+
- AWS CLI configured with appropriate permissions
- Required AWS permissions:
  - CloudWatch: `cloudwatch:GetMetricStatistics`
  - Cost Explorer: `ce:GetCostAndUsage`
  - RDS: `rds:DescribeDBInstances`
- RDS instance must have `Cloudera-Resource-Name` tag for accurate cost filtering

## Installation

1. Install required dependencies:

```bash
pip install -r requirements.txt
```

2. Ensure AWS credentials are configured:

```bash
aws configure
```

## Cost Filtering

The script automatically applies the following filters to ensure accurate cost analysis:

1. **Service Filter**: `Amazon Relational Database Service` - Only RDS costs
2. **Region Filter**: Matches the specified AWS region
3. **Tag Filter**: `Cloudera-Resource-Name` - Only costs for the specific Cloudera resource

This ensures that the cost analysis is specific to your RDS instance and not mixed with other RDS instances or AWS services.

### Cost Metrics Explained

The script provides multiple cost views to give you a complete picture:

- **Unblended Costs**: Your actual costs on the day they were charged (cash basis)
- **Blended Costs**: Average costs across your consolidated billing family
- **Amortized Costs**: Costs spread across the billing period (accrual basis)
- **Net Unblended Costs**: Unblended costs after all discounts are applied
- **Net Amortized Costs**: Amortized costs after all discounts are applied

### Data Availability

- Cost Explorer API may have up to 24-48 hour delay for recent data
- The script includes fallback mechanisms to find data in broader date ranges
- If no data is found with tag filtering, it falls back to general RDS filtering

## Usage

### Basic Usage

```bash
python get_cost_usage.py --db-instance-id mydbinstance --start-time 2024-01-01 --end-time 2024-01-31 --region us-east-1
```

### Advanced Usage

```bash
python get_cost_usage.py \
  --db-instance-id mydbinstance \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-31T23:59:59Z \
  --region us-west-2 \
  --output-file rds_analysis_report.txt
```

### CSV Export Usage

```bash
# Export detailed breakdowns to CSV files with timestamped folder
python get_cost_usage.py \
  --db-instance-id mydbinstance \
  --start-time 2024-01-01 \
  --end-time 2024-01-31 \
  --region us-east-1 \
  --export-csv ./rds_cost_analysis
```

This will create a timestamped folder (e.g., `rds_cost_analysis_20250115_143022/`) containing three CSV files:

- `rds_cost_breakdown_daily_breakdown.csv` - Daily cost breakdown
- `rds_cost_breakdown_monthly_breakdown.csv` - Monthly cost breakdown
- `rds_cost_breakdown_usage_breakdown.csv` - Usage type breakdown

**Note**: The folder name will automatically have a timestamp appended (format: `YYYYMMDD_HHMMSS`)

**Examples of folder names:**

- `--export-csv ./rds_analysis` → `rds_analysis_20250115_143022/`
- `--export-csv /tmp/cost_data` → `/tmp/cost_data_20250115_143022/`
- `--export-csv ~/reports/rds_costs` → `~/reports/rds_costs_20250115_143022/`

### Arguments

- `--db-instance-id` (required): RDS DB Instance Identifier
- `--start-time` (required): Start time in format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ
- `--end-time` (required): End time in format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ
- `--region` (required): AWS region
- `--output-file` (optional): File to save the report
- `--export-csv` (optional): Export detailed cost breakdowns to CSV files. Specify folder name (timestamp will be added automatically)

### Date Format Examples

- `2024-01-01` (automatically becomes `2024-01-01T00:00:00Z`)
- `2024-01-01T00:00:00Z`
- `2024-01-31T23:59:59Z`

## Sample Output

The script generates a comprehensive report including:

```
================================================================================
RDS INSTANCE ANALYSIS REPORT
Instance ID: mydbinstance
Generated: 2024-01-15 10:30:00
================================================================================

📋 INSTANCE CONFIGURATION
----------------------------------------
+------------------+------------------+
| Property         | Value            |
+==================+==================+
| Instance Class   | db.t3.medium     |
| Engine           | mysql            |
| Engine Version   | 8.0.35           |
| Allocated Storage| 100 GB           |
| Storage Type     | gp2              |
| Multi-AZ         | False            |
| Status           | available        |
| Availability Zone| us-east-1a       |
+------------------+------------------+

📊 PERFORMANCE METRICS
----------------------------------------
+-------------------+----------+----------+----------+-------------+
| Metric            | Average  | Maximum  | Minimum  | Data Points |
+===================+==========+==========+==========+=============+
| CPUUtilization    | 45.23    | 78.50    | 12.10    | 744         |
| DatabaseConnections| 25.67   | 45.00    | 8.00     | 744         |
| FreeableMemory    | 1.2GB    | 1.5GB    | 0.8GB    | 744         |
+-------------------+----------+----------+----------+-------------+

🔍 PERFORMANCE INSIGHTS
----------------------------------------
✅ CPU UTILIZATION: Within normal range
🔗 CONNECTIONS: Average 26, Peak 45
💾 FREE MEMORY: Average 1.20 GB

💰 COST ANALYSIS
----------------------------------------
Total Cost: $156.78
Daily Average: $5.06

Cost Breakdown by Usage Type:
+----------------------+--------+
| Usage Type           | Cost   |
+======================+========+
| RDS Instance         | $120.50|
| Storage              | $25.30 |
| Backup Storage       | $10.98 |
+----------------------+--------+

💡 RECOMMENDATIONS
----------------------------------------
• Consider Reserved Instances for cost optimization if usage is predictable
```

## Error Handling

The script includes comprehensive error handling for:

- Invalid date formats
- AWS API errors
- Missing permissions
- Network connectivity issues
- Invalid instance identifiers

## Dependencies

- `boto3`: AWS SDK for Python
- `pandas`: Data manipulation and analysis
- `tabulate`: Pretty-print tabular data
