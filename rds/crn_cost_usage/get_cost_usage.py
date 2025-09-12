#!/usr/bin/env python3
"""
RDS Performance and Cost Analysis Script

This script provides comprehensive analysis of Cloudera RDS instances including:
- Performance metrics collection from CloudWatch
- Cost analysis using Cost Explorer API with Cloudera-Resource-Name filtering
- Detailed daily and monthly cost breakdowns
- CSV export functionality with timestamped folders
- Multiple cost metrics (Blended, Unblended, Amortized, Net costs)

Usage:
    python get_cost_usage.py --db-instance-id mydbinstance --start-time 2024-01-01 --end-time 2024-01-31 --region us-east-1

Features:
    - CloudWatch metrics: CPU, Memory, Connections, IOPS, Latency
    - Cost filtering by Service (RDS), Region, and Cloudera-Resource-Name tag
    - Multiple cost views: Unblended, Blended, Amortized, Net costs
    - Detailed breakdowns: Daily, Monthly, Usage Type
    - CSV export with automatic timestamping
    - Trend analysis and cost projections
"""

import argparse
import boto3
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from tabulate import tabulate


class RDSAnalyzer:
    """
    RDS Performance and Cost Analyzer.
    
    This class provides comprehensive analysis of RDS instances including
    performance metrics from CloudWatch and cost data from Cost Explorer API.
    It supports filtering by Cloudera-Resource-Name tags and provides detailed
    daily and monthly cost breakdowns.
    """
    
    def __init__(self, region: str):
        """
        Initialize the analyzer with AWS clients.
        
        Args:
            region (str): AWS region for the analysis
        """
        self.region = region
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
        self.cost_explorer = boto3.client('ce', region_name=region)
        self.rds = boto3.client('rds', region_name=region)
        
    def get_rds_metrics(self, db_instance_id: str, start_time: str, end_time: str) -> Dict:
        """
        Collect RDS performance metrics from CloudWatch.
        
        Args:
            db_instance_id (str): RDS DB Instance Identifier
            start_time (str): Start time in ISO format
            end_time (str): End time in ISO format
            
        Returns:
            Dict: Dictionary containing metrics data with datapoints for each metric
        """
        print(f"Collecting performance metrics for {db_instance_id}...")
        
        # Parse datetime strings
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        metrics = {}
        
        # Define key RDS metrics to collect
        metric_queries = {
            'CPUUtilization': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'CPUUtilization',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            },
            'DatabaseConnections': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'DatabaseConnections',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            },
            'FreeableMemory': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'FreeableMemory',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            },
            'FreeStorageSpace': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'FreeStorageSpace',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            },
            'ReadIOPS': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'ReadIOPS',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            },
            'WriteIOPS': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'WriteIOPS',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            },
            'ReadLatency': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'ReadLatency',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            },
            'WriteLatency': {
                'Namespace': 'AWS/RDS',
                'MetricName': 'WriteLatency',
                'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}]
            }
        }
        
        for metric_name, metric_config in metric_queries.items():
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace=metric_config['Namespace'],
                    MetricName=metric_config['MetricName'],
                    Dimensions=metric_config['Dimensions'],
                    StartTime=start_dt,
                    EndTime=end_dt,
                    Period=3600,  # 1 hour periods
                    Statistics=['Average', 'Maximum', 'Minimum']
                )
                
                metrics[metric_name] = response['Datapoints']
                print(f"  ✓ Collected {metric_name}: {len(response['Datapoints'])} data points")
                
            except Exception as e:
                print(f"  ✗ Failed to collect {metric_name}: {str(e)}")
                metrics[metric_name] = []
        
        return metrics
    
    def get_cost_data(self, db_instance_id: str, start_time: str, end_time: str, cloudera_resource_name: str = None) -> Dict:
        """
        Collect cost data using Cost Explorer API with proper filters.
        
        Args:
            db_instance_id (str): RDS DB Instance Identifier
            start_time (str): Start time in ISO format
            end_time (str): End time in ISO format
            cloudera_resource_name (str, optional): Cloudera-Resource-Name tag value for filtering
            
        Returns:
            Dict: Dictionary containing general costs and detailed RDS costs
        """
        print(f"Collecting cost data for {db_instance_id}...")
        
        # Parse datetime strings and convert to yyyy-MM-dd format for Cost Explorer
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # Cost Explorer expects yyyy-MM-dd format
        start_date = start_dt.strftime('%Y-%m-%d')
        end_date = end_dt.strftime('%Y-%m-%d')
        
        # Build filter for Cost Explorer
        cost_filter = {
            'And': [
                {
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Relational Database Service']
                    }
                },
                {
                    'Dimensions': {
                        'Key': 'REGION',
                        'Values': [self.region]
                    }
                }
            ]
        }
        
        # Add tag filter if Cloudera-Resource-Name is available
        if cloudera_resource_name:
            cost_filter['And'].append({
                'Tags': {
                    'Key': 'Cloudera-Resource-Name',
                    'Values': [cloudera_resource_name]
                }
            })
            print(f"  Using Cloudera-Resource-Name filter: {cloudera_resource_name}")
        else:
            print(f"  Warning: No Cloudera-Resource-Name tag found, using general RDS filter")
        
        try:
            # Get cost and usage data with all available metrics
            response = self.cost_explorer.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date,
                    'End': end_date
                },
                Granularity='DAILY',
                Metrics=['BlendedCost', 'UnblendedCost', 'AmortizedCost', 'NetUnblendedCost', 'NetAmortizedCost', 'UsageQuantity'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ],
                Filter=cost_filter
            )
            
            # Get detailed RDS costs with same filters
            rds_response = self.cost_explorer.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date,
                    'End': end_date
                },
                Granularity='DAILY',
                Metrics=['BlendedCost', 'UnblendedCost', 'AmortizedCost', 'NetUnblendedCost', 'NetAmortizedCost'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'USAGE_TYPE'
                    }
                ],
                Filter=cost_filter
            )
            
            # If no data with tag filter, try without tag filter as fallback
            if not response['ResultsByTime'] and cloudera_resource_name:
                print(f"  No data found with tag filter, trying without tag filter...")
                # Fallback filter without tag filtering
                fallback_filter = {
                    'And': [
                        {
                            'Dimensions': {
                                'Key': 'SERVICE',
                                'Values': ['Amazon Relational Database Service']
                            }
                        },
                        {
                            'Dimensions': {
                                'Key': 'REGION',
                                'Values': [self.region]
                            }
                        }
                    ]
                }
                
                response = self.cost_explorer.get_cost_and_usage(
                    TimePeriod={
                        'Start': start_date,
                        'End': end_date
                    },
                    Granularity='DAILY',
                    Metrics=['BlendedCost', 'UnblendedCost', 'AmortizedCost', 'NetUnblendedCost', 'NetAmortizedCost', 'UsageQuantity'],
                    GroupBy=[
                        {
                            'Type': 'DIMENSION',
                            'Key': 'SERVICE'
                        }
                    ],
                    Filter=fallback_filter
                )
                
                rds_response = self.cost_explorer.get_cost_and_usage(
                    TimePeriod={
                        'Start': start_date,
                        'End': end_date
                    },
                    Granularity='DAILY',
                    Metrics=['BlendedCost', 'UnblendedCost', 'AmortizedCost', 'NetUnblendedCost', 'NetAmortizedCost'],
                    GroupBy=[
                        {
                            'Type': 'DIMENSION',
                            'Key': 'USAGE_TYPE'
                        }
                    ],
                    Filter=fallback_filter
                )
            
            # If still no data, try a broader date range (last 30 days)
            if not response['ResultsByTime']:
                print(f"  No data found for specified period, trying last 30 days...")
                from datetime import timedelta
                broader_start = (start_dt - timedelta(days=30)).strftime('%Y-%m-%d')
                broader_end = end_dt.strftime('%Y-%m-%d')
                
                # Try with broader date range and no filters first
                response = self.cost_explorer.get_cost_and_usage(
                    TimePeriod={
                        'Start': broader_start,
                        'End': broader_end
                    },
                    Granularity='DAILY',
                    Metrics=['BlendedCost', 'UnblendedCost', 'AmortizedCost', 'NetUnblendedCost', 'NetAmortizedCost', 'UsageQuantity'],
                    GroupBy=[
                        {
                            'Type': 'DIMENSION',
                            'Key': 'SERVICE'
                        }
                    ]
                )
                
                if response['ResultsByTime']:
                    print(f"  Found data in broader date range: {broader_start} to {broader_end}")
                    # Filter for RDS only in the results
                    rds_results = []
                    for result in response['ResultsByTime']:
                        for group in result.get('Groups', []):
                            if group['Keys'][0] == 'Amazon Relational Database Service':
                                rds_results.append(result)
                                break
                    response['ResultsByTime'] = rds_results
            
            print(f"  ✓ Collected cost data: {len(response['ResultsByTime'])} days")
            
            # Debug: Print sample response structure
            if response['ResultsByTime']:
                sample_result = response['ResultsByTime'][0]
                print(f"  Debug: Sample cost result structure: {list(sample_result.keys())}")
                if 'Total' in sample_result:
                    print(f"  Debug: Total metrics available: {list(sample_result['Total'].keys())}")
            
            return {
                'general_costs': response['ResultsByTime'],
                'rds_detailed_costs': rds_response['ResultsByTime']
            }
            
        except Exception as e:
            print(f"  ✗ Failed to collect cost data: {str(e)}")
            return {'general_costs': [], 'rds_detailed_costs': []}
    
    def get_rds_instance_info(self, db_instance_id: str) -> Dict:
        """
        Get RDS instance configuration information.
        
        Args:
            db_instance_id (str): RDS DB Instance Identifier
            
        Returns:
            Dict: Dictionary containing instance configuration details including Cloudera-Resource-Name tag
        """
        try:
            response = self.rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)
            instance = response['DBInstances'][0]
            
            # Extract Cloudera-Resource-Name tag
            cloudera_resource_name = None
            if 'TagList' in instance:
                for tag in instance['TagList']:
                    if tag['Key'] == 'Cloudera-Resource-Name':
                        cloudera_resource_name = tag['Value']
                        break
            
            return {
                'DBInstanceClass': instance['DBInstanceClass'],
                'Engine': instance['Engine'],
                'EngineVersion': instance['EngineVersion'],
                'AllocatedStorage': instance['AllocatedStorage'],
                'StorageType': instance['StorageType'],
                'MultiAZ': instance['MultiAZ'],
                'PubliclyAccessible': instance['PubliclyAccessible'],
                'DBInstanceStatus': instance['DBInstanceStatus'],
                'MasterUsername': instance['MasterUsername'],
                'DBName': instance.get('DBName', 'N/A'),
                'VpcSecurityGroups': [sg['VpcSecurityGroupId'] for sg in instance['VpcSecurityGroups']],
                'DBSubnetGroup': instance['DBSubnetGroup']['DBSubnetGroupName'],
                'AvailabilityZone': instance['AvailabilityZone'],
                'BackupRetentionPeriod': instance['BackupRetentionPeriod'],
                'PreferredBackupWindow': instance['PreferredBackupWindow'],
                'PreferredMaintenanceWindow': instance['PreferredMaintenanceWindow'],
                'ClouderaResourceName': cloudera_resource_name
            }
        except Exception as e:
            print(f"  ✗ Failed to get RDS instance info: {str(e)}")
            return {}
    
    def analyze_performance(self, metrics: Dict) -> Dict:
        """
        Analyze performance metrics and generate insights.
        
        Args:
            metrics (Dict): Dictionary containing CloudWatch metrics data
            
        Returns:
            Dict: Dictionary containing analyzed performance metrics with statistics
        """
        print("Analyzing performance metrics...")
        
        analysis = {}
        
        for metric_name, datapoints in metrics.items():
            if not datapoints:
                continue
                
            values = [dp['Average'] for dp in datapoints if 'Average' in dp]
            max_values = [dp['Maximum'] for dp in datapoints if 'Maximum' in dp]
            min_values = [dp['Minimum'] for dp in datapoints if 'Minimum' in dp]
            
            if values:
                analysis[metric_name] = {
                    'avg': sum(values) / len(values),
                    'max': max(values),
                    'min': min(values),
                    'data_points': len(values)
                }
        
        return analysis
    
    def analyze_costs(self, cost_data: Dict) -> Dict:
        """
        Analyze cost data and generate insights.
        
        Args:
            cost_data (Dict): Dictionary containing cost data from Cost Explorer API
            
        Returns:
            Dict: Dictionary containing analyzed cost data with multiple cost metrics
        """
        print("Analyzing cost data...")
        
        analysis = {
            'total_costs': {
                'blended': 0,
                'unblended': 0,
                'amortized': 0,
                'net_unblended': 0,
                'net_amortized': 0
            },
            'daily_costs': [],
            'cost_breakdown': {},
            'available_metrics': []
        }
        
        # Analyze general RDS costs
        for result in cost_data.get('general_costs', []):
            try:
                # Initialize daily cost structure for all metric types
                daily_costs = {
                    'date': result['TimePeriod']['Start'],
                    'blended': 0,
                    'unblended': 0,
                    'amortized': 0,
                    'net_unblended': 0,
                    'net_amortized': 0
                }
                
                # Check all available cost metrics
                for metric in ['BlendedCost', 'UnblendedCost', 'AmortizedCost', 'NetUnblendedCost', 'NetAmortizedCost']:
                    if metric in result['Total']:
                        cost = float(result['Total'][metric]['Amount'])
                        daily_costs[metric.lower().replace('cost', '')] = cost
                        analysis['total_costs'][metric.lower().replace('cost', '')] += cost
                        if metric not in analysis['available_metrics']:
                            analysis['available_metrics'].append(metric)
                
                # Use the first available cost metric for backward compatibility
                if not analysis['available_metrics']:
                    print(f"  Warning: No cost data found for {result['TimePeriod']['Start']}")
                    continue
                
                # Add to daily costs
                analysis['daily_costs'].append(daily_costs)
                
            except (KeyError, ValueError, TypeError) as e:
                print(f"  Warning: Error processing cost data for {result.get('TimePeriod', {}).get('Start', 'unknown')}: {e}")
                continue
        
        # Analyze detailed RDS costs
        for result in cost_data.get('rds_detailed_costs', []):
            try:
                for group in result.get('Groups', []):
                    usage_type = group['Keys'][0]
                    
                    # Check all available cost metrics for detailed breakdown
                    for metric in ['BlendedCost', 'UnblendedCost', 'AmortizedCost', 'NetUnblendedCost', 'NetAmortizedCost']:
                        if metric in group['Metrics']:
                            cost = float(group['Metrics'][metric]['Amount'])
                            metric_key = f"{usage_type}_{metric.lower().replace('cost', '')}"
                            if metric_key not in analysis['cost_breakdown']:
                                analysis['cost_breakdown'][metric_key] = 0
                            analysis['cost_breakdown'][metric_key] += cost
                            
            except (KeyError, ValueError, TypeError) as e:
                print(f"  Warning: Error processing detailed cost data: {e}")
                continue
        
        return analysis
    
    def calculate_monthly_breakdown(self, daily_costs: List[Dict], available_metrics: List[str]) -> Dict:
        """
        Calculate monthly breakdown from daily costs.
        
        Args:
            daily_costs (List[Dict]): List of daily cost data
            available_metrics (List[str]): List of available cost metrics
            
        Returns:
            Dict: Dictionary containing monthly aggregated cost data
        """
        monthly_data = {}
        
        for daily_cost in daily_costs:
            # Extract year-month from date (format: YYYY-MM-DD)
            date_str = daily_cost['date']
            year_month = date_str[:7]  # YYYY-MM
            
            if year_month not in monthly_data:
                monthly_data[year_month] = {
                    'days': 0,
                    'blended': 0,
                    'unblended': 0,
                    'amortized': 0,
                    'net_unblended': 0,
                    'net_amortized': 0
                }
            
            monthly_data[year_month]['days'] += 1
            
            # Sum up all cost metrics for this month
            for metric in available_metrics:
                metric_key = metric.lower().replace('cost', '')
                cost_value = daily_cost.get(metric_key, 0)
                monthly_data[year_month][metric_key] += cost_value
        
        return monthly_data
    
    def export_cost_breakdowns_to_csv(self, cost_analysis: Dict, folder_name: str):
        """
        Export detailed cost breakdowns to CSV files with timestamped folder.
        
        Args:
            cost_analysis (Dict): Dictionary containing analyzed cost data
            folder_name (str): Base folder name (timestamp will be added automatically)
        """
        import csv
        import os
        from datetime import datetime
        
        print(f"\n📊 Exporting detailed cost breakdowns to CSV files...")
        
        # Add timestamp to folder name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if folder_name.endswith('/'):
            folder_name = folder_name[:-1]  # Remove trailing slash
        
        timestamped_folder = f"{folder_name}_{timestamp}"
        
        # Create output directory if it doesn't exist
        os.makedirs(timestamped_folder, exist_ok=True)
        print(f"  📁 Creating timestamped folder: {timestamped_folder}")
        
        # Base filename for CSV files
        base_filename = os.path.join(timestamped_folder, "rds_cost_breakdown")
        
        # 1. Daily breakdown CSV - Export day-by-day cost data
        if cost_analysis['daily_costs']:
            daily_filename = f"{base_filename}_daily_breakdown.csv"
            with open(daily_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                headers = ['Date'] + [metric for metric in cost_analysis['available_metrics']]
                writer.writerow(headers)
                
                # Write daily data
                for daily_cost in cost_analysis['daily_costs']:
                    row = [daily_cost['date']]
                    for metric in cost_analysis['available_metrics']:
                        metric_key = metric.lower().replace('cost', '')
                        cost_value = daily_cost.get(metric_key, 0)
                        row.append(f"{cost_value:.2f}")
                    writer.writerow(row)
            
            print(f"  ✓ Daily breakdown exported to: {daily_filename}")
        
        # 2. Monthly breakdown CSV - Export monthly aggregated cost data
        if cost_analysis['daily_costs']:
            monthly_breakdown = self.calculate_monthly_breakdown(cost_analysis['daily_costs'], cost_analysis['available_metrics'])
            if monthly_breakdown:
                monthly_filename = f"{base_filename}_monthly_breakdown.csv"
                with open(monthly_filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    # Write header
                    headers = ['Month'] + [metric for metric in cost_analysis['available_metrics']] + ['Days']
                    writer.writerow(headers)
                    
                    # Write monthly data
                    for month, data in monthly_breakdown.items():
                        row = [month]
                        for metric in cost_analysis['available_metrics']:
                            metric_key = metric.lower().replace('cost', '')
                            cost_value = data.get(metric_key, 0)
                            row.append(f"{cost_value:.2f}")
                        row.append(str(data['days']))
                        writer.writerow(row)
                
                print(f"  ✓ Monthly breakdown exported to: {monthly_filename}")
        
        # 3. Usage type breakdown CSV - Export cost breakdown by RDS usage types
        if cost_analysis['cost_breakdown']:
            usage_filename = f"{base_filename}_usage_breakdown.csv"
            with open(usage_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Group by usage type
                usage_breakdown = {}
                for key, cost in cost_analysis['cost_breakdown'].items():
                    if '_' in key:
                        usage_type, cost_type = key.split('_', 1)
                        if usage_type not in usage_breakdown:
                            usage_breakdown[usage_type] = {}
                        usage_breakdown[usage_type][cost_type] = cost
                    else:
                        if "Other" not in usage_breakdown:
                            usage_breakdown["Other"] = {}
                        usage_breakdown["Other"][key] = cost
                
                # Write header
                headers = ['Usage Type'] + [metric for metric in cost_analysis['available_metrics']] + ['Total']
                writer.writerow(headers)
                
                # Write usage data
                for usage_type, costs in usage_breakdown.items():
                    row = [usage_type]
                    total_for_type = 0
                    
                    for metric in cost_analysis['available_metrics']:
                        metric_key = metric.lower().replace('cost', '')
                        cost_value = costs.get(metric_key, 0)
                        row.append(f"{cost_value:.2f}")
                        total_for_type += cost_value
                    
                    row.append(f"{total_for_type:.2f}")
                    writer.writerow(row)
            
            print(f"  ✓ Usage type breakdown exported to: {usage_filename}")
        
        print(f"  📁 All CSV files exported to: {timestamped_folder}")
    
    def generate_report(self, db_instance_id: str, instance_info: Dict, 
                       performance_analysis: Dict, cost_analysis: Dict) -> str:
        """
        Generate comprehensive performance and cost report.
        
        Args:
            db_instance_id (str): RDS DB Instance Identifier
            instance_info (Dict): Dictionary containing instance configuration
            performance_analysis (Dict): Dictionary containing performance analysis
            cost_analysis (Dict): Dictionary containing cost analysis
            
        Returns:
            str: Formatted report string
        """
        
        report = []
        report.append("=" * 80)
        report.append(f"RDS INSTANCE ANALYSIS REPORT")
        report.append(f"Instance ID: {db_instance_id}")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 80)
        
        # Instance Information
        report.append("\n📋 INSTANCE CONFIGURATION")
        report.append("-" * 40)
        if instance_info:
            info_table = [
                ["Property", "Value"],
                ["Instance Class", instance_info.get('DBInstanceClass', 'N/A')],
                ["Engine", instance_info.get('Engine', 'N/A')],
                ["Engine Version", instance_info.get('EngineVersion', 'N/A')],
                ["Allocated Storage", f"{instance_info.get('AllocatedStorage', 'N/A')} GB"],
                ["Storage Type", instance_info.get('StorageType', 'N/A')],
                ["Multi-AZ", instance_info.get('MultiAZ', 'N/A')],
                ["Status", instance_info.get('DBInstanceStatus', 'N/A')],
                ["Availability Zone", instance_info.get('AvailabilityZone', 'N/A')],
                ["Cloudera Resource Name", instance_info.get('ClouderaResourceName', 'N/A')]
            ]
            report.append(tabulate(info_table, headers="firstrow", tablefmt="grid"))
        
        # Performance Analysis
        report.append("\n📊 PERFORMANCE METRICS")
        report.append("-" * 40)
        
        if performance_analysis:
            perf_table = [["Metric", "Average", "Maximum", "Minimum", "Data Points"]]
            
            for metric, stats in performance_analysis.items():
                perf_table.append([
                    metric,
                    f"{stats['avg']:.2f}",
                    f"{stats['max']:.2f}",
                    f"{stats['min']:.2f}",
                    stats['data_points']
                ])
            
            report.append(tabulate(perf_table, headers="firstrow", tablefmt="grid"))
            
            # Performance insights
            report.append("\n🔍 PERFORMANCE INSIGHTS")
            report.append("-" * 40)
            
            if 'CPUUtilization' in performance_analysis:
                cpu_avg = performance_analysis['CPUUtilization']['avg']
                if cpu_avg > 80:
                    report.append("⚠️  HIGH CPU UTILIZATION: Average CPU usage is above 80%")
                elif cpu_avg > 60:
                    report.append("⚡ MODERATE CPU UTILIZATION: Average CPU usage is above 60%")
                else:
                    report.append("✅ CPU UTILIZATION: Within normal range")
            
            if 'DatabaseConnections' in performance_analysis:
                conn_avg = performance_analysis['DatabaseConnections']['avg']
                conn_max = performance_analysis['DatabaseConnections']['max']
                report.append(f"🔗 CONNECTIONS: Average {conn_avg:.0f}, Peak {conn_max:.0f}")
            
            if 'FreeableMemory' in performance_analysis:
                mem_avg = performance_analysis['FreeableMemory']['avg']
                mem_gb = mem_avg / (1024**3)
                report.append(f"💾 FREE MEMORY: Average {mem_gb:.2f} GB")
        
        # Cost Analysis
        report.append("\n💰 COST ANALYSIS")
        report.append("-" * 40)
        
        # Show filter information
        if instance_info.get('ClouderaResourceName'):
            report.append(f"Filtered by Cloudera-Resource-Name: {instance_info.get('ClouderaResourceName')}")
        else:
            report.append("Warning: No Cloudera-Resource-Name tag found - using general RDS costs")
        
        # Display all available cost metrics
        if cost_analysis['available_metrics']:
            report.append(f"\nAvailable Cost Metrics: {', '.join(cost_analysis['available_metrics'])}")
            
            # Show total costs for each metric
            cost_table = [["Cost Type", "Total Cost", "Description"]]
            cost_descriptions = {
                'BlendedCost': 'Average cost across consolidated billing family',
                'UnblendedCost': 'Actual cost on the day it was charged (cash basis)',
                'AmortizedCost': 'Cost spread across billing period (accrual basis)',
                'NetUnblendedCost': 'Unblended cost after discounts',
                'NetAmortizedCost': 'Amortized cost after discounts'
            }
            
            for metric in cost_analysis['available_metrics']:
                metric_key = metric.lower().replace('cost', '')
                total_cost = cost_analysis['total_costs'].get(metric_key, 0)
                description = cost_descriptions.get(metric, 'Cost metric')
                cost_table.append([metric, f"${total_cost:.2f}", description])
            
            report.append(tabulate(cost_table, headers="firstrow", tablefmt="grid"))
            
            # Show daily averages
            if cost_analysis['daily_costs']:
                report.append(f"\nDaily Averages:")
                for metric in cost_analysis['available_metrics']:
                    metric_key = metric.lower().replace('cost', '')
                    total_cost = cost_analysis['total_costs'].get(metric_key, 0)
                    daily_avg = total_cost / len(cost_analysis['daily_costs'])
                    report.append(f"  {metric}: ${daily_avg:.2f}/day")
                
                # Detailed Daily Breakdown
                report.append(f"\n📅 DETAILED DAILY BREAKDOWN")
                report.append("-" * 50)
                
                # Create daily breakdown table
                daily_headers = ["Date"] + [metric for metric in cost_analysis['available_metrics']]
                daily_table = [daily_headers]
                
                for daily_cost in cost_analysis['daily_costs']:
                    row = [daily_cost['date']]
                    for metric in cost_analysis['available_metrics']:
                        metric_key = metric.lower().replace('cost', '')
                        cost_value = daily_cost.get(metric_key, 0)
                        row.append(f"${cost_value:.2f}")
                    daily_table.append(row)
                
                report.append(tabulate(daily_table, headers="firstrow", tablefmt="grid"))
                
                # Monthly breakdown
                monthly_breakdown = self.calculate_monthly_breakdown(cost_analysis['daily_costs'], cost_analysis['available_metrics'])
                if monthly_breakdown:
                    report.append(f"\n📊 MONTHLY BREAKDOWN")
                    report.append("-" * 50)
                    
                    monthly_headers = ["Month"] + [metric for metric in cost_analysis['available_metrics']] + ["Days"]
                    monthly_table = [monthly_headers]
                    
                    for month, data in monthly_breakdown.items():
                        row = [month]
                        for metric in cost_analysis['available_metrics']:
                            metric_key = metric.lower().replace('cost', '')
                            cost_value = data.get(metric_key, 0)
                            row.append(f"${cost_value:.2f}")
                        row.append(str(data['days']))
                        monthly_table.append(row)
                    
                    report.append(tabulate(monthly_table, headers="firstrow", tablefmt="grid"))
            
            # Show detailed cost breakdown
            if cost_analysis['cost_breakdown']:
                report.append("\n🔍 DETAILED COST BREAKDOWN BY USAGE TYPE")
                report.append("-" * 60)
                
                # Group by usage type
                usage_breakdown = {}
                for key, cost in cost_analysis['cost_breakdown'].items():
                    if '_' in key:
                        usage_type, cost_type = key.split('_', 1)
                        if usage_type not in usage_breakdown:
                            usage_breakdown[usage_type] = {}
                        usage_breakdown[usage_type][cost_type] = cost
                    else:
                        if "Other" not in usage_breakdown:
                            usage_breakdown["Other"] = {}
                        usage_breakdown["Other"][key] = cost
                
                # Create detailed breakdown table
                breakdown_headers = ["Usage Type"] + [metric for metric in cost_analysis['available_metrics']] + ["Total"]
                breakdown_table = [breakdown_headers]
                
                for usage_type, costs in usage_breakdown.items():
                    row = [usage_type]
                    total_for_type = 0
                    
                    for metric in cost_analysis['available_metrics']:
                        metric_key = metric.lower().replace('cost', '')
                        cost_value = costs.get(metric_key, 0)
                        row.append(f"${cost_value:.2f}")
                        total_for_type += cost_value
                    
                    row.append(f"${total_for_type:.2f}")
                    breakdown_table.append(row)
                
                report.append(tabulate(breakdown_table, headers="firstrow", tablefmt="grid"))
                
                # Add cost trend analysis
                if len(cost_analysis['daily_costs']) > 1:
                    report.append(f"\n📈 COST TREND ANALYSIS")
                    report.append("-" * 40)
                    
                    # Calculate trend for primary cost metric
                    primary_metric = cost_analysis['available_metrics'][0] if cost_analysis['available_metrics'] else 'UnblendedCost'
                    metric_key = primary_metric.lower().replace('cost', '')
                    
                    first_day_cost = cost_analysis['daily_costs'][0].get(metric_key, 0)
                    last_day_cost = cost_analysis['daily_costs'][-1].get(metric_key, 0)
                    
                    if first_day_cost > 0:
                        trend_percent = ((last_day_cost - first_day_cost) / first_day_cost) * 100
                        trend_direction = "increased" if trend_percent > 0 else "decreased" if trend_percent < 0 else "remained stable"
                        report.append(f"• {primary_metric} {trend_direction} by {abs(trend_percent):.1f}% from first to last day")
                    
                    # Calculate average daily cost
                    total_days = len(cost_analysis['daily_costs'])
                    avg_daily = cost_analysis['total_costs'].get(metric_key, 0) / total_days
                    report.append(f"• Average daily {primary_metric}: ${avg_daily:.2f}")
                    
                    # Calculate projected monthly cost
                    projected_monthly = avg_daily * 30
                    report.append(f"• Projected monthly {primary_metric}: ${projected_monthly:.2f}")
        else:
            report.append("No cost data available for the specified period")
            report.append("Note: Cost Explorer API may have up to 24-48 hour delay for recent data")
        
        # Recommendations
        report.append("\n💡 RECOMMENDATIONS")
        report.append("-" * 40)
        
        if performance_analysis:
            if 'CPUUtilization' in performance_analysis and performance_analysis['CPUUtilization']['avg'] > 80:
                report.append("• Consider upgrading to a larger instance class for better CPU performance")
            
            if 'FreeableMemory' in performance_analysis:
                mem_avg = performance_analysis['FreeableMemory']['avg']
                if mem_avg < 100 * 1024 * 1024:  # Less than 100MB
                    report.append("• Low free memory detected - consider instance upgrade or query optimization")
            
            if 'DatabaseConnections' in performance_analysis:
                conn_max = performance_analysis['DatabaseConnections']['max']
                if conn_max > 100:
                    report.append("• High connection count detected - review connection pooling and application logic")
        
        # Cost-based recommendations
        if cost_analysis['available_metrics']:
            # Use UnblendedCost as the primary metric for recommendations
            primary_cost = cost_analysis['total_costs'].get('unblended', 0)
            if primary_cost == 0:
                primary_cost = cost_analysis['total_costs'].get('blended', 0)
            
            if primary_cost > 100:
                report.append("• Consider Reserved Instances for cost optimization if usage is predictable")
            
            # Compare different cost metrics for insights
            if 'BlendedCost' in cost_analysis['available_metrics'] and 'UnblendedCost' in cost_analysis['available_metrics']:
                blended_total = cost_analysis['total_costs'].get('blended', 0)
                unblended_total = cost_analysis['total_costs'].get('unblended', 0)
                if blended_total > 0 and unblended_total > 0:
                    diff_percent = ((blended_total - unblended_total) / unblended_total) * 100
                    if abs(diff_percent) > 10:
                        report.append(f"• Significant difference between Blended (${blended_total:.2f}) and Unblended (${unblended_total:.2f}) costs: {diff_percent:+.1f}%")
                        report.append("  This may indicate Reserved Instance or Savings Plan usage")
        
        report.append("\n" + "=" * 80)
        
        return "\n".join(report)


def main():
    """
    Main function to run the RDS analyzer.
    
    Parses command line arguments and orchestrates the analysis process.
    """
    parser = argparse.ArgumentParser(
        description='Analyze RDS instance performance and costs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python get_cost_usage.py --db-instance-id mydb --start-time 2024-01-01 --end-time 2024-01-31 --region us-east-1
  python get_cost_usage.py --db-instance-id mydb --start-time 2024-01-01T00:00:00Z --end-time 2024-01-31T23:59:59Z --region us-west-2
        """
    )
    
    parser.add_argument('--db-instance-id', required=True,
                       help='RDS DB Instance Identifier')
    parser.add_argument('--start-time', required=True,
                       help='Start time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)')
    parser.add_argument('--end-time', required=True,
                       help='End time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)')
    parser.add_argument('--region', required=True,
                       help='AWS region')
    parser.add_argument('--output-file', 
                       help='Output file to save the report (optional)')
    parser.add_argument('--export-csv', 
                       help='Export detailed cost breakdowns to CSV files. Specify folder name (timestamp will be added automatically)')
    
    args = parser.parse_args()
    
    # Validate and format datetime strings
    try:
        # Handle different datetime formats
        if 'T' not in args.start_time:
            args.start_time += 'T00:00:00Z'
        if 'T' not in args.end_time:
            args.end_time += 'T23:59:59Z'
        
        # Validate datetime format
        datetime.fromisoformat(args.start_time.replace('Z', '+00:00'))
        datetime.fromisoformat(args.end_time.replace('Z', '+00:00'))
        
    except ValueError as e:
        print(f"Error: Invalid datetime format. {e}")
        print("Use format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ")
        sys.exit(1)
    
    try:
        # Initialize analyzer
        analyzer = RDSAnalyzer(args.region)
        
        # Get RDS instance information
        print(f"Getting RDS instance information for {args.db_instance_id}...")
        instance_info = analyzer.get_rds_instance_info(args.db_instance_id)
        
        # Collect performance metrics
        metrics = analyzer.get_rds_metrics(args.db_instance_id, args.start_time, args.end_time)
        
        # Collect cost data with Cloudera-Resource-Name filter
        cloudera_resource_name = instance_info.get('ClouderaResourceName')
        cost_data = analyzer.get_cost_data(args.db_instance_id, args.start_time, args.end_time, cloudera_resource_name)
        
        # Analyze data
        performance_analysis = analyzer.analyze_performance(metrics)
        cost_analysis = analyzer.analyze_costs(cost_data)
        
        # Generate report
        report = analyzer.generate_report(
            args.db_instance_id, 
            instance_info, 
            performance_analysis, 
            cost_analysis
        )
        
        # Output report
        print("\n" + report)
        
        # Save to file if requested
        if args.output_file:
            with open(args.output_file, 'w') as f:
                f.write(report)
            print(f"\nReport saved to: {args.output_file}")
        
        # Export CSV files if requested
        if args.export_csv:
            analyzer.export_cost_breakdowns_to_csv(cost_analysis, args.export_csv)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
