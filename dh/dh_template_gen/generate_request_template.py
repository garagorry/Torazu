#!/usr/bin/env python3
"""
DH Request Template Generator

This script generates request templates from either:
1. A running Cloudera DataHub cluster using 'cdp datahub describe-cluster'
2. A JSON file containing cluster description data

"""

import json
import argparse
import subprocess
import sys
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DistroXRequestTemplateGenerator:
    """Generates DistroX request templates from cluster data"""
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.cli_command_data = None
        
    def get_cluster_data_from_cli(self, cluster_name: str) -> Dict[str, Any]:
        """Get cluster data using CDP CLI"""
        try:
            logger.info(f"Fetching cluster data for '{cluster_name}' using CDP CLI...")
            cmd = ["cdp", "datahub", "describe-cluster", "--cluster-name", cluster_name]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            cluster_data = json.loads(result.stdout)
            
            logger.info(f"Successfully retrieved data for cluster '{cluster_name}'")
            return cluster_data
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to execute CDP CLI command: {e}")
            logger.error(f"Command output: {e.stderr}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse CLI output as JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting cluster data: {e}")
            raise
    
    def get_cluster_data_from_file(self, file_path: str) -> Dict[str, Any]:
        """Get cluster data from JSON file"""
        try:
            logger.info(f"Reading cluster data from file: {file_path}")
            with open(file_path, 'r') as f:
                cluster_data = json.load(f)
            
            logger.info(f"Successfully loaded data from file: {file_path}")
            return cluster_data
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading file: {e}")
            raise
    
    def parse_cli_command_file(self, cli_file_path: str) -> Dict[str, Any]:
        """Parse CDP CLI command file to extract additional configuration details"""
        try:
            logger.info(f"Reading CLI command from file: {cli_file_path}")
            with open(cli_file_path, 'r') as f:
                cli_content = f.read().strip()
            
            # Parse the CLI command to extract key information
            parsed_data = self._parse_cli_command(cli_content)
            
            logger.info(f"Successfully parsed CLI command from: {cli_file_path}")
            return parsed_data
            
        except FileNotFoundError:
            logger.error(f"CLI command file not found: {cli_file_path}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading CLI command file: {e}")
            raise
    
    def _parse_cli_command(self, cli_command: str) -> Dict[str, Any]:
        """Parse CDP CLI command string to extract configuration details"""
        parsed = {
            "tags": {},
            "subnet_id": None,
            "multi_az": False,
            "enable_load_balancer": False,
            "datahub_database": "NONE",
            "instance_groups_override": {}
        }
        
        if '--tags' in cli_command:
            start_pos = cli_command.find('--tags') + len('--tags')
            next_dash = cli_command.find('--', start_pos)
            if next_dash > 0:
                tags_str = cli_command[start_pos:next_dash].strip()
            else:
                tags_str = cli_command[start_pos:].strip()
            
            logger.debug(f"Found tags string: {tags_str}")
            
            tag_pairs = re.findall(r'key="([^"]+)",value="([^"]+)"', tags_str)
            logger.debug(f"Parsed tag pairs: {tag_pairs}")
            
            for key, value in tag_pairs:
                parsed["tags"][key] = value
                logger.debug(f"Added tag: {key} = {value}")
        else:
            logger.debug("No --tags found in CLI command")
        
        subnet_match = re.search(r'--subnet-id\s+(\S+)', cli_command)
        if subnet_match:
            parsed["subnet_id"] = subnet_match.group(1)
        
        if '--no-multi-az' in cli_command:
            parsed["multi_az"] = False
        elif '--multi-az' in cli_command:
            parsed["multi_az"] = True
        
        if '--no-enable-load-balancer' in cli_command:
            parsed["enable_load_balancer"] = False
        elif '--enable-load-balancer' in cli_command:
            parsed["enable_load_balancer"] = True
        
        db_match = re.search(r'--datahub-database\s+(\S+)', cli_command)
        if db_match:
            parsed["datahub_database"] = db_match.group(1)
        
        instance_groups_match = re.search(r'--instance-groups\s+(.+?)(?=\s+--|\s*$)', cli_command, re.DOTALL)
        if instance_groups_match:
            instance_groups_str = instance_groups_match.group(1).strip()
            logger.debug(f"Extracted instance groups string: {instance_groups_str}")
            parsed["instance_groups_override"] = self._parse_instance_groups_string(instance_groups_str)
        else:
            logger.debug("No --instance-groups found in CLI command")
        
        return parsed
    
    def _parse_instance_groups_string(self, instance_groups_str: str) -> Dict[str, Dict[str, Any]]:
        """Parse instance groups string from CLI command"""
        instance_groups = {}
        
        logger.debug(f"Parsing instance groups string: {instance_groups_str}")
        
        group_definitions = instance_groups_str.split(' ')
        logger.debug(f"Found {len(group_definitions)} group definitions")
        
        for i, group_def in enumerate(group_definitions):
            if not group_def.strip():
                continue
                
            logger.debug(f"Processing group definition {i+1}: {group_def}")
            
            group_config = {}
            pairs = group_def.split(',')
            
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    group_config[key] = value
                    logger.debug(f"  Parsed: {key} = {value}")
            
            group_name = group_config.get('instanceGroupName', 'unknown')
            instance_groups[group_name] = group_config
            logger.debug(f"Added instance group '{group_name}' with config: {group_config}")
        
        logger.info(f"Successfully parsed {len(instance_groups)} instance groups from CLI command")
        return instance_groups
    
    def extract_instance_group_details(self, group: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and format instance group details"""
        instances = group.get("instances", [])
        first_instance = instances[0] if instances else {}
        
        instance_type = first_instance.get("instanceVmType", "m6i.4xlarge")
        attached_volumes = []
        
        raw_volumes = first_instance.get("attachedVolumes", [])
        if raw_volumes:
            for volume in raw_volumes:
                volume_type = volume.get("volumeType", "gp3")
                if volume_type == "ephemeral":
                    volume_type = "gp3"
                
                attached_volumes.append({
                    "size": volume.get("size", 256),
                    "count": volume.get("count", 1),
                    "type": volume_type
                })
        else:
            attached_volumes = [{
                "size": 256,
                "count": 2,
                "type": "gp3"
            }]
        
        instance_group_type = "CORE"  # Default fallback

        if self.cli_command_data and self.cli_command_data.get("instance_groups_override"):
            group_name = group.get("name", "default")
            cli_group_config = self.cli_command_data["instance_groups_override"].get(group_name, {})
            logger.debug(f"Looking for group '{group_name}' in CLI command data: {cli_group_config}")
            if cli_group_config.get("instanceGroupType"):
                instance_group_type = cli_group_config["instanceGroupType"]
                logger.info(f"Using instanceGroupType from CLI command: {instance_group_type} for group {group_name}")
            else:
                logger.debug(f"No instanceGroupType found in CLI command for group {group_name}, using fallback logic")
                instance_type_value = first_instance.get("instanceType")
                if instance_type_value == "GATEWAY_PRIMARY":
                    instance_group_type = "GATEWAY"
                else:
                    instance_group_type = "CORE"
        else:
            logger.debug("No CLI command data available, using fallback logic for instance group type")
            instance_type_value = first_instance.get("instanceType")
            if instance_type_value == "GATEWAY_PRIMARY":
                instance_group_type = "GATEWAY"
            else:
                instance_group_type = "CORE"

        logger.info(f"Final instance group type determined: {instance_group_type} for group {group.get('name', 'default')}")
        
        return {
            "name": group.get("name", "default"),
            "nodeCount": len(instances),
            "type": instance_group_type,
            "recoveryMode": "MANUAL",
            "minimumNodeCount": 0,
            "scalabilityOption": "ALLOWED",
            "template": {
                "aws": {
                    "encryption": {
                        "type": "DEFAULT",
                        "key": None
                    },
                    "placementGroup": {
                        "strategy": "PARTITION"
                    }
                },
                "instanceType": instance_type,
                "rootVolume": {
                    "size": 100
                },
                "attachedVolumes": attached_volumes,
                "cloudPlatform": "AWS"
            },
            "recipeNames": group.get("recipes", []),
            "subnetIds": group.get("subnetIds", []),
            "availabilityZones": group.get("availabilityZones", [])
        }
    
    def extract_image_details(self, image_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract image details from cluster data"""
        return {
            "catalogName": image_data.get("catalogName", "cdp-default"),
            "id": image_data.get("id")
        }
    
    def extract_network_details(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract network configuration"""
        subnet_ids = []
        if self.cli_command_data and self.cli_command_data.get("subnet_id"):
            subnet_ids = [self.cli_command_data["subnet_id"]]
        else:
            cluster_info = cluster_data.get("cluster", {})
            for group in cluster_info.get("instanceGroups", []):
                subnet_ids.extend(group.get("subnetIds", []))
            subnet_ids = list(dict.fromkeys(subnet_ids))
        
        return {
            "aws": {
                "vpcId": None,
                "subnetIds": subnet_ids
            }
        }
    
    def extract_cluster_details(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cluster configuration details
        
        Note: 
        - blueprintName is populated from workloadType since describe-cluster returns null for blueprintName
        - Only essential fields for request templates are included
        """
        cluster_info = cluster_data.get("cluster", {})
        blueprint_name = cluster_info.get("workloadType")
        return {
            "blueprintName": blueprint_name
        }
    
    def _build_tags(self, cluster_info: Dict[str, Any]) -> Dict[str, Any]:
        """Build tags combining CLI command tags with generated tags"""
        tags = {
            "userDefined": {
                "generated-date": self.timestamp,
                "source-cluster": cluster_info.get("clusterName", "unknown")                
            }
        }
        
        if self.cli_command_data and self.cli_command_data.get("tags"):
            logger.debug(f"CLI command tags found: {self.cli_command_data['tags']}")
            for key, value in self.cli_command_data["tags"].items():
                tags["userDefined"][key] = value
                logger.debug(f"Added CLI tag to template: {key} = {value}")
        else:
            logger.debug("No CLI command tags found")
        
        return tags
    
    def _get_load_balancer_setting(self, cluster_info: Dict[str, Any]) -> bool:
        """Get load balancer setting from CLI command or default"""
        if self.cli_command_data and "enable_load_balancer" in self.cli_command_data:
            return self.cli_command_data["enable_load_balancer"]
        return False
    
    def _get_multi_az_setting(self, cluster_info: Dict[str, Any]) -> bool:
        """Get multi-AZ setting from CLI command or cluster data"""
        if self.cli_command_data and "multi_az" in self.cli_command_data:
            return self.cli_command_data["multi_az"]
        return cluster_info.get("multiAz", False)
    
    def generate_request_template(self, cluster_data: Dict[str, Any], 
                                cluster_name: Optional[str] = None,
                                environment_name: Optional[str] = None) -> Dict[str, Any]:
        """Generate the complete DistroX request template"""
        
        cluster_info = cluster_data.get("cluster", {})
        final_cluster_name = cluster_name or cluster_info.get("clusterName", "generated-cluster")
        final_environment_name = environment_name or cluster_info.get("environmentName", "default-environment")
        
        instance_groups = []
        for group in cluster_info.get("instanceGroups", []):
            instance_groups.append(self.extract_instance_group_details(group))
        
        image_details = None
        if "imageDetails" in cluster_info:
            image_details = self.extract_image_details(cluster_info["imageDetails"])
        
        network_details = self.extract_network_details(cluster_data)
        
        cluster_details = self.extract_cluster_details(cluster_data)
        
        request_template = {
            "environmentName": final_environment_name,
            "instanceGroups": instance_groups,
            "image": image_details,
            "network": network_details,
            "cluster": cluster_details,
            "sdx": None,
            "externalDatabase": None,
            "tags": self._build_tags(cluster_info),
            "inputs": {},
            "gatewayPort": None,
            "enableLoadBalancer": self._get_load_balancer_setting(cluster_info),
            "variant": None,
            "javaVersion": None,
            "enableMultiAz": self._get_multi_az_setting(cluster_info),
            "architecture": None,
            "disableDbSslEnforcement": False,
            "security": {
                "seLinux": cluster_info.get("security", {}).get("seLinux", "PERMISSIVE")
            }
        }
        
        return request_template
    
    def save_template(self, template: Dict[str, Any], output_dir: str, 
                     cluster_name: str, source_type: str) -> str:
        """Save the generated template to a file"""
        
        timestamped_dir = Path(output_dir) / f"request-template-{self.timestamp}"
        timestamped_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"{cluster_name}_{source_type}_template_{self.timestamp}.json"
        filepath = timestamped_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(template, f, indent=2)
        
        logger.info(f"Template saved to: {filepath}")
        return str(filepath)

def main():
    """Main function to handle command line arguments and execute template generation"""
    
    parser = argparse.ArgumentParser(
        description="Generate DistroX request templates from running clusters or JSON files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate from running cluster
  python generate_request_template.py --cluster-name jdga-dm-01 --output ./templates
  
  # Generate from JSON file
  python generate_request_template.py --input-file cluster_data.json --output ./templates
  
  # Generate with custom names
  python generate_request_template.py --cluster-name my-cluster --environment-name my-env --output ./templates
  
  # Generate with CLI command file for additional configuration
  python generate_request_template.py --input-file cluster_data.json --cli-command-file cli_command.txt --output ./templates
        """
    )
    
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--cluster-name", "-c",
        help="Name of the running cluster to describe"
    )
    input_group.add_argument(
        "--input-file", "-f",
        help="Path to JSON file containing cluster description data"
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output directory for generated templates (default: /tmp/request-template-timestamp)"
    )
    
    parser.add_argument(
        "--environment-name", "-e",
        help="Override environment name in the generated template"
    )
    
    parser.add_argument(
        "--cli-command-file", "-l",
        help="Path to file containing CDP CLI create command for additional configuration"
    )
    
    args = parser.parse_args()
    
    generator = DistroXRequestTemplateGenerator()
    
    if args.cli_command_file:
        try:
            cli_data = generator.parse_cli_command_file(args.cli_command_file)
            generator.cli_command_data = cli_data
            logger.info(f"Loaded CLI command data from: {args.cli_command_file}")
            logger.info(f"Parsed CLI data: {cli_data}")
            if cli_data.get("tags"):
                logger.info(f"Found tags in CLI command: {cli_data['tags']}")
            else:
                logger.warning("No tags found in CLI command data")
        except Exception as e:
            logger.warning(f"Failed to load CLI command file: {e}")
            logger.warning("Continuing without CLI command data")
    
    try:
        if args.output:
            output_dir = args.output
        elif args.input_file:
            output_dir = str(Path(args.input_file).parent)
        else:
            output_dir = "/tmp"
        
        if args.cluster_name:
            logger.info(f"Generating template from running cluster: {args.cluster_name}")
            cluster_data = generator.get_cluster_data_from_cli(args.cluster_name)
            source_type = "running-cluster"
            cluster_name = args.cluster_name
        else:
            logger.info(f"Generating template from file: {args.input_file}")
            cluster_data = generator.get_cluster_data_from_file(args.input_file)
            source_type = "json-file"
            cluster_name = cluster_data.get("cluster", {}).get("clusterName", "unknown-cluster")
        
        logger.info("Generating request template...")
        template = generator.generate_request_template(
            cluster_data,
            cluster_name=cluster_name,
            environment_name=args.environment_name
        )
        
        output_path = generator.save_template(template, output_dir, cluster_name, source_type)
        
        logger.info("Template generation completed successfully!")
        logger.info(f"Output file: {output_path}")
        
        print(f"\n=== Template Generation Summary ===")
        print(f"Source: {source_type}")
        print(f"Cluster: {cluster_name}")
        print(f"Output: {output_path}")
        print(f"Timestamp: {generator.timestamp}")
        
    except Exception as e:
        logger.error(f"Template generation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
