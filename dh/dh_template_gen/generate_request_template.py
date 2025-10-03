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
from typing import Dict, Any, Optional, Union, List
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DistroXRequestTemplateGenerator:
    """Generates DistroX request templates from cluster data"""
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.cli_command_data = None
        
    def get_cluster_data_from_cli(self, cluster_name: str) -> Dict[str, Any]:
        """
        Get cluster data using CDP CLI.
        
        Args:
            cluster_name (str): Name of the cluster to describe
            
        Returns:
            Dict[str, Any]: Cluster data in JSON format
            
        Raises:
            subprocess.CalledProcessError: If CDP CLI command fails
            json.JSONDecodeError: If CLI output cannot be parsed as JSON
        """
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
        """
        Get cluster data from JSON file.
        
        Args:
            file_path (str): Path to the JSON file containing cluster data
            
        Returns:
            Dict[str, Any]: Cluster data loaded from JSON file
            
        Raises:
            FileNotFoundError: If the specified file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
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
        """
        Parse CDP CLI command file to extract additional configuration details.
        
        Args:
            cli_file_path (str): Path to file containing CDP CLI command
            
        Returns:
            Dict[str, Any]: Parsed configuration data including tags, subnet ID, multi-AZ settings, etc.
            
        Raises:
            FileNotFoundError: If the CLI command file doesn't exist
        """
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
        """
        Parse CDP CLI command string to extract configuration details.
        
        Args:
            cli_command (str): Raw CLI command string to parse
            
        Returns:
            Dict[str, Any]: Dictionary containing extracted configuration:
                - tags: User-defined tags
                - subnet_id: Subnet ID if specified
                - multi_az: Multi-AZ setting
                - enable_load_balancer: Load balancer setting
                - datahub_database: Database configuration
                - instance_groups_override: Instance group overrides
        """
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

            # Some exported CLI files may contain doubled quotes (e.g., key=""k"",value=""v"")
            # Normalize them to single quotes to make regex parsing robust
            normalized_tags_str = tags_str.replace('""', '"')

            tag_pairs = re.findall(r'key="([^"]+)",value="([^"]+)"', normalized_tags_str)
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
    
    def _get_bucket_name_from_datalake_crn(self, datalake_crn: Optional[str]) -> Optional[str]:
        """
        Get bucket name from datalake CRN using CDP CLI.
        
        Args:
            datalake_crn (Optional[str]): Datalake CRN to query for bucket information
            
        Returns:
            Optional[str]: S3 bucket name extracted from datalake configuration, or None if not found
        """
        if not datalake_crn:
            return None
        
        try:
            for arg in ["--datalake-name", "--datalake-crn"]:
                cmd = ["cdp", "datalake", "describe-datalake", arg, datalake_crn]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                data = json.loads(result.stdout)
                
                if data and "datalake" in data:
                    location = data["datalake"].get("cloudStorageBaseLocation")
                    if location and location.startswith("s3a://"):
                        bucket = location[6:].split("/", 1)[0]
                        return bucket
        except Exception as e:
            logger.warning(f"Failed to get bucket name from datalake CRN: {e}")
        
        return None
    
    def parse_instance_groups_argument(self, instance_groups_arg: str) -> List[Dict[str, Any]]:
        """
        Parse the --instance-groups argument into a list of instance group configurations.
        
        Args:
            instance_groups_arg (str): Space-separated string of instance group configurations
                Format: "nodeCount=2,instanceGroupName=core,... nodeCount=1,instanceGroupName=worker,..."
            
        Returns:
            List[Dict[str, Any]]: List of parsed instance group configuration dictionaries
        """
        import ast
        
        def parse_attached_volumes(val):
            """
            Parse attached volume configuration string.
            
            Args:
                val (str): Volume configuration string in format "[{volumeSize=256,volumeCount=2,volumeType=gp3}]"
                
            Returns:
                List[Dict[str, Any]]: List of volume configuration dictionaries with mapped field names
            """
            if not val or not val.startswith("[") or not val.endswith("]"):
                return []
            
            # Replace = with : and wrap keys/values in quotes for ast.literal_eval
            items = val[1:-1].split("},{")
            result = []
            for item in items:
                item = item.strip("{}")
                d = {}
                for pair in item.split(","):
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        # Try to convert to int if possible
                        if k in ("volumeSize", "volumeCount"):
                            try:
                                v = int(v)
                            except Exception:
                                pass
                        d[k] = v
                if d:
                    # Map CLI field names to template field names
                    mapped_volume = {}
                    if "volumeSize" in d:
                        mapped_volume["size"] = d["volumeSize"]
                    if "volumeCount" in d:
                        mapped_volume["count"] = d["volumeCount"]
                    if "volumeType" in d:
                        mapped_volume["type"] = d["volumeType"]
                    if mapped_volume:
                        result.append(mapped_volume)
            return result

        # Split by spaces, each is an instance group
        groups = []
        for group_str in instance_groups_arg.strip().split(" "):
            if not group_str.strip():
                continue
            group = {}
            
            # Handle attachedVolumeConfiguration specially since it contains commas
            # First, extract attachedVolumeConfiguration if present
            if "attachedVolumeConfiguration=" in group_str:
                # Find the start and end of the attachedVolumeConfiguration value
                start_pos = group_str.find("attachedVolumeConfiguration=") + len("attachedVolumeConfiguration=")
                
                # Find the matching closing bracket
                bracket_count = 0
                end_pos = start_pos
                for i, char in enumerate(group_str[start_pos:], start_pos):
                    if char == '[':
                        bracket_count += 1
                    elif char == ']':
                        bracket_count -= 1
                        if bracket_count == 0:
                            end_pos = i + 1
                            break
                
                # Extract the volume config and the rest of the string
                volume_config_str = group_str[start_pos:end_pos]
                before_volume = group_str[:start_pos-len("attachedVolumeConfiguration=")]
                after_volume = group_str[end_pos:]
                
                # Parse the volume configuration
                group["attachedVolumeConfiguration"] = parse_attached_volumes(volume_config_str)
                
                # Parse the remaining parts
                remaining_parts = []
                if before_volume.strip():
                    remaining_parts.extend(before_volume.rstrip(",").split(","))
                if after_volume.strip():
                    remaining_parts.extend(after_volume.lstrip(",").split(","))
                
                # Parse remaining key=value pairs
                for part in remaining_parts:
                    part = part.strip()
                    if "=" in part and part:
                        k, v = part.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        if k == "nodeCount":
                            try:
                                v = int(v)
                            except Exception:
                                pass
                        elif k == "rootVolumeSize":
                            try:
                                v = int(v)
                            except Exception:
                                pass
                        elif k == "recipeNames":
                            v = [x.strip() for x in v.split(",") if x.strip()]
                        group[k] = v
            else:
                # No attachedVolumeConfiguration, parse normally
                for pair in group_str.split(","):
                    if "=" not in pair:
                        continue
                    k, v = pair.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if k == "nodeCount":
                        try:
                            v = int(v)
                        except Exception:
                            pass
                    elif k == "rootVolumeSize":
                        try:
                            v = int(v)
                        except Exception:
                            pass
                    elif k == "recipeNames":
                        v = [x.strip() for x in v.split(",") if x.strip()]
                    group[k] = v
            
            groups.append(group)
        return groups
    
    def merge_instance_group_override(self, template_group: Dict[str, Any], override_group: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge a template instance group with an override group.
        
        Args:
            template_group (Dict[str, Any]): Base instance group configuration from template
            override_group (Dict[str, Any]): Override configuration to apply
            
        Returns:
            Dict[str, Any]: Merged instance group configuration with overrides applied
        """
        merged = template_group.copy()
        
        logger.info(f"Overriding instance group '{template_group.get('name', 'unknown')}' with: {override_group}")
        
        # Map from override_group keys to template_group keys
        key_map = {
            "instanceGroupName": "name",
            "nodeCount": "nodeCount",
            "instanceGroupType": "type",
            "instanceType": ("template", "instanceType"),
            "attachedVolumeConfiguration": ("template", "attachedVolumes"),
            "rootVolumeSize": ("template", "rootVolume", "size"),
            "recipeNames": "recipeNames",
            "recoveryMode": "recoveryMode",
        }
        
        for k, v in override_group.items():
            if k not in key_map:
                logger.debug(f"Skipping unknown override key: {k}")
                continue
            mapped = key_map[k]
            if isinstance(mapped, str):
                logger.info(f"Setting {mapped} = {v}")
                merged[mapped] = v
            elif isinstance(mapped, tuple):
                # Nested dicts
                d = merged
                for key in mapped[:-1]:
                    if key not in d or not isinstance(d[key], dict):
                        d[key] = {}
                    d = d[key]
                logger.info(f"Setting nested {mapped} = {v}")
                d[mapped[-1]] = v
        
        return merged
    
    def _parse_instance_groups_string(self, instance_groups_str: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse instance groups string from CLI command.
        
        Args:
            instance_groups_str (str): Space-separated string of instance group configurations from CLI
            
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping group names to their configurations
        """
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
    
    def extract_instance_group_details(self, group: Dict[str, Any], skip_cli_overrides: bool = False) -> Dict[str, Any]:
        """
        Extract and format instance group details.
        
        Args:
            group (Dict[str, Any]): Raw instance group data from cluster description
            skip_cli_overrides (bool): If True, skip applying CLI command overrides for instance group type
            
        Returns:
            Dict[str, Any]: Formatted instance group configuration for request template
        """
        instances = group.get("instances", [])
        first_instance = instances[0] if instances else {}
        
        instance_type = first_instance.get("instanceVmType", "m6i.4xlarge")
        attached_volumes = []
        
        raw_volumes = first_instance.get("attachedVolumes", [])
        if raw_volumes:
            for volume in raw_volumes:
                volume_type = volume.get("volumeType", "gp3")
                # Convert only gp2 volumes to gp3, keep ephemeral as is
                if volume_type == "gp2":
                    volume_type = "gp3"
                    logger.info(f"Converted volume type from gp2 to gp3")
                
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

        if not skip_cli_overrides and self.cli_command_data and self.cli_command_data.get("instance_groups_override"):
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
            if skip_cli_overrides:
                logger.debug("Skipping CLI command data for instance group type due to explicit overrides")
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
            "subnetIds": group.get("subnetIds", [])
        }
    
    def extract_image_details(self, image_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract image details from cluster data.
        
        Args:
            image_data (Dict[str, Any]): Image information from cluster description
            
        Returns:
            Dict[str, Any]: Formatted image configuration with id and catalog fields
        """
        return {
            "id": image_data.get("id"),
            "catalog": image_data.get("catalogName", "cdp-default")
        }
    
    def extract_network_details(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract network configuration from cluster data.
        
        Args:
            cluster_data (Dict[str, Any]): Complete cluster description data
            
        Returns:
            Dict[str, Any]: Network configuration with subnetId and networkId fields
        """
        subnet_ids = []
        if self.cli_command_data and self.cli_command_data.get("subnet_id"):
            subnet_ids = [self.cli_command_data["subnet_id"]]
        else:
            cluster_info = cluster_data.get("cluster", {})
            for group in cluster_info.get("instanceGroups", []):
                subnet_ids.extend(group.get("subnetIds", []))
            subnet_ids = list(dict.fromkeys(subnet_ids))
        
        return {
            "subnetId": subnet_ids[0] if subnet_ids else None,
            "networkId": None
        }
    
    def extract_cluster_details(self, cluster_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract cluster configuration details.
        
        Note: 
        - blueprintName is populated from workloadType since describe-cluster returns null for blueprintName
        - Only essential fields for request templates are included
        
        Args:
            cluster_data (Dict[str, Any]): Complete cluster description data
            
        Returns:
            Dict[str, Any]: Cluster configuration with blueprintName field
        """
        cluster_info = cluster_data.get("cluster", {})
        blueprint_name = cluster_info.get("workloadType")
        return {
            "blueprintName": blueprint_name
        }
    
    def _build_tags(self, cluster_info: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
        """
        Build tags combining CLI command tags with generated tags.
        
        Args:
            cluster_info (Dict[str, Any]): Cluster information from description
            cluster_name (str): Final cluster name to use in dhname tag
            
        Returns:
            Dict[str, Any]: Combined tags including generated and CLI command tags
        """
        user_defined_tags = {
            "generated-date": self.timestamp,
            "source-cluster": cluster_info.get("clusterName", "unknown"),
            "dhname": cluster_name
        }
        
        if self.cli_command_data and self.cli_command_data.get("tags"):
            logger.debug(f"CLI command tags found: {self.cli_command_data['tags']}")
            for key, value in self.cli_command_data["tags"].items():
                user_defined_tags[key] = value
                logger.debug(f"Added CLI tag to template: {key} = {value}")
        else:
            logger.debug("No CLI command tags found")
        
        return user_defined_tags
    
    def _get_load_balancer_setting(self, cluster_info: Dict[str, Any]) -> bool:
        """
        Get load balancer setting from CLI command or default.
        
        Args:
            cluster_info (Dict[str, Any]): Cluster information (currently unused)
            
        Returns:
            bool: Load balancer setting from CLI command or False as default
        """
        if self.cli_command_data and "enable_load_balancer" in self.cli_command_data:
            return self.cli_command_data["enable_load_balancer"]
        return False
    
    def _get_multi_az_setting(self, cluster_info: Dict[str, Any]) -> bool:
        """
        Get multi-AZ setting from CLI command or cluster data.
        
        Args:
            cluster_info (Dict[str, Any]): Cluster information containing multiAz field
            
        Returns:
            bool: Multi-AZ setting from CLI command or cluster data, defaulting to False
        """
        if self.cli_command_data and "multi_az" in self.cli_command_data:
            return self.cli_command_data["multi_az"]
        return cluster_info.get("multiAz", False)
    
    def generate_request_template(self, cluster_data: Dict[str, Any], 
                                cluster_name: Optional[str] = None,
                                environment_name: Optional[str] = None,
                                bucket_name: Optional[str] = None,
                                datalake_name: Optional[str] = None,
                                dh_name: Optional[str] = None,
                                instance_groups_override: Optional[List[Dict[str, Any]]] = None,
                                subnet_id: Optional[str] = None,
                                subnet_ids: Optional[List[str]] = None,
                                java_version: int = 8) -> Dict[str, Any]:
        """
        Generate the complete DistroX request template.
        
        Args:
            cluster_data (Dict[str, Any]): Raw cluster description data
            cluster_name (Optional[str]): Override cluster name (overridden by dh_name)
            environment_name (Optional[str]): Override environment name
            bucket_name (Optional[str]): Override S3 bucket name
            datalake_name (Optional[str]): Override datalake name
            dh_name (Optional[str]): Override DataHub cluster name (highest priority for cluster name)
            instance_groups_override (Optional[List[Dict[str, Any]]]): Instance group configurations to override
            subnet_id (Optional[str]): Single subnet ID to apply to all instance groups
            subnet_ids (Optional[List[str]]): List of subnet IDs to apply to all instance groups
            java_version (int): Java version to use in the template (default: 8)
            
        Returns:
            Dict[str, Any]: Complete DistroX request template in JSON format
        """
        
        cluster_info = cluster_data.get("cluster", {})
        # Priority: dh_name > cluster_name > original cluster.clusterName > fallback
        final_cluster_name = dh_name or cluster_name or cluster_info.get("clusterName", "generated-cluster")
        final_environment_name = environment_name or cluster_info.get("environmentName", "default-environment")
        
        # Get bucket name
        final_bucket_name = bucket_name
        if not final_bucket_name:
            datalake_crn = cluster_info.get("datalakeCrn")
            final_bucket_name = self._get_bucket_name_from_datalake_crn(datalake_crn)
            if not final_bucket_name:
                final_bucket_name = "customer-bucket-name"
        
        instance_groups = []
        skip_cli_overrides = bool(instance_groups_override)
        for group in cluster_info.get("instanceGroups", []):
            instance_groups.append(self.extract_instance_group_details(group, skip_cli_overrides=skip_cli_overrides))
        
        # Determine subnet IDs to use
        final_subnet_ids = []
        if subnet_ids:
            final_subnet_ids = subnet_ids
            logger.info(f"Using subnet IDs from --subnet-ids: {final_subnet_ids}")
        elif subnet_id:
            final_subnet_ids = [subnet_id]
            logger.info(f"Using single subnet ID from --subnet-id: {final_subnet_ids}")
        else:
            # Use original subnet IDs from the first instance group as fallback
            if instance_groups:
                final_subnet_ids = instance_groups[0].get("subnetIds", [])
                logger.info(f"Using original subnet IDs from template: {final_subnet_ids}")
        
        # Apply subnet IDs to all instance groups
        if final_subnet_ids:
            for group in instance_groups:
                group["subnetIds"] = final_subnet_ids
            logger.info(f"Applied subnet IDs to all instance groups: {final_subnet_ids}")
        
        # Apply instance groups overrides if provided
        if instance_groups_override:
            for override in instance_groups_override:
                # Only override groups that match the instanceGroupName
                if "instanceGroupName" in override:
                    target_name = override["instanceGroupName"]
                    # Find and override only the matching group
                    for i, group in enumerate(instance_groups):
                        if group.get("name") == target_name:
                            logger.info(f"Overriding instance group '{target_name}' with CLI arguments")
                            merged = self.merge_instance_group_override(group, override)
                            instance_groups[i] = merged
                            break
                    else:
                        logger.warning(f"No instance group found with name '{target_name}' to override")
                else:
                    logger.warning("Instance group override provided without instanceGroupName - skipping")
        
        image_details = None
        if "imageDetails" in cluster_info:
            image_details = self.extract_image_details(cluster_info["imageDetails"])
        
        network_details = self.extract_network_details(cluster_data)
        
        cluster_details = self.extract_cluster_details(cluster_data)
        
        request_template = {
            "environmentName": final_environment_name,
            "name": final_cluster_name,
            "instanceGroups": instance_groups,
            "image": image_details,
            "network": network_details,
            "cluster": {
                "databases": [],
                "cloudStorage": {
                    "locations": [
                        {
                            "type": "YARN_LOG",
                            "value": f"s3a://{final_bucket_name}/datalake/oplogs/yarn-app-logs"
                        },
                        {
                            "type": "ZEPPELIN_NOTEBOOK",
                            "value": f"s3a://{final_bucket_name}/datalake/{final_cluster_name}/zeppelin/notebook"
                        }
                    ]
                },
                "exposedServices": ["ALL"],
                "blueprintName": cluster_info.get("workloadType", "<unknown>"),
                "validateBlueprint": False
            },
            "externalDatabase": {
                "availabilityType": "HA"
            },
            "tags": {
                "application": None,
                "userDefined": self._build_tags(cluster_info, final_cluster_name),
                "defaults": None
            },
            "inputs": {
                "ynlogd.dirs": "/hadoopfs/fs1/nodemanager/log,/hadoopfs/fs2/nodemanager/log",
                "ynld.dirs": "/hadoopfs/fs1/nodemanager,/hadoopfs/fs2/nodemanager",
                "dfs.dirs": "/hadoopfs/fs3/datanode,/hadoopfs/fs4/datanode",
                "query_data_hive_path": f"s3a://{final_bucket_name}/warehouse/tablespace/external/{final_cluster_name}/hive/sys.db/query_data",
                "query_data_tez_path": f"s3a://{final_bucket_name}/warehouse/tablespace/external/{final_cluster_name}/hive/sys.db"
            },
            "gatewayPort": None,
            "enableLoadBalancer": self._get_load_balancer_setting(cluster_info),
            "variant": "CDP",
            "javaVersion": java_version,
            "enableMultiAz": self._get_multi_az_setting(cluster_info),
            "architecture": "x86_64",
            "disableDbSslEnforcement": False,
            "security": cluster_info.get("security", {})
        }
        
        return request_template
    
    def save_template(self, template: Dict[str, Any], output_dir: str, 
                     cluster_name: str, source_type: str) -> str:
        """
        Save the generated template to a file.
        
        Args:
            template (Dict[str, Any]): The generated request template
            output_dir (str): Directory where the template file should be saved
            cluster_name (str): Name of the cluster (used in filename)
            source_type (str): Type of source (e.g., "running-cluster", "json-file")
            
        Returns:
            str: Full path to the saved template file
        """
        
        timestamped_dir = Path(output_dir) / f"request-template-{self.timestamp}"
        timestamped_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"{cluster_name}_{source_type}_template_{self.timestamp}.json"
        filepath = timestamped_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(template, f, indent=2)
        
        logger.info(f"Template saved to: {filepath}")
        return str(filepath)

def main():
    """
    Main function to handle command line arguments and execute template generation.
    
    Processes command line arguments, loads cluster data, generates request template,
    and saves the result to a timestamped file in the specified output directory.
    """
    
    parser = argparse.ArgumentParser(
        description="Generate DistroX request templates from running clusters or JSON files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate from running cluster
  python generate_request_template.py --cluster-name jdga-dm-01 --output ./templates
  
  # Generate from JSON file
  python generate_request_template.py --input-file cluster_data.json --output ./templates
  
  # Generate with custom names and bucket
  python generate_request_template.py --cluster-name my-cluster --environment-name my-env --bucket-name my-bucket --output ./templates
  
  # Generate with DataHub name override
  python generate_request_template.py --input-file cluster_data.json --dh-name my-new-dh-cluster --output ./templates
  
  # Generate with instance groups override and subnet IDs
  python generate_request_template.py --cluster-name my-cluster --instance-groups "nodeCount=3,instanceGroupName=core,instanceGroupType=CORE,instanceType=m6i.4xlarge,rootVolumeSize=200" --subnet-ids subnet-123 subnet-456 --output ./templates
  
  # Generate with single subnet ID
  python generate_request_template.py --cluster-name my-cluster --instance-groups "nodeCount=3,instanceGroupName=core,instanceGroupType=CORE,instanceType=m6i.4xlarge,rootVolumeSize=200" --subnet-id subnet-123 --output ./templates
  
  # Generate with CLI command file for additional configuration
  python generate_request_template.py --input-file cluster_data.json --cli-command-file cli_command.txt --bucket-name my-bucket --output ./templates
  
  # Generate with custom Java version
  python generate_request_template.py --cluster-name my-cluster --java-version 11 --output ./templates
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
    
    parser.add_argument(
        "--bucket-name", "-b",
        help="S3 bucket name to use in the generated template"
    )
    
    parser.add_argument(
        "--datalake-name", "-d",
        help="Override the datalake name in the template"
    )
    
    parser.add_argument(
        "--dh-name",
        help="Override the DataHub cluster name in the template (overrides --cluster-name)"
    )
    
    parser.add_argument(
        "--instance-groups", "-i",
        nargs='+',
        help="Override instance groups configuration. Can specify multiple groups. Syntax: nodeCount=2,instanceGroupName=core,instanceGroupType=CORE,instanceType=m6i.4xlarge,attachedVolumeConfiguration=[{volumeSize=256,volumeCount=2,volumeType=gp3}],rootVolumeSize=200,recipeNames=recipe1,recipe2,recoveryMode=MANUAL"
    )
    
    parser.add_argument(
        "--subnet-id",
        help="Single subnet ID to apply to all instance groups"
    )
    
    parser.add_argument(
        "--subnet-ids",
        nargs='+',
        help="List of subnet IDs to apply to all instance groups"
    )
    
    parser.add_argument(
        "--java-version",
        type=int,
        default=8,
        help="Java version to use in the template (default: 8)"
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
    
    # Parse instance groups override if provided
    instance_groups_override = None
    if args.instance_groups:
        try:
            # Join all instance group strings into one string for parsing
            instance_groups_str = " ".join(args.instance_groups)
            instance_groups_override = generator.parse_instance_groups_argument(instance_groups_str)
            logger.info(f"Parsed instance groups override: {instance_groups_override}")
        except Exception as e:
            logger.error(f"Failed to parse instance groups argument: {e}")
            sys.exit(1)
    
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
            environment_name=args.environment_name,
            bucket_name=args.bucket_name,
            datalake_name=args.datalake_name,
            dh_name=args.dh_name,
            instance_groups_override=instance_groups_override,
            subnet_id=args.subnet_id,
            subnet_ids=args.subnet_ids,
            java_version=args.java_version
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
