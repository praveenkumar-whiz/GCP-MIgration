import json
import logging
from typing import Any, Dict, List
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("vertexai-abuse")
logger.setLevel(logging.INFO)


def abuse_scan_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    GCP vertex ai / Compute Engine VM abuse scan.
    
    Expected spec keys:
    - project_id: GCP project ID
    - region: Allowed region(s) - can be string "us-central1" or list ["us-central1", "us-east1"]
    - gcp_access_token / access_token: GCP access token
    - rules: {
          "max_instances": <int>,
          "allowed_machine_types": ["e2-micro", "n1-standard-1"],
          "allowed_disk_types": ["pd-standard", "pd-ssd"],
          "allowed_disk_size_gb": <int>,
          "token_limit": {
              "time_period_in_mins": 5,
              "allowed_token_limitation": 10000
          },
          "model_restriction": {
              "time_period_in_mins": 5,
              "allowed_model": ["gemini-2.5-flash", "gemini-2.5-pro"]
          }
      }
    """
    action = "VM_ABUSE_SCAN"
    
    logger.info("Vertex ai / VM Abuse Scan triggered")
    logger.info("Incoming spec: {}".format(json.dumps(spec)))
    
    abuse_message: List[str] = []
    rules = spec.get("rules", {}) or {}
    
    logger.info("Abuse rules: {}".format(json.dumps(rules)))
    
    project_id = spec.get("project_id")
    region = spec.get("region")
    
    # -------------------------------------------------------------------
    # VALIDATION
    # -------------------------------------------------------------------
    if not project_id:
        return {
            "status": "FAILED",
            "action": action,
            "reason": "MissingProjectId",
            "detail": "project_id is required",
        }
    
    if not region:
        return {
            "status": "FAILED",
            "action": action,
            "reason": "MissingRegion",
            "detail": "region is required",
            "project_id": project_id,
        }
    
    # Parse allowed regions - handle both string and list
    if isinstance(region, str):
        allowed_regions = set([region])
    elif isinstance(region, list):
        allowed_regions = set(region)
    else:
        return {
            "status": "FAILED",
            "action": action,
            "reason": "InvalidRegionFormat",
            "detail": "region must be a string or list of strings",
            "project_id": project_id,
        }
    
    try:
        # -------------------------------------------------------------------
        # CREDENTIALS
        # -------------------------------------------------------------------
        access_token = (
            spec.get("gcp_access_token")
            or spec.get("access_token")
            or spec.get("google_access_token")
        )
        
        if not access_token:
            return {
                "status": "FAILED",
                "action": action,
                "reason": "MissingAccessToken",
                "detail": "Access token is required",
                "project_id": project_id,
                "region": region,
            }
        
        client_creds = {"access_token": access_token}
        
        # -------------------------------------------------------------------
        # CLIENT CREATION
        # -------------------------------------------------------------------
        try:
            compute_client = GCPClientFactory.create(
                service="compute",
                creds=client_creds,
            )
        except Exception as e:
            logger.error("Compute client creation failed", exc_info=True)
            return {
                "status": "FAILED",
                "action": action,
                "reason": "ClientCreationFailed",
                "error": str(e),
                "project_id": project_id,
                "region": region,
            }
        
        logger.info("Connected to Compute Engine | Project={} | Region={}".format(
            project_id, region
        ))
        
        # ----------------------------------------------------------------
        # Client connection to GCP Cloud Monitoring Services
        # ----------------------------------------------------------------
        monitoring_client = None
        if "token_limit" in rules or "model_restriction" in rules:
            try:
                monitoring_client = GCPClientFactory.create(
                    "monitoring",
                    creds=client_creds,
                )
                logger.info("Connected to Cloud Monitoring | Project={}".format(project_id))
            except Exception as e:
                logger.error("Monitoring client creation failed", exc_info=True)
                return {
                    "status": "FAILED",
                    "action": action,
                    "reason": "MonitoringClientCreationFailed",
                    "error": str(e),
                    "project_id": project_id,
                    "region": region,
                }
        
        # -------------------------------------------------------------------
        # LIST ALL INSTANCES (aggregated across all zones)
        # -------------------------------------------------------------------
        try:
            all_instances = []
            page_token = None
            
            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token
                
                response = compute_client.request(
                    "GET",
                    "projects/{}/aggregated/instances".format(project_id),
                    params=params,
                )
                
                # Parse aggregated response
                items = response.get("items", {})
                for location_key, location_data in items.items():
                    if not isinstance(location_data, dict):
                        continue
                    
                    instances = location_data.get("instances", [])
                    for instance in instances:
                        if isinstance(instance, dict):
                            all_instances.append(instance)
                
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
            
            logger.info("Fetched all VM instances | Project={} | total={}".format(
                project_id, len(all_instances)
            ))
            
            # Filter for active instances only
            active_states = {
                "PROVISIONING", "STAGING", "RUNNING", "STOPPING",
                "SUSPENDING", "REPAIRING"
            }
            active_instances = [
                vm for vm in all_instances
                if vm.get("status") in active_states
            ]
            
            number_of_vm_instances = len(active_instances)
            
            logger.info("VM instances | Project={} | active={} | total={}".format(
                project_id, number_of_vm_instances, len(all_instances)
            ))
            
        except Exception as e:
            logger.error("List instances failed", exc_info=True)
            return {
                "status": "FAILED",
                "action": action,
                "reason": "ListInstancesFailed",
                "error": str(e),
                "project_id": project_id,
                "region": region,
            }
        
        # -------------------------------------------------------------------
        # APPLY RULES
        # -------------------------------------------------------------------
        
        # Rule 1: Region validation - check if instances are in allowed regions
        for instance in active_instances:
            name = instance.get("name")
            zone_url = instance.get("zone", "")
            zone = zone_url.split("/")[-1] if zone_url else ""
            
            # Extract region from zone (e.g., us-central1-a -> us-central1)
            if zone and "-" in zone:
                region_name = "-".join(zone.split("-")[:-1])
            else:
                region_name = ""
            
            if region_name and region_name not in allowed_regions:
                abuse_message.append(
                    "Project {} | VM instance {} is in {}, not in allowed regions.".format(
                        project_id, name, region_name
                    )
                )
                logger.warning(
                    "ABUSE DETECTED - Unauthorized region | Project={} | Instance={} | Region={} | Zone={} | Allowed={}".format(
                        project_id, name, region_name, zone, ", ".join(sorted(allowed_regions))
                    )
                )
        
        # Rule 2: max_instances
        if "max_instances" in rules:
            max_instances = rules.get("max_instances")
            if not isinstance(max_instances, int) or max_instances < 0:
                return {
                    "status": "FAILED",
                    "action": action,
                    "reason": "InvalidRuleValue",
                    "detail": "max_instances must be non-negative integer",
                }
            
            if number_of_vm_instances > max_instances:
                abuse_message.append(
                    "Project {} | Created {} VM instances when limit is {}.".format(
                        project_id, number_of_vm_instances, max_instances
                    )
                )
                logger.warning(
                    "ABUSE DETECTED - Instance count exceeded | Project={} | Count={} | Limit={}".format(
                        project_id, number_of_vm_instances, max_instances
                    )
                )
        
        # Rule 3: allowed_machine_types
        if "allowed_machine_types" in rules:
            allowed_machine_types = rules.get("allowed_machine_types")
            if not isinstance(allowed_machine_types, list) or not allowed_machine_types:
                return {
                    "status": "FAILED",
                    "action": action,
                    "reason": "InvalidRuleValue",
                    "detail": "allowed_machine_types must be a non-empty list",
                }
            
            allowed_machine_types_set = set(allowed_machine_types)
            
            for instance in active_instances:
                name = instance.get("name")
                machine_type_url = instance.get("machineType", "")
                machine_type = machine_type_url.split("/")[-1] if machine_type_url else ""
                
                if machine_type and machine_type not in allowed_machine_types_set:
                    abuse_message.append(
                        "Project {} | VM instance {} has machine type {} not in allowed types.".format(
                            project_id, name, machine_type
                        )
                    )
                    logger.warning(
                        "ABUSE DETECTED - Unauthorized machine type | Project={} | Instance={} | MachineType={} | Allowed={}".format(
                            project_id, name, machine_type, ", ".join(sorted(allowed_machine_types_set))
                        )
                    )
        
        # Rule 4: allowed_disk_types
        if "allowed_disk_types" in rules:
            allowed_disk_types = rules.get("allowed_disk_types")
            if not isinstance(allowed_disk_types, list) or not allowed_disk_types:
                return {
                    "status": "FAILED",
                    "action": action,
                    "reason": "InvalidRuleValue",
                    "detail": "allowed_disk_types must be a non-empty list",
                }
            
            allowed_disk_types_set = set(allowed_disk_types)
            
            for instance in active_instances:
                name = instance.get("name")
                disks = instance.get("disks", [])
                
                for disk in disks:
                    disk_type_url = disk.get("type", "")
                    disk_type = disk_type_url.split("/")[-1] if disk_type_url else ""
                    
                    if disk_type and disk_type not in allowed_disk_types_set:
                        abuse_message.append(
                            "Project {} | VM instance {} has disk type {} not allowed.".format(
                                project_id, name, disk_type
                            )
                        )
                        logger.warning(
                            "ABUSE DETECTED - Unauthorized disk type | Project={} | Instance={} | DiskType={} | Allowed={}".format(
                                project_id, name, disk_type, ", ".join(sorted(allowed_disk_types_set))
                            )
                        )
        
        # Rule 5: allowed_disk_size_gb
        if "allowed_disk_size_gb" in rules:
            max_disk_size = rules.get("allowed_disk_size_gb")
            if not isinstance(max_disk_size, int) or max_disk_size <= 0:
                return {
                    "status": "FAILED",
                    "action": action,
                    "reason": "InvalidRuleValue",
                    "detail": "allowed_disk_size_gb must be a positive integer",
                }
            
            for instance in active_instances:
                name = instance.get("name")
                disks = instance.get("disks", [])
                
                for disk in disks:
                    disk_size = disk.get("diskSizeGb")
                    
                    if disk_size:
                        try:
                            disk_size_int = int(disk_size)
                            if disk_size_int > max_disk_size:
                                abuse_message.append(
                                    "Project {} | VM instance {} has disk size {} GB (limit {} GB).".format(
                                        project_id, name, disk_size_int, max_disk_size
                                    )
                                )
                                logger.warning(
                                    "ABUSE DETECTED - Disk size exceeded | Project={} | Instance={} | DiskSize={} GB | Limit={} GB".format(
                                        project_id, name, disk_size_int, max_disk_size
                                    )
                                )
                        except (ValueError, TypeError):
                            pass
        
        # -------------------------------------------------------------------
        # Rule 6: Token Usage Limit Check
        # -------------------------------------------------------------------
        if "token_limit" in rules:
            token_config = rules.get("token_limit")
            if isinstance(token_config, dict):
                time_period = token_config.get("time_period_in_mins")
                allowed_limit = token_config.get("allowed_token_limitation")
                
                if time_period and allowed_limit:
                    try:
                        logger.info("Checking token usage | Period={}min | Limit={}".format(
                            time_period, allowed_limit
                        ))
                        
                        # Calculate time window
                        now = datetime.now(timezone.utc)
                        start = now - timedelta(minutes=time_period)
                        
                        # Query parameters
                        params = {
                            "filter": 'metric.type="aiplatform.googleapis.com/publisher/online_serving/token_count"',
                            "interval.endTime": now.isoformat(),
                            "interval.startTime": start.isoformat(),
                        }
                        
                        # Query Cloud Monitoring API
                        response = monitoring_client.request(
                            "GET",
                            "projects/{}/timeSeries".format(project_id),
                            params=params,
                        )
                        
                        # Aggregate token usage
                        total_input = 0
                        total_output = 0
                        usage_by_model = defaultdict(lambda: {"input": 0, "output": 0})
                        
                        time_series_list = response.get("timeSeries", [])
                        for series in time_series_list:
                            model = series.get("resource", {}).get("labels", {}).get("model_user_id", "unknown")
                            token_type = series.get("metric", {}).get("labels", {}).get("type", "unknown")
                            
                            # Sum all points - convert to int to handle string values
                            points = series.get("points", [])
                            total = 0
                            for point in points:
                                value = point.get("value", {}).get("int64Value", 0)
                                try:
                                    total += int(value) if value else 0
                                except (ValueError, TypeError):
                                    pass
                            
                            if token_type == "input":
                                total_input += total
                                usage_by_model[model]["input"] += total
                            elif token_type == "output":
                                total_output += total
                                usage_by_model[model]["output"] += total
                        
                        total_tokens = total_input + total_output
                        
                        logger.info(
                            "Token usage | Project={} | Period={}min | Total={} | Input={} | Output={} | Limit={}".format(
                                project_id, time_period, total_tokens, total_input, total_output, allowed_limit
                            )
                        )
                        
                        if total_tokens > allowed_limit:
                            abuse_message.append(
                                "Project {} | Token usage {} exceeded limit {} in last {} minutes.".format(
                                    project_id, total_tokens, allowed_limit, time_period
                                )
                            )
                            logger.warning(
                                "ABUSE DETECTED - Token limit exceeded | Project={} | Total={} | Limit={} | Period={}min".format(
                                    project_id, total_tokens, allowed_limit, time_period
                                )
                            )
                    
                    except Exception as e:
                        logger.error("Token usage check failed", exc_info=True)
                        abuse_message.append(
                            "Project {} | Token usage check failed: {}".format(project_id, str(e))
                        )
        
        # -------------------------------------------------------------------
        # Rule 7: Model Restriction Check
        # -------------------------------------------------------------------
        if "model_restriction" in rules:
            model_config = rules.get("model_restriction")
            if isinstance(model_config, dict):
                time_period = model_config.get("time_period_in_mins")
                allowed_models = model_config.get("allowed_model")
                
                if time_period and allowed_models and isinstance(allowed_models, list):
                    try:
                        logger.info("Checking model usage | Period={}min | AllowedModels={}".format(
                            time_period, ", ".join(allowed_models)
                        ))
                        
                        # Calculate time window
                        now = datetime.now(timezone.utc)
                        start = now - timedelta(minutes=time_period)
                        
                        # Query parameters
                        params = {
                            "filter": 'metric.type="aiplatform.googleapis.com/publisher/online_serving/token_count"',
                            "interval.endTime": now.isoformat(),
                            "interval.startTime": start.isoformat(),
                        }
                        
                        # Query Cloud Monitoring API
                        response = monitoring_client.request(
                            "GET",
                            "projects/{}/timeSeries".format(project_id),
                            params=params,
                        )
                        
                        # Collect all used models
                        used_models = set()
                        unauthorized_models = []
                        allowed_models_set = set(allowed_models)
                        
                        time_series_list = response.get("timeSeries", [])
                        for series in time_series_list:
                            model = series.get("resource", {}).get("labels", {}).get("model_user_id", "unknown")
                            if model and model != "unknown":
                                used_models.add(model)
                                if model not in allowed_models_set:
                                    unauthorized_models.append(model)
                        
                        unauthorized_models = list(set(unauthorized_models))  # Remove duplicates
                        
                        logger.info(
                            "Model usage | Project={} | Period={}min | UsedModels={} | Unauthorized={} | Allowed={}".format(
                                project_id, time_period, len(used_models), len(unauthorized_models), len(allowed_models)
                            )
                        )
                        
                        if unauthorized_models:
                            abuse_message.append(
                                "Project {} | Unauthorized models used: {} (allowed: {}) in last {} minutes.".format(
                                    project_id,
                                    ", ".join(unauthorized_models),
                                    ", ".join(allowed_models),
                                    time_period
                                )
                            )
                            logger.warning(
                                "ABUSE DETECTED - Unauthorized models | Project={} | Unauthorized={} | Allowed={} | Period={}min".format(
                                    project_id,
                                    ", ".join(unauthorized_models),
                                    ", ".join(allowed_models),
                                    time_period
                                )
                            )
                    
                    except Exception as e:
                        logger.error("Model usage check failed", exc_info=True)
                        abuse_message.append(
                            "Project {} | Model usage check failed: {}".format(project_id, str(e))
                        )
        
        # -------------------------------------------------------------------
        # RESPONSE DATA
        # -------------------------------------------------------------------
        instance_details = []
        for instance in active_instances:
            zone_url = instance.get("zone", "")
            zone = zone_url.split("/")[-1] if zone_url else ""
            
            if zone and "-" in zone:
                region_name = "-".join(zone.split("-")[:-1])
            else:
                region_name = ""
            
            machine_type_url = instance.get("machineType", "")
            machine_type = machine_type_url.split("/")[-1] if machine_type_url else ""
            
            disks_info = []
            for disk in instance.get("disks", []):
                disk_type_url = disk.get("type", "")
                disk_type = disk_type_url.split("/")[-1] if disk_type_url else ""
                
                disk_size = disk.get("diskSizeGb")
                try:
                    disk_size_int = int(disk_size) if disk_size is not None else None
                except (ValueError, TypeError):
                    disk_size_int = None
                
                disks_info.append({
                    "disk_size_gb": disk_size_int,
                    "disk_type": disk_type,
                    "boot": disk.get("boot", False),
                })
            
            instance_details.append({
                "name": instance.get("name"),
                "machine_type": machine_type,
                "zone": zone,
                "region": region_name,
                "status": instance.get("status"),
                "disks": disks_info,
                "creation_timestamp": instance.get("creationTimestamp"),
            })
        
        response_data = {
            "number_of_vm_instances": number_of_vm_instances,
            "instance_details": instance_details,
        }
        
        # -------------------------------------------------------------------
        # FINAL RESULT
        # -------------------------------------------------------------------
        if abuse_message:
            logger.warning(
                "VM abuse scan FAILED | Project={} | Region={} | Issues={}".format(
                    project_id, region, len(abuse_message)
                )
            )
            return {
                "status": "FAILED",
                "action": action,
                "reason": "ResourceLimitExceeded",
                "project_id": project_id,
                "region": region,
                "abuse_message": abuse_message,
                "data": response_data,
            }
        
        # ------------------------------------------------------------
        # Abuse scan passed
        # ------------------------------------------------------------
        logger.info("VM abuse scan PASSED | Project={} | Region={}".format(
            project_id, region
        ))
        
        return {
            "status": "SUCCESS",
            "action": action,
            "project_id": project_id,
            "region": region,
            "abuse_message": [],
            "data": response_data,
        }
        
    except GCPAPIError as e:
        logger.error("GCP API Error", exc_info=True)
        return {
            "status": "FAILED",
            "action": action,
            "reason": "GCPAPIError",
            "error": str(e),
            "project_id": project_id,
            "region": region,
        }
    
    except Exception as e:
        logger.error("Unhandled Exception", exc_info=True)
        return {
            "status": "FAILED",
            "action": action,
            "reason": "UnhandledException",
            "error": str(e),
            "project_id": project_id,
            "region": region,
        }
