import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-apigateway-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP API Gateway Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP API Gateway cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP API Gateway Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "API_GATEWAY_TERMINATION",
            "reason": "MissingRegion",
        }

    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "API_GATEWAY_TERMINATION",
            "reason": "InvalidRegionFormat"
        }

    results = []
    project_id = cleanup_event.get("project_id")

    # Credentials are assumed upstream; use them for all GCP clients in this run
    client_creds = {
        "access_token": (
            cleanup_event.get("gcp_access_token")
            or cleanup_event.get("access_token")
            or cleanup_event.get("google_access_token")
        ),
        "timeout": int(cleanup_event.get("timeout") or 30),
    }

    for region in regions:
        deleted_resources = {
            "deleted_gateways": [],
            "deleted_api_configs": [],
            "deleted_apis": [],
            "deleted_endpoint_services": [],
            "total_gateways": 0,
            "total_api_configs": 0,
            "total_apis": 0,
            "total_endpoint_services": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connection to GCP API Gateway Service
            # ----------------------------------------------------------------
            apigateway_client = GCPClientFactory.create(
                "apigateway",
                creds=client_creds,
            )

            logger.info("Connected to GCP API Gateway service | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP API GATEWAY FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: Delete Gateways
            # ========================================================
            logger.info("=== STEP 1: Discovering API Gateways ===")
            try:
                all_gateways = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = apigateway_client.request(
                        "GET",
                        "projects/{}/locations/{}/gateways".format(project_id, region),
                        params=params,
                    )

                    items = response.get("gateways", [])
                    all_gateways.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_gateways"] = len(all_gateways)
                logger.info("API Gateways discovered | Count=%d", len(all_gateways))

                for gateway in all_gateways:
                    gateway_name = gateway.get("name")
                    if not gateway_name:
                        continue

                    try:
                        delete_response = apigateway_client.request(
                            "DELETE",
                            gateway_name,
                        )
                        
                        # Wait for the operation to complete
                        operation_name = delete_response.get("name")
                        if operation_name:
                            logger.info("Waiting for gateway deletion operation: %s", operation_name)
                            
                            max_wait_time = 60
                            poll_interval = 2
                            elapsed_time = 0
                            operation_done = False
                            operation_error = None
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = apigateway_client.request(
                                        "GET",
                                        operation_name,
                                    )
                                    
                                    if operation_status.get("done"):
                                        operation_done = True
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            operation_error = error_obj.get("message", str(error_obj))
                                        break
                                    
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                except GCPAPIError as poll_error:
                                    logger.warning("Error polling operation status: %s", str(poll_error))
                                    break
                            
                            if not operation_done:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "API Gateway",
                                    "resource_name": gateway_name,
                                    "region": region,
                                    "error": "Operation timed out after {}s".format(max_wait_time)
                                })
                                logger.warning("Gateway deletion operation timed out: %s", gateway_name)
                            elif operation_error:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "API Gateway",
                                    "resource_name": gateway_name,
                                    "region": region,
                                    "error": operation_error
                                })
                                logger.error("API Gateway deletion failed for %s: %s", gateway_name, operation_error)
                            else:
                                deleted_resources["deleted_gateways"].append(gateway_name)
                                logger.info("API Gateway deleted successfully: %s", gateway_name)
                        else:
                            deleted_resources["deleted_gateways"].append(gateway_name)
                            logger.info("API Gateway deleted: %s", gateway_name)
                            
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "API Gateway",
                            "resource_name": gateway_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("API Gateway deletion failed for %s: %s", gateway_name, str(e))

            except GCPAPIError as e:
                logger.error("List API Gateways failed: %s", str(e))
            except Exception as e:
                logger.error("API Gateways processing failed: %s", str(e), exc_info=True)

            # Wait for gateways to be deleted
            if len(deleted_resources["deleted_gateways"]) > 0:
                logger.info("Waiting 10 seconds for gateway deletions to propagate")
                time.sleep(10)

            # ========================================================
            # STEP 2: Delete APIs (with nested API Configs)
            # ========================================================
            logger.info("=== STEP 2: Discovering APIs ===")
            try:
                all_apis = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = apigateway_client.request(
                        "GET",
                        "projects/{}/locations/global/apis".format(project_id),
                        params=params,
                    )

                    items = response.get("apis", [])
                    all_apis.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_apis"] = len(all_apis)
                logger.info("APIs discovered | Count=%d", len(all_apis))

                for api in all_apis:
                    api_name = api.get("name")
                    if not api_name:
                        continue

                    # ========================================================
                    # STEP 2a: Delete API Configs for this API
                    # ========================================================
                    logger.info("=== STEP 2a: Discovering API Configs for API: %s ===", api_name)
                    try:
                        all_api_configs = []
                        config_page_token = None

                        while True:
                            config_params = {}
                            if config_page_token:
                                config_params["pageToken"] = config_page_token

                            config_response = apigateway_client.request(
                                "GET",
                                "{}/configs".format(api_name),
                                params=config_params,
                            )

                            config_items = config_response.get("apiConfigs", [])
                            all_api_configs.extend(config_items)

                            config_page_token = config_response.get("nextPageToken")
                            if not config_page_token:
                                break

                        deleted_resources["total_api_configs"] += len(all_api_configs)
                        
                        if len(all_api_configs) > 0:
                            logger.info("API Configs discovered for API %s | Count=%d", api_name, len(all_api_configs))

                        for api_config in all_api_configs:
                            api_config_name = api_config.get("name")
                            if not api_config_name:
                                continue

                            try:
                                delete_response = apigateway_client.request(
                                    "DELETE",
                                    api_config_name,
                                )
                                
                                # Wait for the operation to complete
                                operation_name = delete_response.get("name")
                                if operation_name:
                                    max_wait_time = 60
                                    poll_interval = 2
                                    elapsed_time = 0
                                    operation_done = False
                                    operation_error = None
                                    
                                    while elapsed_time < max_wait_time:
                                        try:
                                            operation_status = apigateway_client.request(
                                                "GET",
                                                operation_name,
                                            )
                                            
                                            if operation_status.get("done"):
                                                operation_done = True
                                                if "error" in operation_status:
                                                    error_obj = operation_status["error"]
                                                    operation_error = error_obj.get("message", str(error_obj))
                                                break
                                            
                                            time.sleep(poll_interval)
                                            elapsed_time += poll_interval
                                        except GCPAPIError as poll_error:
                                            logger.warning("Error polling operation status: %s", str(poll_error))
                                            break
                                    
                                    if not operation_done:
                                        deleted_resources["failed_deletions"].append({
                                            "resource_type": "API Config",
                                            "resource_name": api_config_name,
                                            "region": region,
                                            "error": "Operation timed out after {}s".format(max_wait_time)
                                        })
                                        logger.warning("API Config deletion operation timed out: %s", api_config_name)
                                    elif operation_error:
                                        deleted_resources["failed_deletions"].append({
                                            "resource_type": "API Config",
                                            "resource_name": api_config_name,
                                            "region": region,
                                            "error": operation_error
                                        })
                                        logger.error("API Config deletion failed for %s: %s", api_config_name, operation_error)
                                    else:
                                        deleted_resources["deleted_api_configs"].append(api_config_name)
                                        logger.info("API Config deleted successfully: %s", api_config_name)
                                else:
                                    deleted_resources["deleted_api_configs"].append(api_config_name)
                                    logger.info("API Config deleted: %s", api_config_name)
                                    
                            except GCPAPIError as e:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "API Config",
                                    "resource_name": api_config_name,
                                    "region": region,
                                    "error": str(e)
                                })
                                logger.warning("API Config deletion failed for %s: %s", api_config_name, str(e))

                    except GCPAPIError as e:
                        logger.error("List API Configs failed for API %s: %s", api_name, str(e))
                    except Exception as e:
                        logger.error("API Configs processing failed for API %s: %s", api_name, str(e), exc_info=True)

                    # Wait for API configs to be deleted before deleting the API
                    if len(all_api_configs) > 0:
                        logger.info("Waiting 10 seconds for API Config deletions to propagate")
                        time.sleep(10)

                    # ========================================================
                    # STEP 2b: Delete the API
                    # ========================================================
                    try:
                        delete_response = apigateway_client.request(
                            "DELETE",
                            api_name,
                        )
                        
                        # Wait for the operation to complete
                        operation_name = delete_response.get("name")
                        if operation_name:
                            logger.info("Waiting for API deletion operation: %s", operation_name)
                            
                            max_wait_time = 60
                            poll_interval = 2
                            elapsed_time = 0
                            operation_done = False
                            operation_error = None
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = apigateway_client.request(
                                        "GET",
                                        operation_name,
                                    )
                                    
                                    if operation_status.get("done"):
                                        operation_done = True
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            operation_error = error_obj.get("message", str(error_obj))
                                        break
                                    
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                except GCPAPIError as poll_error:
                                    logger.warning("Error polling operation status: %s", str(poll_error))
                                    break
                            
                            if not operation_done:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "API",
                                    "resource_name": api_name,
                                    "region": region,
                                    "error": "Operation timed out after {}s".format(max_wait_time)
                                })
                                logger.warning("API deletion operation timed out: %s", api_name)
                            elif operation_error:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "API",
                                    "resource_name": api_name,
                                    "region": region,
                                    "error": operation_error
                                })
                                logger.error("API deletion failed for %s: %s", api_name, operation_error)
                            else:
                                deleted_resources["deleted_apis"].append(api_name)
                                logger.info("API deleted successfully: %s", api_name)
                        else:
                            deleted_resources["deleted_apis"].append(api_name)
                            logger.info("API deleted: %s", api_name)
                            
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "API",
                            "resource_name": api_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("API deletion failed for %s: %s", api_name, str(e))

            except GCPAPIError as e:
                logger.error("List APIs failed: %s", str(e))
            except Exception as e:
                logger.error("APIs processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 3: Delete Endpoint Services (Service Management)
            # ========================================================
            logger.info("=== STEP 3: Discovering Endpoint Services ===")
            try:
                # Create Service Management client
                endpoints_client = GCPClientFactory.create(
                    "endpoints",
                    creds=client_creds,
                )

                all_endpoint_services = []
                page_token = None

                while True:
                    params = {
                        "producerProjectId": project_id
                    }
                    if page_token:
                        params["pageToken"] = page_token

                    response = endpoints_client.request(
                        "GET",
                        "services",
                        params=params,
                    )

                    items = response.get("services", [])
                    all_endpoint_services.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_endpoint_services"] = len(all_endpoint_services)
                logger.info("Endpoint Services discovered | Count=%d", len(all_endpoint_services))

                for service in all_endpoint_services:
                    service_name = service.get("serviceName")
                    if not service_name:
                        continue

                    try:
                        delete_response = endpoints_client.request(
                            "DELETE",
                            "services/{}".format(service_name),
                        )
                        
                        # Wait for the operation to complete
                        operation_name = delete_response.get("name")
                        if operation_name:
                            max_wait_time = 60
                            poll_interval = 2
                            elapsed_time = 0
                            operation_done = False
                            operation_error = None
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = endpoints_client.request(
                                        "GET",
                                        operation_name,
                                    )
                                    
                                    if operation_status.get("done"):
                                        operation_done = True
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            operation_error = error_obj.get("message", str(error_obj))
                                        break
                                    
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                except GCPAPIError as poll_error:
                                    logger.warning("Error polling operation status: %s", str(poll_error))
                                    break
                            
                            if not operation_done:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Endpoint Service",
                                    "resource_name": service_name,
                                    "region": region,
                                    "error": "Operation timed out after {}s".format(max_wait_time)
                                })
                                logger.warning("Endpoint Service deletion operation timed out: %s", service_name)
                            elif operation_error:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Endpoint Service",
                                    "resource_name": service_name,
                                    "region": region,
                                    "error": operation_error
                                })
                                logger.error("Endpoint Service deletion failed for %s: %s", service_name, operation_error)
                            else:
                                deleted_resources["deleted_endpoint_services"].append(service_name)
                                logger.info("Endpoint Service deleted successfully: %s", service_name)
                        else:
                            deleted_resources["deleted_endpoint_services"].append(service_name)
                            logger.info("Endpoint Service deleted: %s", service_name)
                            
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Endpoint Service",
                            "resource_name": service_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Endpoint Service deletion failed for %s: %s", service_name, str(e))

            except GCPAPIError as e:
                logger.error("List Endpoint Services failed: %s", str(e))
            except Exception as e:
                logger.error("Endpoint Services processing failed: %s", str(e), exc_info=True)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP API Gateway cleanup COMPLETED | Project=%s | Region=%s | "
                       "Gateways=%d | APIs=%d | APIConfigs=%d | EndpointServices=%d",
                       project_id, region,
                       len(deleted_resources["deleted_gateways"]),
                       len(deleted_resources["deleted_apis"]),
                       len(deleted_resources["deleted_api_configs"]),
                       len(deleted_resources["deleted_endpoint_services"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "API_GATEWAY_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "API_GATEWAY_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during API Gateway deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "API_GATEWAY_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during API Gateway deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "API_GATEWAY_TERMINATION",
                "reason": "UnhandledException",
                "project_id": project_id,
                "region": region,
                "detail": str(e),
            })

    return {
        "status": "SUCCESS" if results else "FAILED",
        "result": results,
        "logs": []
    }
