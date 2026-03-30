import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-cloudnat-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Cloud NAT Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Cloud NAT cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.
    
    Cloud NAT is managed through Cloud Routers, so this function deletes routers
    which will also delete any NAT configurations associated with them.

    Expected spec keys:
    - project_id (required)
    - region_name (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Cloud NAT Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "CLOUD_NAT_TERMINATION",
            "reason": "MissingRegion",
        }

    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "CLOUD_NAT_TERMINATION",
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
            "deleted_routers": [],
            "total_routers": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connection to GCP Compute Engine Service
            # ----------------------------------------------------------------
            compute_client = GCPClientFactory.create(
                "compute",
                creds=client_creds,
            )

            logger.info("Connected to GCP Compute Engine service | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP CLOUD NAT FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: Delete Cloud Routers (which includes NAT configs)
            # ========================================================
            logger.info("=== STEP 1: Discovering Cloud Routers ===")
            try:
                all_routers = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/routers".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_routers.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_routers"] = len(all_routers)
                logger.info("Cloud Routers discovered | Count=%d", len(all_routers))

                for router in all_routers:
                    router_name = router.get("name")
                    if not router_name:
                        continue

                    try:
                        delete_response = compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/routers/{}".format(project_id, region, router_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        
                        # Wait for the operation to complete
                        operation_name = delete_response.get("name")
                        if operation_name:
                            logger.info("Waiting for router deletion operation: %s", operation_name)
                            
                            max_wait_time = 60
                            poll_interval = 2
                            elapsed_time = 0
                            operation_done = False
                            operation_error = None
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = compute_client.request(
                                        "GET",
                                        "projects/{}/regions/{}/operations/{}".format(project_id, region, operation_name),
                                    )
                                    
                                    status = operation_status.get("status")
                                    if status == "DONE":
                                        operation_done = True
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            errors = error_obj.get("errors", [])
                                            if errors:
                                                error_messages = [err.get("message", str(err)) for err in errors]
                                                operation_error = "; ".join(error_messages)
                                        break
                                    
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                except GCPAPIError as poll_error:
                                    logger.warning("Error polling operation status: %s", str(poll_error))
                                    break
                            
                            if not operation_done:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Cloud Router",
                                    "resource_name": router_name,
                                    "region": region,
                                    "error": "Operation timed out after {}s".format(max_wait_time)
                                })
                                logger.warning("Router deletion operation timed out: %s", router_name)
                            elif operation_error:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Cloud Router",
                                    "resource_name": router_name,
                                    "region": region,
                                    "error": operation_error
                                })
                                logger.error("Cloud Router deletion failed for %s: %s", router_name, operation_error)
                            else:
                                deleted_resources["deleted_routers"].append(router_name)
                                logger.info("Cloud Router deleted successfully: %s", router_name)
                        else:
                            deleted_resources["deleted_routers"].append(router_name)
                            logger.info("Cloud Router deleted: %s", router_name)
                            
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Cloud Router",
                            "resource_name": router_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Cloud Router deletion failed for %s: %s", router_name, str(e))

            except GCPAPIError as e:
                logger.error("List Cloud Routers failed: %s", str(e))
            except Exception as e:
                logger.error("Cloud Routers processing failed: %s", str(e), exc_info=True)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Cloud NAT cleanup COMPLETED | Project=%s | Region=%s | Routers=%d",
                       project_id, region,
                       len(deleted_resources["deleted_routers"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "CLOUD_NAT_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "CLOUD_NAT_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Cloud NAT deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "CLOUD_NAT_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Cloud NAT deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "CLOUD_NAT_TERMINATION",
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
