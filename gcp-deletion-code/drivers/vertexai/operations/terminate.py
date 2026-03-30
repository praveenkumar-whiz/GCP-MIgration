import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-vertexai-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Vertex AI Deletion Function (Workbench + Endpoints)
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Vertex AI cleanup routine.
    Deletes Workbench v2 instances and Endpoints.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name / region (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Vertex AI Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "VERTEX_AI_TERMINATION",
            "reason": "MissingRegion",
        }

    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "VERTEX_AI_TERMINATION",
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
            "deleted_workbench_v2": [],
            "deleted_endpoints": [],
            "total_workbench_v2": 0,
            "total_endpoints": 0,
            "zones_processed": [],
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connections to GCP Services
            # ----------------------------------------------------------------
            notebooks_v2_client = GCPClientFactory.create(
                "notebooks",
                creds=client_creds,
            )
            
            aiplatform_client = GCPClientFactory.create(
                "aiplatform",
                creds=client_creds,
            )

            logger.info("Connected to GCP Vertex AI services | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP VERTEX AI FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # Get all zones in the region for Workbench instances
            # ========================================================
            zones = ["{}-{}".format(region, z) for z in ["a", "b", "c"]]
            
            # ========================================================
            # STEP 1: Delete Managed Workbench Instances (v2)
            # ========================================================
            logger.info("=== STEP 1: Discovering Managed Workbench Instances (v2) ===")
            
            for zone in zones:
                logger.info("Checking zone for v2 instances: %s", zone)
                if zone not in deleted_resources["zones_processed"]:
                    deleted_resources["zones_processed"].append(zone)
                
                try:
                    all_instances = []
                    page_token = None

                    while True:
                        params = {}
                        if page_token:
                            params["pageToken"] = page_token

                        response = notebooks_v2_client.request(
                            "GET",
                            "projects/{}/locations/{}/instances".format(project_id, zone),
                            params=params,
                        )

                        items = response.get("instances", [])
                        all_instances.extend(items)

                        page_token = response.get("nextPageToken")
                        if not page_token:
                            break

                    logger.info("Managed Workbench (v2) discovered in %s | Count=%d", zone, len(all_instances))
                    deleted_resources["total_workbench_v2"] += len(all_instances)

                    for instance in all_instances:
                        instance_name = instance.get("name")
                        if not instance_name:
                            continue

                        try:
                            notebooks_v2_client.request(
                                "DELETE",
                                "{}".format(instance_name),
                            )
                            deleted_resources["deleted_workbench_v2"].append(instance_name)
                            logger.info("Managed Workbench (v2) deleted: %s", instance_name)
                        except GCPAPIError as e:
                            deleted_resources["failed_deletions"].append({
                                "resource_type": "Managed Workbench (v2)",
                                "resource_name": instance_name,
                                "zone": zone,
                                "error": str(e)
                            })
                            logger.warning("Managed Workbench (v2) deletion failed for %s: %s", instance_name, str(e))

                except GCPAPIError as e:
                    if e.status_code == 404:
                        logger.info("Zone %s not found for v2 instances, skipping", zone)
                    elif e.status_code == 403:
                        logger.info("v2 Workbench API not enabled or zone %s not accessible, skipping", zone)
                    else:
                        logger.warning("List Managed Workbench (v2) failed in zone %s: %s", zone, str(e))
                except Exception as e:
                    logger.error("Managed Workbench (v2) processing failed in zone %s: %s", zone, str(e), exc_info=True)

            # ========================================================
            # STEP 2: Delete Vertex AI Endpoints (Global)
            # ========================================================
            logger.info("=== STEP 2: Discovering Vertex AI Endpoints (Global) ===")
            
            # Vertex AI Endpoints are global resources, not regional
            # We only need to check once per project, not per region
            # Skip if we've already processed endpoints for this project
            if region == regions[0]:  # Only process on first region
                try:
                    all_endpoints = []
                    page_token = None

                    while True:
                        params = {}
                        if page_token:
                            params["pageToken"] = page_token

                        # Use "global" as the location for endpoints
                        response = aiplatform_client.request(
                            "GET",
                            "projects/{}/locations/global/endpoints".format(project_id),
                            params=params,
                        )

                        items = response.get("endpoints", [])
                        all_endpoints.extend(items)

                        page_token = response.get("nextPageToken")
                        if not page_token:
                            break

                    deleted_resources["total_endpoints"] = len(all_endpoints)
                    logger.info("Vertex AI Endpoints discovered (Global) | Count=%d", len(all_endpoints))

                    for endpoint in all_endpoints:
                        endpoint_name = endpoint.get("name")
                        if not endpoint_name:
                            continue

                        try:
                            aiplatform_client.request(
                                "DELETE",
                                "{}".format(endpoint_name),
                            )
                            deleted_resources["deleted_endpoints"].append(endpoint_name)
                            logger.info("Vertex AI Endpoint deleted: %s", endpoint_name)
                        except GCPAPIError as e:
                            deleted_resources["failed_deletions"].append({
                                "resource_type": "Vertex AI Endpoint",
                                "resource_name": endpoint_name,
                                "location": "global",
                                "error": str(e)
                            })
                            logger.warning("Vertex AI Endpoint deletion failed for %s: %s", endpoint_name, str(e))

                except GCPAPIError as e:
                    logger.error("List Vertex AI Endpoints failed: %s", str(e))
                except Exception as e:
                    logger.error("Vertex AI Endpoints processing failed: %s", str(e), exc_info=True)
            else:
                logger.info("Skipping Vertex AI Endpoints check (already processed for project)")

            # Conditional sleep - only if resources were deleted
            total_deleted = (len(deleted_resources["deleted_workbench_v2"]) + 
                            len(deleted_resources["deleted_endpoints"]))
            if total_deleted > 0:
                logger.info("Waiting for resource deletions to complete...")
                time.sleep(5)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Vertex AI cleanup COMPLETED | Project=%s | Region=%s | "
                       "Workbench_v2=%d | Endpoints=%d | ZonesProcessed=%d",
                       project_id, region,
                       len(deleted_resources["deleted_workbench_v2"]),
                       len(deleted_resources["deleted_endpoints"]),
                       len(deleted_resources["zones_processed"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "VERTEX_AI_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "VERTEX_AI_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Vertex AI deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "VERTEX_AI_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Vertex AI deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "VERTEX_AI_TERMINATION",
                "reason": "UnhandledException",
                "project_id": project_id,
                "region": region,
                "detail": str(e),
            })

    return results
