import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-deploymentmanager-terminate")
logger.setLevel(logging.INFO)

#Cloud Deployment Manager will reach end of support on March 31, 2026.

# -------------------------------------------------------------------
# GCP Deployment Manager Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Deployment Manager cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (optional - Deployment Manager is global)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Deployment Manager Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Deployment Manager is global
    region = cleanup_event.get("region_name") or cleanup_event.get("region") or "global"
    
    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        regions = ["global"]

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
            "deleted_deployments": [],
            "total_deployments": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connection to GCP Deployment Manager Service
            # ----------------------------------------------------------------
            dm_client = GCPClientFactory.create(
                "deploymentmanager",
                creds=client_creds,
            )

            logger.info("Connected to GCP Deployment Manager service | Project=%s", project_id)

            logger.info("=== GCP DEPLOYMENT MANAGER FULL CLEANUP STARTED | Project=%s ===", project_id)

            # ========================================================
            # STEP 1: Delete Deployments
            # ========================================================
            logger.info("=== STEP 1: Discovering Deployments ===")
            try:
                all_deployments = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = dm_client.request(
                        "GET",
                        "projects/{}/global/deployments".format(project_id),
                        params=params,
                    )

                    items = response.get("deployments", [])
                    all_deployments.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_deployments"] = len(all_deployments)
                logger.info("Deployments discovered | Count=%d", len(all_deployments))

                for deployment in all_deployments:
                    deployment_name = deployment.get("name")
                    if not deployment_name:
                        continue

                    try:
                        dm_client.request(
                            "DELETE",
                            "projects/{}/global/deployments/{}".format(project_id, deployment_name),
                        )
                        deleted_resources["deleted_deployments"].append(deployment_name)
                        logger.info("Deployment deleted: %s", deployment_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Deployment",
                            "resource_name": deployment_name,
                            "error": str(e)
                        })
                        logger.warning("Deployment deletion failed for %s: %s", deployment_name, str(e))

                # Conditional sleep - only if resources were deleted
                if deleted_resources["deleted_deployments"]:
                    logger.info("Waiting for deployment deletions to complete...")
                    time.sleep(3)

            except GCPAPIError as e:
                logger.error("List Deployments failed: %s", str(e))
            except Exception as e:
                logger.error("Deployments processing failed: %s", str(e), exc_info=True)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Deployment Manager cleanup COMPLETED | Project=%s | Deployments=%d",
                       project_id,
                       len(deleted_resources["deleted_deployments"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "DEPLOYMENT_MANAGER_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "DEPLOYMENT_MANAGER_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Deployment Manager deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "DEPLOYMENT_MANAGER_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Deployment Manager deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "DEPLOYMENT_MANAGER_TERMINATION",
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
