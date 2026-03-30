import json
import logging
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-batch-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Batch Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Batch cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Batch Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "BATCH_TERMINATION",
            "reason": "MissingRegion",
        }

    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "BATCH_TERMINATION",
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
            "deleted_jobs": [],
            "total_jobs": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connection to GCP Batch Service
            # ----------------------------------------------------------------
            batch_client = GCPClientFactory.create(
                "batch",
                creds=client_creds,
            )

            logger.info("Connected to GCP Batch services | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP BATCH FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: Delete Batch Jobs
            # ========================================================
            logger.info("=== STEP 1: Discovering Batch Jobs ===")
            try:
                all_jobs = []
                page_token = None

                # Batch uses "locations" instead of "regions"
                parent = "projects/{}/locations/{}".format(project_id, region)

                while True:
                    params = {
                        "pageSize": 100
                    }
                    if page_token:
                        params["pageToken"] = page_token

                    response = batch_client.request(
                        "GET",
                        "{}/jobs".format(parent),
                        params=params,
                    )

                    items = response.get("jobs", [])
                    all_jobs.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_jobs"] = len(all_jobs)
                logger.info("Batch Jobs discovered | Count=%d", len(all_jobs))

                for job in all_jobs:
                    job_name = job.get("name")
                    if not job_name:
                        continue

                    try:
                        delete_response = batch_client.request(
                            "DELETE",
                            job_name,
                        )
                        
                        # Wait for the operation to complete
                        operation_name = delete_response.get("name")
                        if operation_name:
                            logger.info("Waiting for batch job deletion operation: %s", operation_name)
                            
                            max_wait_time = 60
                            poll_interval = 2
                            elapsed_time = 0
                            operation_done = False
                            operation_error = None
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = batch_client.request(
                                        "GET",
                                        operation_name,
                                    )
                                    
                                    if operation_status.get("done"):
                                        operation_done = True
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            operation_error = error_obj.get("message", str(error_obj))
                                        break
                                    
                                    import time
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                except GCPAPIError as poll_error:
                                    logger.warning("Error polling operation status: %s", str(poll_error))
                                    break
                            
                            if not operation_done:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Batch Job",
                                    "resource_name": job_name,
                                    "region": region,
                                    "error": "Operation timed out after {}s".format(max_wait_time)
                                })
                                logger.warning("Batch Job deletion operation timed out: %s", job_name)
                            elif operation_error:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Batch Job",
                                    "resource_name": job_name,
                                    "region": region,
                                    "error": operation_error
                                })
                                logger.error("Batch Job deletion failed for %s: %s", job_name, operation_error)
                            else:
                                deleted_resources["deleted_jobs"].append(job_name)
                                logger.info("Batch Job deleted successfully: %s", job_name)
                        else:
                            deleted_resources["deleted_jobs"].append(job_name)
                            logger.info("Batch Job deleted: %s", job_name)
                            
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Batch Job",
                            "resource_name": job_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Batch Job deletion failed for %s: %s", job_name, str(e))

            except GCPAPIError as e:
                logger.error("List Batch Jobs failed: %s", str(e))
            except Exception as e:
                logger.error("Batch Jobs processing failed: %s", str(e), exc_info=True)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Batch cleanup COMPLETED | Project=%s | Region=%s | Jobs=%d",
                       project_id, region,
                       len(deleted_resources["deleted_jobs"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "BATCH_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "BATCH_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Batch deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "BATCH_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Batch deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "BATCH_TERMINATION",
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
