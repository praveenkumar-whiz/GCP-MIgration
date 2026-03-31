import json
import logging
import time
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-storagetransfer-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Storage Transfer Service Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Storage Transfer Service cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name / region (optional - Storage Transfer is global)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Storage Transfer Service Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Storage Transfer is global, but we support region for consistency
    region = cleanup_event.get("region_name") or cleanup_event.get("region") or "global"
    
    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "STORAGE_TRANSFER_TERMINATION",
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
            "deleted_transfer_jobs": [],
            "total_transfer_jobs": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connection to GCP Storage Transfer Service
            # ----------------------------------------------------------------
            transfer_client = GCPClientFactory.create(
                "storagetransfer",
                creds=client_creds,
            )

            # Add quota project header required by Storage Transfer API
            transfer_client.session.headers.update({
                "X-Goog-User-Project": project_id
            })

            logger.info("Connected to GCP Storage Transfer service | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP STORAGE TRANSFER SERVICE FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: List and Delete Transfer Jobs
            # ========================================================
            logger.info("=== STEP 1: Discovering Transfer Jobs ===")
            
            try:
                all_transfer_jobs = []
                page_token = None

                # Build filter for project
                filter_json = json.dumps({"project_id": project_id})

                while True:
                    params = {"filter": filter_json}
                    if page_token:
                        params["pageToken"] = page_token

                    response = transfer_client.request(
                        "GET",
                        "transferJobs",
                        params=params,
                    )

                    items = response.get("transferJobs", [])
                    all_transfer_jobs.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_transfer_jobs"] = len(all_transfer_jobs)
                logger.info("Transfer Jobs discovered | Count=%d", len(all_transfer_jobs))

                # Delete all transfer jobs
                for job in all_transfer_jobs:
                    job_name = job.get("name")
                    if not job_name:
                        continue

                    try:
                        # Update job status to DELETED
                        update_body = {
                            "projectId": project_id,
                            "transferJob": {
                                "name": job_name,
                                "status": "DELETED"
                            },
                            "updateTransferJobFieldMask": "status"
                        }

                        transfer_client.request(
                            "PATCH",
                            job_name,
                            json_body=update_body,
                        )
                        deleted_resources["deleted_transfer_jobs"].append(job_name)
                        logger.info("Transfer Job deleted: %s", job_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Transfer Job",
                            "resource_name": job_name,
                            "error": str(e)
                        })
                        logger.warning("Transfer Job deletion failed for %s: %s", job_name, str(e))

            except GCPAPIError as e:
                logger.error("List Transfer Jobs failed: %s", str(e))
            except Exception as e:
                logger.error("Transfer Jobs processing failed: %s", str(e), exc_info=True)

            # Conditional sleep - only if resources were deleted
            if deleted_resources["deleted_transfer_jobs"]:
                logger.info("Waiting for transfer job deletions to complete...")
                time.sleep(3)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Storage Transfer Service cleanup COMPLETED | Project=%s | Region=%s | TransferJobs=%d",
                       project_id, region,
                       len(deleted_resources["deleted_transfer_jobs"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "STORAGE_TRANSFER_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "STORAGE_TRANSFER_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Storage Transfer Service deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "STORAGE_TRANSFER_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Storage Transfer Service deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "STORAGE_TRANSFER_TERMINATION",
                "reason": "UnhandledException",
                "project_id": project_id,
                "region": region,
                "detail": str(e),
            })

    return results
