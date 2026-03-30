import json
import logging
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-cloudscheduler-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Cloud Scheduler Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Cloud Scheduler cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Cloud Scheduler Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "CLOUD_SCHEDULER_TERMINATION",
            "reason": "MissingRegion",
        }

    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "CLOUD_SCHEDULER_TERMINATION",
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
            # Client connection to GCP Cloud Scheduler Service
            # ----------------------------------------------------------------
            scheduler_client = GCPClientFactory.create(
                "cloudscheduler",
                creds=client_creds,
            )

            logger.info("Connected to GCP Cloud Scheduler services | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP CLOUD SCHEDULER FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: Delete Cloud Scheduler Jobs
            # ========================================================
            logger.info("=== STEP 1: Discovering Cloud Scheduler Jobs ===")
            try:
                all_jobs = []
                page_token = None

                # Cloud Scheduler uses "locations" instead of "regions"
                # The location format is: projects/{project}/locations/{location}
                parent = "projects/{}/locations/{}".format(project_id, region)

                while True:
                    params = {
                        "pageSize": 100
                    }
                    if page_token:
                        params["pageToken"] = page_token

                    response = scheduler_client.request(
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
                logger.info("Cloud Scheduler Jobs discovered | Count=%d", len(all_jobs))

                for job in all_jobs:
                    job_name = job.get("name")
                    if not job_name:
                        continue

                    try:
                        scheduler_client.request(
                            "DELETE",
                            job_name,
                        )
                        deleted_resources["deleted_jobs"].append(job_name)
                        logger.info("Cloud Scheduler Job deleted: %s", job_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Cloud Scheduler Job",
                            "resource_name": job_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Cloud Scheduler Job deletion failed for %s: %s", job_name, str(e))

            except GCPAPIError as e:
                logger.error("List Cloud Scheduler Jobs failed: %s", str(e))
            except Exception as e:
                logger.error("Cloud Scheduler Jobs processing failed: %s", str(e), exc_info=True)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Cloud Scheduler cleanup COMPLETED | Project=%s | Region=%s | "
                       "Jobs=%d",
                       project_id, region,
                       len(deleted_resources["deleted_jobs"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "CLOUD_SCHEDULER_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "CLOUD_SCHEDULER_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Cloud Scheduler deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "CLOUD_SCHEDULER_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Cloud Scheduler deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "CLOUD_SCHEDULER_TERMINATION",
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
