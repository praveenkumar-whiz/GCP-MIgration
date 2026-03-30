import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-cloudtasks-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Cloud Tasks Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Cloud Tasks cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name / region (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Cloud Tasks Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "CLOUD_TASKS_TERMINATION",
            "reason": "MissingRegion",
            "detail": "region_name or region is required"
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

    deleted_resources = {
        "deleted_queues": [],
        "total_queues": 0,
    }

    try:
        # ----------------------------------------------------------------
        # Client connection to GCP Cloud Tasks Service
        # ----------------------------------------------------------------
        tasks_client = GCPClientFactory.create(
            "cloudtasks",
            creds=client_creds,
        )

        logger.info("Connected to GCP Cloud Tasks service | Project=%s | Region=%s", project_id, region)

        logger.info("=== GCP CLOUD TASKS FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

        # ========================================================
        # STEP 1: Delete Task Queues
        # ========================================================
        logger.info("=== STEP 1: Discovering Task Queues ===")
        try:
            all_queues = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = tasks_client.request(
                    "GET",
                    "projects/{}/locations/{}/queues".format(project_id, region),
                    params=params,
                )

                items = response.get("queues", [])
                all_queues.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_queues"] = len(all_queues)
            logger.info("Task Queues discovered | Count=%d", len(all_queues))

            for queue in all_queues:
                queue_name = queue.get("name")
                if not queue_name:
                    continue

                try:
                    tasks_client.request(
                        "DELETE",
                        "{}".format(queue_name),
                    )
                    deleted_resources["deleted_queues"].append(queue_name)
                    logger.info("Task Queue deleted: %s", queue_name)
                except GCPAPIError as e:
                    logger.warning("Task Queue deletion failed for %s: %s", queue_name, str(e))

            # Conditional sleep - only if resources were deleted
            if deleted_resources["deleted_queues"]:
                logger.info("Waiting for queue deletions to complete...")
                time.sleep(2)

        except GCPAPIError as e:
            logger.error("List Task Queues failed: %s", str(e))
        except Exception as e:
            logger.error("Task Queues processing failed: %s", str(e), exc_info=True)

        # ------------------------------------------------------------
        # Workflow completed successfully
        # ------------------------------------------------------------
        logger.info("GCP Cloud Tasks cleanup COMPLETED | Project=%s | Region=%s | Queues=%d",
                   project_id,
                   region,
                   len(deleted_resources["deleted_queues"]))

        results.append({
            "status": "SUCCESS",
            "action": "CLOUD_TASKS_TERMINATION",
            "project_id": project_id,
            "region": region,
            "data": deleted_resources
        })

    except GCPAPIError as e:
        logger.error("GCP API Error during Cloud Tasks deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "CLOUD_TASKS_TERMINATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(e.message),
            "http_status": e.status_code,
        })
    except Exception as e:
        logger.error("Unhandled exception during Cloud Tasks deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "CLOUD_TASKS_TERMINATION",
            "reason": "UnhandledException",
            "project_id": project_id,
            "region": region,
            "detail": str(e),
        })

    return results[0] if results else {
        "status": "FAILED",
        "action": "CLOUD_TASKS_TERMINATION",
        "reason": "NoResults"
    }
