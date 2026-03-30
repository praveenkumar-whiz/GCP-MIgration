import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-cloudlogging-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Cloud Logging Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Cloud Logging cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (optional - Cloud Logging is global)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Cloud Logging Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Cloud Logging is global, but we support region for consistency
    region = cleanup_event.get("region_name") or cleanup_event.get("region") or "global"

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
        "deleted_sinks": [],
        "deleted_metrics": [],
        "skipped_sinks": [],
        "total_sinks": 0,
        "total_metrics": 0,
    }

    try:
        # ----------------------------------------------------------------
        # Client connection to GCP Cloud Logging Service
        # ----------------------------------------------------------------
        logging_client = GCPClientFactory.create(
            "logging",
            creds=client_creds,
        )

        logger.info("Connected to GCP Cloud Logging service | Project=%s", project_id)

        logger.info("=== GCP CLOUD LOGGING FULL CLEANUP STARTED | Project=%s ===", project_id)

        # ========================================================
        # STEP 1: Delete Log Sinks (except _Default and _Required)
        # ========================================================
        logger.info("=== STEP 1: Discovering Log Sinks ===")
        try:
            all_sinks = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = logging_client.request(
                    "GET",
                    "projects/{}/sinks".format(project_id),
                    params=params,
                )

                items = response.get("sinks", [])
                all_sinks.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_sinks"] = len(all_sinks)
            logger.info("Log Sinks discovered | Count=%d", len(all_sinks))

            for sink in all_sinks:
                sink_name = sink.get("name")
                if not sink_name:
                    continue

                # Extract the sink ID from the full name (projects/{project}/sinks/{sinkId})
                sink_id = sink_name.split("/")[-1] if "/" in sink_name else sink_name

                # Skip system sinks (_Default and _Required)
                if sink_id in ["_Default", "_Required"]:
                    deleted_resources["skipped_sinks"].append(sink_id)
                    logger.info("Skipping system sink: %s", sink_id)
                    continue

                try:
                    logging_client.request(
                        "DELETE",
                        "projects/{}/sinks/{}".format(project_id, sink_id),
                    )
                    deleted_resources["deleted_sinks"].append(sink_id)
                    logger.info("Log Sink deleted: %s", sink_id)
                except GCPAPIError as e:
                    logger.warning("Log Sink deletion failed for %s: %s", sink_id, str(e))

        except GCPAPIError as e:
            logger.error("List Log Sinks failed: %s", str(e))
        except Exception as e:
            logger.error("Log Sinks processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 2: Delete Log Metrics
        # ========================================================
        logger.info("=== STEP 2: Discovering Log Metrics ===")
        try:
            all_metrics = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = logging_client.request(
                    "GET",
                    "projects/{}/metrics".format(project_id),
                    params=params,
                )

                items = response.get("metrics", [])
                all_metrics.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_metrics"] = len(all_metrics)
            logger.info("Log Metrics discovered | Count=%d", len(all_metrics))

            for metric in all_metrics:
                metric_name = metric.get("name")
                if not metric_name:
                    continue

                # Extract the metric ID from the full name (projects/{project}/metrics/{metricId})
                metric_id = metric_name.split("/")[-1] if "/" in metric_name else metric_name

                try:
                    logging_client.request(
                        "DELETE",
                        "projects/{}/metrics/{}".format(project_id, metric_id),
                    )
                    deleted_resources["deleted_metrics"].append(metric_id)
                    logger.info("Log Metric deleted: %s", metric_id)
                except GCPAPIError as e:
                    logger.warning("Log Metric deletion failed for %s: %s", metric_id, str(e))

        except GCPAPIError as e:
            logger.error("List Log Metrics failed: %s", str(e))
        except Exception as e:
            logger.error("Log Metrics processing failed: %s", str(e), exc_info=True)

        # ------------------------------------------------------------
        # Workflow completed successfully
        # ------------------------------------------------------------
        logger.info("GCP Cloud Logging cleanup COMPLETED | Project=%s | "
                   "Sinks=%d | SkippedSinks=%d | Metrics=%d",
                   project_id,
                   len(deleted_resources["deleted_sinks"]),
                   len(deleted_resources["skipped_sinks"]),
                   len(deleted_resources["deleted_metrics"]))

        results.append({
            "status": "SUCCESS",
            "action": "CLOUD_LOGGING_TERMINATION",
            "project_id": project_id,
            "region": region,
            "data": deleted_resources
        })

    except GCPAPIError as e:
        logger.error("GCP API Error during Cloud Logging deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "CLOUD_LOGGING_TERMINATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(e.message),
            "http_status": e.status_code,
        })
    except Exception as e:
        logger.error("Unhandled exception during Cloud Logging deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "CLOUD_LOGGING_TERMINATION",
            "reason": "UnhandledException",
            "project_id": project_id,
            "region": region,
            "detail": str(e),
        })

    return results[0] if results else {
        "status": "FAILED",
        "action": "CLOUD_LOGGING_TERMINATION",
        "reason": "NoResults"
    }
