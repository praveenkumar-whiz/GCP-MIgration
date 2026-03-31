import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-eventarc-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Eventarc validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - zone (optional)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    """
    logger.info("GCP Eventarc Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_EVENTARC_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_EVENTARC_VALIDATION",
            "reason": "RegionRequired",
        }

    try:
        client_creds = {
            "access_token": (
                spec.get("gcp_access_token")
                or spec.get("access_token")
                or spec.get("google_access_token")
            ),
            "timeout": int(spec.get("timeout") or 30),
        }

        eventarc_client = GCPClientFactory.create(
            "eventarc",
            creds=client_creds,
        )

        logger.info(
            "Connected to GCP Eventarc | Project=%s | Region=%s",
            project_id, region
        )

        # --------------------------------------------------
        # Collect Eventarc Triggers
        # Query: projects/{project}/locations/{region}/triggers
        # --------------------------------------------------
        all_triggers: List[Dict[str, Any]] = []
        page_token = None

        while True:
            params = {"pageSize": 500}
            if page_token:
                params["pageToken"] = page_token

            response = eventarc_client.request(
                "GET",
                "projects/{}/locations/{}/triggers".format(project_id, region),
                params=params,
            )
            all_triggers.extend(response.get("triggers", []))

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        logger.info("Found %s GCP Eventarc trigger(s)", len(all_triggers))

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Eventarc Trigger Created
        # ================================================================
        if "eventarc_trigger_created" in rules:
            try:
                passed = (len(all_triggers) > 0) == rules["eventarc_trigger_created"]
                all_results["eventarc_trigger_created"] = passed
                if not passed:
                    all_failure_reasons["eventarc_trigger_created"] = (
                        "No Eventarc trigger found"
                        if rules["eventarc_trigger_created"]
                        else "Eventarc trigger exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check eventarc_trigger_created: %s", e)
                all_results["eventarc_trigger_created"] = False
                all_failure_reasons["eventarc_trigger_created"] = "Eventarc trigger check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_EVENTARC_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "triggers_checked": len(all_triggers),
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_EVENTARC_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "triggers_checked": len(all_triggers),
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Eventarc validation")
        return {
            "status": "FAILED",
            "action": "GCP_EVENTARC_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Eventarc validation")
        return {
            "status": "FAILED",
            "action": "GCP_EVENTARC_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
