import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-monitoring-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Monitoring validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - zone (optional)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    """
    logger.info("GCP Monitoring Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_MONITORING_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_MONITORING_VALIDATION",
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

        monitoring_client = GCPClientFactory.create(
            "monitoring",
            creds=client_creds,
        )

        logger.info(
            "Connected to GCP Monitoring | Project=%s | Region=%s",
            project_id, region
        )

        # --------------------------------------------------
        # Collect Monitoring Dashboards
        # Dashboards API uses v1 endpoint (different from v3)
        # Query: ../v1/projects/{project}/dashboards
        # --------------------------------------------------
        all_dashboards: List[Dict[str, Any]] = []
        page_token = None

        # Query dashboards using v1 API
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token

            response = monitoring_client.request(
                "GET",
                "../v1/projects/{}/dashboards".format(project_id),
                params=params,
            )
            all_dashboards.extend(response.get("dashboards", []))

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        logger.info("Found %s GCP Monitoring dashboard(s)", len(all_dashboards))

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Monitoring Dashboard Created
        # ================================================================
        if "monitoring_dashboard_created" in rules:
            try:
                passed = (len(all_dashboards) > 0) == rules["monitoring_dashboard_created"]
                all_results["monitoring_dashboard_created"] = passed
                if not passed:
                    all_failure_reasons["monitoring_dashboard_created"] = (
                        "No Monitoring dashboard found"
                        if rules["monitoring_dashboard_created"]
                        else "Monitoring dashboard exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check monitoring_dashboard_created: %s", e)
                all_results["monitoring_dashboard_created"] = False
                all_failure_reasons["monitoring_dashboard_created"] = "Monitoring dashboard check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_MONITORING_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "dashboards_checked": len(all_dashboards),
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_MONITORING_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "dashboards_checked": len(all_dashboards),
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Monitoring validation")
        return {
            "status": "FAILED",
            "action": "GCP_MONITORING_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Monitoring validation")
        return {
            "status": "FAILED",
            "action": "GCP_MONITORING_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
