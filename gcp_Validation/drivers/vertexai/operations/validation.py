import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-vertexai-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Vertex AI Workbench validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - zone (optional)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    """
    logger.info("GCP Vertex AI Workbench Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_VERTEXAI_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_VERTEXAI_VALIDATION",
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

        notebooks_client = GCPClientFactory.create(
            "notebooks",
            creds=client_creds,
        )

        logger.info(
            "Connected to GCP Vertex AI Workbench | Project=%s | Region=%s",
            project_id, region
        )

        # --------------------------------------------------
        # Collect Vertex AI Workbench Instances
        # Query: projects/{project}/locations/{region}/instances
        # --------------------------------------------------
        all_instances: List[Dict[str, Any]] = []
        page_token = None

        # Query specific region only
        while True:
            params = {"pageSize": 500}
            if page_token:
                params["pageToken"] = page_token

            response = notebooks_client.request(
                "GET",
                "projects/{}/locations/{}/instances".format(project_id, region),
                params=params,
            )
            all_instances.extend(response.get("instances", []))

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        logger.info("Found %s GCP Vertex AI Workbench instance(s)", len(all_instances))

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Vertex AI Workbench Created
        # ================================================================
        if "vertex_ai_workbench_created" in rules:
            try:
                passed = (len(all_instances) > 0) == rules["vertex_ai_workbench_created"]
                all_results["vertex_ai_workbench_created"] = passed
                if not passed:
                    all_failure_reasons["vertex_ai_workbench_created"] = (
                        "No Vertex AI Workbench instance found"
                        if rules["vertex_ai_workbench_created"]
                        else "Vertex AI Workbench instance exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check vertex_ai_workbench_created: %s", e)
                all_results["vertex_ai_workbench_created"] = False
                all_failure_reasons["vertex_ai_workbench_created"] = "Vertex AI Workbench check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_VERTEXAI_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "instances_checked": len(all_instances),
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_VERTEXAI_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "instances_checked": len(all_instances),
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Vertex AI Workbench validation")
        return {
            "status": "FAILED",
            "action": "GCP_VERTEXAI_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Vertex AI Workbench validation")
        return {
            "status": "FAILED",
            "action": "GCP_VERTEXAI_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
