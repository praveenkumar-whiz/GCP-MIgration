import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-cloudnat-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Cloud NAT validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - zone (optional)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    """
    logger.info("GCP Cloud NAT Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDNAT_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDNAT_VALIDATION",
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

        compute_client = GCPClientFactory.create(
            "compute",
            creds=client_creds,
        )

        logger.info(
            "Connected to GCP Compute Engine | Project=%s | Region=%s",
            project_id, region
        )

        # --------------------------------------------------
        # Collect Cloud Routers (Cloud NAT is managed through routers)
        # Query: projects/{project}/regions/{region}/routers
        # --------------------------------------------------
        all_routers: List[Dict[str, Any]] = []
        page_token = None

        # Query specific region only
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token

            response = compute_client.request(
                "GET",
                "projects/{}/regions/{}/routers".format(project_id, region),
                params=params,
            )
            all_routers.extend(response.get("items", []))

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        logger.info("Found %s GCP Cloud Router(s)", len(all_routers))

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Cloud NAT Created
        # ================================================================
        if "cloudnat_created" in rules:
            try:
                passed = (len(all_routers) > 0) == rules["cloudnat_created"]
                all_results["cloudnat_created"] = passed
                if not passed:
                    all_failure_reasons["cloudnat_created"] = (
                        "No Cloud NAT router found"
                        if rules["cloudnat_created"]
                        else "Cloud NAT router exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check cloudnat_created: %s", e)
                all_results["cloudnat_created"] = False
                all_failure_reasons["cloudnat_created"] = "Cloud NAT check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_CLOUDNAT_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "routers_checked": len(all_routers),
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDNAT_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "routers_checked": len(all_routers),
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Cloud NAT validation")
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDNAT_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Cloud NAT validation")
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDNAT_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
