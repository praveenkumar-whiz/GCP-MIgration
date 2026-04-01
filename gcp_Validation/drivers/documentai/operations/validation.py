import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-documentai-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Document AI validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - zone (optional)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    
    Note: Currently supports US-based regions only
    """
    logger.info("GCP Document AI Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_DOCUMENTAI_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_DOCUMENTAI_VALIDATION",
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

        documentai_client = GCPClientFactory.create(
            "documentai",
            creds=client_creds,
        )

        # Override endpoint for US regions
        location = "us"
        if region.startswith("us-"):
            documentai_client.base_url = "https://us-documentai.googleapis.com/v1"

        logger.info(
            "Connected to GCP Document AI | Project=%s | Region=%s | Location=%s",
            project_id, region, location
        )

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Document Processor Created
        # ================================================================
        if "document_processor_created" in rules:
            try:
                # Collect Document AI Processors only when needed
                # Query: projects/{project}/locations/{location}/processors
                all_processors: List[Dict[str, Any]] = []
                page_token = None

                # Query specific location only
                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = documentai_client.request(
                        "GET",
                        "projects/{}/locations/{}/processors".format(project_id, location),
                        params=params,
                    )
                    all_processors.extend(response.get("processors", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s GCP Document AI processor(s)", len(all_processors))

                passed = (len(all_processors) > 0) == rules["document_processor_created"]
                all_results["document_processor_created"] = passed
                if not passed:
                    all_failure_reasons["document_processor_created"] = (
                        "No Document AI processor found"
                        if rules["document_processor_created"]
                        else "Document AI processor exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check document_processor_created: %s", e)
                all_results["document_processor_created"] = False
                all_failure_reasons["document_processor_created"] = "Document processor check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_DOCUMENTAI_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_DOCUMENTAI_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Document AI validation")
        return {
            "status": "FAILED",
            "action": "GCP_DOCUMENTAI_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Document AI validation")
        return {
            "status": "FAILED",
            "action": "GCP_DOCUMENTAI_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
