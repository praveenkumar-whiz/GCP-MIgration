import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-loadbalancer-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Load Balancer validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - zone (optional)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    """
    logger.info("GCP Load Balancer Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_LOADBALANCER_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_LOADBALANCER_VALIDATION",
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
        # Collect Load Balancer Components
        # --------------------------------------------------
        all_global_forwarding_rules: List[Dict[str, Any]] = []
        all_regional_forwarding_rules: List[Dict[str, Any]] = []
        page_token = None

        # Collect Global Forwarding Rules
        try:
            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/forwardingRules".format(project_id),
                    params=params,
                )
                all_global_forwarding_rules.extend(response.get("items", []))

                page_token = response.get("nextPageToken")
                if not page_token:
                    break
        except GCPAPIError as e:
            logger.warning("Failed to list global forwarding rules: %s", str(e))

        # Collect Regional Forwarding Rules
        page_token = None
        try:
            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/forwardingRules".format(project_id, region),
                    params=params,
                )
                all_regional_forwarding_rules.extend(response.get("items", []))

                page_token = response.get("nextPageToken")
                if not page_token:
                    break
        except GCPAPIError as e:
            logger.warning("Failed to list regional forwarding rules: %s", str(e))

        total_forwarding_rules = len(all_global_forwarding_rules) + len(all_regional_forwarding_rules)
        logger.info("Found %s GCP Load Balancer forwarding rule(s)", total_forwarding_rules)

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Load Balancer Created
        # ================================================================
        if "loadbalancer_created" in rules:
            try:
                passed = (total_forwarding_rules > 0) == rules["loadbalancer_created"]
                all_results["loadbalancer_created"] = passed
                if not passed:
                    all_failure_reasons["loadbalancer_created"] = (
                        "No Load Balancer found"
                        if rules["loadbalancer_created"]
                        else "Load Balancer exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check loadbalancer_created: %s", e)
                all_results["loadbalancer_created"] = False
                all_failure_reasons["loadbalancer_created"] = "Load Balancer check error: {}".format(e)

        # ================================================================
        # Load Balancer Type
        # ================================================================
        if "loadbalancer_type" in rules:
            try:
                expected_types = rules["loadbalancer_type"]
                if not isinstance(expected_types, list):
                    expected_types = [expected_types]

                # Application LB = HTTP/HTTPS protocols or EXTERNAL_MANAGED scheme
                # Network LB = TCP/UDP protocols
                
                has_application_lb = False
                has_network_lb = False

                # Check global forwarding rules
                for rule in all_global_forwarding_rules:
                    load_balancing_scheme = rule.get("loadBalancingScheme", "")
                    ip_protocol = rule.get("IPProtocol", "")
                    
                    # Application LB indicators
                    if ("EXTERNAL_MANAGED" in load_balancing_scheme or 
                        "INTERNAL_MANAGED" in load_balancing_scheme or
                        ip_protocol in ["HTTP", "HTTPS"]):
                        has_application_lb = True
                    # Network LB indicators
                    elif ip_protocol in ["TCP", "UDP"]:
                        has_network_lb = True

                # Check regional forwarding rules
                for rule in all_regional_forwarding_rules:
                    load_balancing_scheme = rule.get("loadBalancingScheme", "")
                    ip_protocol = rule.get("IPProtocol", "")
                    
                    # Application LB indicators
                    if ("EXTERNAL_MANAGED" in load_balancing_scheme or 
                        "INTERNAL_MANAGED" in load_balancing_scheme or
                        ip_protocol in ["HTTP", "HTTPS"]):
                        has_application_lb = True
                    # Network LB indicators
                    elif ip_protocol in ["TCP", "UDP"]:
                        has_network_lb = True

                type_match = False
                for expected_type in expected_types:
                    if expected_type.lower() == "application" and has_application_lb:
                        type_match = True
                        break
                    elif expected_type.lower() == "network" and has_network_lb:
                        type_match = True
                        break

                all_results["loadbalancer_type"] = type_match
                if not type_match:
                    found_types = []
                    if has_application_lb:
                        found_types.append("application")
                    if has_network_lb:
                        found_types.append("network")
                    
                    all_failure_reasons["loadbalancer_type"] = (
                        "Expected load balancer type(s) {} but found {}".format(
                            expected_types,
                            found_types if found_types else "none"
                        )
                    )
            except Exception as e:
                logger.warning("Failed to check loadbalancer_type: %s", e)
                all_results["loadbalancer_type"] = False
                all_failure_reasons["loadbalancer_type"] = "Load Balancer type check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_LOADBALANCER_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "forwarding_rules_checked": total_forwarding_rules,
                    "global_forwarding_rules": len(all_global_forwarding_rules),
                    "regional_forwarding_rules": len(all_regional_forwarding_rules),
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_LOADBALANCER_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "forwarding_rules_checked": total_forwarding_rules,
                "global_forwarding_rules": len(all_global_forwarding_rules),
                "regional_forwarding_rules": len(all_regional_forwarding_rules),
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Load Balancer validation")
        return {
            "status": "FAILED",
            "action": "GCP_LOADBALANCER_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Load Balancer validation")
        return {
            "status": "FAILED",
            "action": "GCP_LOADBALANCER_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
