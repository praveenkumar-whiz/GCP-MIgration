import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-vpc-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP VPC Network validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - zone (optional)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    """
    logger.info("GCP VPC Network Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_VPC_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_VPC_VALIDATION",
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
        # Collect VPC Networks (excluding default)
        # Query: projects/{project}/global/networks
        # --------------------------------------------------
        all_networks: List[Dict[str, Any]] = []
        page_token = None

        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token

            response = compute_client.request(
                "GET",
                "projects/{}/global/networks".format(project_id),
                params=params,
            )
            items = response.get("items", [])
            # Filter out default network
            for item in items:
                if item.get("name") != "default":
                    all_networks.append(item)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        logger.info("Found %s GCP VPC Network(s) (excluding default)", len(all_networks))

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # VPC Network Created
        # ================================================================
        if "vpc_network_created" in rules:
            try:
                passed = (len(all_networks) > 0) == rules["vpc_network_created"]
                all_results["vpc_network_created"] = passed
                if not passed:
                    all_failure_reasons["vpc_network_created"] = (
                        "No VPC network found (excluding default)"
                        if rules["vpc_network_created"]
                        else "VPC network exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check vpc_network_created: %s", e)
                all_results["vpc_network_created"] = False
                all_failure_reasons["vpc_network_created"] = "VPC network check error: {}".format(e)

        # ================================================================
        # Subnet Region
        # ================================================================
        if "subnet_region" in rules:
            try:
                expected_region = rules["subnet_region"]
                
                # Collect Subnets only when needed
                all_subnets: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/subnetworks".format(project_id, expected_region),
                        params=params,
                    )
                    items = response.get("items", [])
                    # Filter out default subnet
                    for item in items:
                        if item.get("name") != "default":
                            all_subnets.append(item)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s GCP Subnet(s) in region %s (excluding default)", len(all_subnets), expected_region)

                # Check if any subnet exists in the specified region
                subnet_found = len(all_subnets) > 0
                
                all_results["subnet_region"] = subnet_found
                if not subnet_found:
                    all_failure_reasons["subnet_region"] = (
                        "No subnet found in region {} (excluding default)".format(expected_region)
                    )
            except Exception as e:
                logger.warning("Failed to check subnet_region: %s", e)
                all_results["subnet_region"] = False
                all_failure_reasons["subnet_region"] = "Subnet region check error: {}".format(e)

        # ================================================================
        # VPC Network Peering Created
        # ================================================================
        if "vpc_network_peering_created" in rules:
            try:
                # Collect VPC Network Peerings only when needed
                all_peerings: List[Dict[str, Any]] = []
                for network in all_networks:
                    peerings = network.get("peerings", [])
                    all_peerings.extend(peerings)

                logger.info("Found %s GCP VPC Network Peering(s)", len(all_peerings))

                passed = (len(all_peerings) > 0) == rules["vpc_network_peering_created"]
                all_results["vpc_network_peering_created"] = passed
                if not passed:
                    all_failure_reasons["vpc_network_peering_created"] = (
                        "No VPC network peering found"
                        if rules["vpc_network_peering_created"]
                        else "VPC network peering exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check vpc_network_peering_created: %s", e)
                all_results["vpc_network_peering_created"] = False
                all_failure_reasons["vpc_network_peering_created"] = "VPC network peering check error: {}".format(e)

        # ================================================================
        # Firewall Rule Created
        # ================================================================
        if "firewall_rule_created" in rules:
            try:
                # Collect Firewall Rules only when needed
                all_firewalls: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/firewalls".format(project_id),
                        params=params,
                    )
                    items = response.get("items", [])
                    # Filter out default firewall rules
                    for item in items:
                        name = item.get("name", "")
                        if not name.startswith("default-"):
                            all_firewalls.append(item)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s GCP Firewall Rule(s) (excluding default)", len(all_firewalls))

                passed = (len(all_firewalls) > 0) == rules["firewall_rule_created"]
                all_results["firewall_rule_created"] = passed
                if not passed:
                    all_failure_reasons["firewall_rule_created"] = (
                        "No firewall rule found (excluding default)"
                        if rules["firewall_rule_created"]
                        else "Firewall rule exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check firewall_rule_created: %s", e)
                all_results["firewall_rule_created"] = False
                all_failure_reasons["firewall_rule_created"] = "Firewall rule check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_VPC_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "networks_checked": len(all_networks),
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_VPC_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "networks_checked": len(all_networks),
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during VPC validation")
        return {
            "status": "FAILED",
            "action": "GCP_VPC_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP VPC validation")
        return {
            "status": "FAILED",
            "action": "GCP_VPC_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
