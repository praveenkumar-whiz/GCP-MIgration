import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-loadbalancer-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Load Balancer Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Load Balancer cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Load Balancer Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "LOAD_BALANCER_TERMINATION",
            "reason": "MissingRegion",
        }

    # Ensure region is a string
    if not isinstance(region, str):
        return {
            "status": "FAILED",
            "action": "LOAD_BALANCER_TERMINATION",
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

    deleted_resources = {
        "deleted_global_forwarding_rules": [],
        "deleted_regional_forwarding_rules": [],
        "deleted_global_target_http_proxies": [],
        "deleted_regional_target_http_proxies": [],
        "deleted_global_target_https_proxies": [],
        "deleted_regional_target_https_proxies": [],
        "deleted_global_target_tcp_proxies": [],
        "deleted_regional_target_tcp_proxies": [],
        "deleted_global_url_maps": [],
        "deleted_regional_url_maps": [],
        "deleted_global_backend_services": [],
        "deleted_regional_backend_services": [],
        "deleted_backend_buckets": [],
        "deleted_global_health_checks": [],
        "deleted_regional_health_checks": [],
        "deleted_instance_groups": [],
        "deleted_instance_group_managers": [],
        "deleted_ssl_certificates": [],
        "deleted_firewall_rules": [],
        "deleted_addresses": [],
        "total_global_forwarding_rules": 0,
        "total_regional_forwarding_rules": 0,
        "total_global_target_http_proxies": 0,
        "total_regional_target_http_proxies": 0,
        "total_global_target_https_proxies": 0,
        "total_regional_target_https_proxies": 0,
        "total_global_target_tcp_proxies": 0,
        "total_regional_target_tcp_proxies": 0,
        "total_global_url_maps": 0,
        "total_regional_url_maps": 0,
        "total_global_backend_services": 0,
        "total_regional_backend_services": 0,
        "total_backend_buckets": 0,
        "total_global_health_checks": 0,
        "total_regional_health_checks": 0,
        "total_instance_groups": 0,
        "total_instance_group_managers": 0,
        "total_ssl_certificates": 0,
        "total_firewall_rules": 0,
        "total_addresses": 0,
    }

    try:
        # ----------------------------------------------------------------
        # Client connection to GCP Compute Engine Service
        # ----------------------------------------------------------------
        compute_client = GCPClientFactory.create(
            "compute",
            creds=client_creds,
        )

        logger.info("Connected to GCP Compute Engine services | Project=%s | Region=%s", project_id, region)

        logger.info("=== GCP LOAD BALANCER FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

        # ========================================================
        # STEP 1: Delete Global Forwarding Rules
        # ========================================================
        logger.info("=== STEP 1: Discovering Global Forwarding Rules ===")
        try:
            all_global_forwarding_rules = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/forwardingRules".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_global_forwarding_rules.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_global_forwarding_rules"] = len(all_global_forwarding_rules)
            logger.info("Global Forwarding Rules discovered | Count=%d", len(all_global_forwarding_rules))

            for forwarding_rule in all_global_forwarding_rules:
                forwarding_rule_name = forwarding_rule.get("name")
                if not forwarding_rule_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/forwardingRules/{}".format(project_id, forwarding_rule_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_global_forwarding_rules"].append(forwarding_rule_name)
                    logger.info("Global Forwarding Rule deleted: %s", forwarding_rule_name)
                except GCPAPIError as e:
                    logger.warning("Global Forwarding Rule deletion failed for %s: %s", forwarding_rule_name, str(e))

        except GCPAPIError as e:
            logger.error("List Global Forwarding Rules failed: %s", str(e))
        except Exception as e:
            logger.error("Global Forwarding Rules processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 2: Delete Regional Forwarding Rules
        # ========================================================
        logger.info("=== STEP 2: Discovering Regional Forwarding Rules ===")
        try:
            all_regional_forwarding_rules = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/forwardingRules".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_regional_forwarding_rules.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_regional_forwarding_rules"] = len(all_regional_forwarding_rules)
            logger.info("Regional Forwarding Rules discovered | Count=%d", len(all_regional_forwarding_rules))

            for forwarding_rule in all_regional_forwarding_rules:
                forwarding_rule_name = forwarding_rule.get("name")
                if not forwarding_rule_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/forwardingRules/{}".format(project_id, region, forwarding_rule_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_regional_forwarding_rules"].append(forwarding_rule_name)
                    logger.info("Regional Forwarding Rule deleted: %s", forwarding_rule_name)
                except GCPAPIError as e:
                    logger.warning("Regional Forwarding Rule deletion failed for %s: %s", forwarding_rule_name, str(e))

        except GCPAPIError as e:
            logger.error("List Regional Forwarding Rules failed: %s", str(e))
        except Exception as e:
            logger.error("Regional Forwarding Rules processing failed: %s", str(e), exc_info=True)

        # Wait for forwarding rules to propagate
        if len(deleted_resources["deleted_global_forwarding_rules"]) > 0 or len(deleted_resources["deleted_regional_forwarding_rules"]) > 0:
            logger.info("Waiting 20 seconds for forwarding rule deletions to propagate")
            time.sleep(20)

        # ========================================================
        # STEP 3: Delete Global Target HTTP Proxies
        # ========================================================
        logger.info("=== STEP 3: Discovering Global Target HTTP Proxies ===")
        try:
            all_global_target_http_proxies = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/targetHttpProxies".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_global_target_http_proxies.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_global_target_http_proxies"] = len(all_global_target_http_proxies)
            logger.info("Global Target HTTP Proxies discovered | Count=%d", len(all_global_target_http_proxies))

            for proxy in all_global_target_http_proxies:
                proxy_name = proxy.get("name")
                if not proxy_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/targetHttpProxies/{}".format(project_id, proxy_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_global_target_http_proxies"].append(proxy_name)
                    logger.info("Global Target HTTP Proxy deleted: %s", proxy_name)
                except GCPAPIError as e:
                    logger.warning("Global Target HTTP Proxy deletion failed for %s: %s", proxy_name, str(e))

        except GCPAPIError as e:
            logger.error("List Global Target HTTP Proxies failed: %s", str(e))
        except Exception as e:
            logger.error("Global Target HTTP Proxies processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 4: Delete Regional Target HTTP Proxies
        # ========================================================
        logger.info("=== STEP 4: Discovering Regional Target HTTP Proxies ===")
        try:
            all_regional_target_http_proxies = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/targetHttpProxies".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_regional_target_http_proxies.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_regional_target_http_proxies"] = len(all_regional_target_http_proxies)
            logger.info("Regional Target HTTP Proxies discovered | Count=%d", len(all_regional_target_http_proxies))

            for proxy in all_regional_target_http_proxies:
                proxy_name = proxy.get("name")
                if not proxy_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/targetHttpProxies/{}".format(project_id, region, proxy_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_regional_target_http_proxies"].append(proxy_name)
                    logger.info("Regional Target HTTP Proxy deleted: %s", proxy_name)
                except GCPAPIError as e:
                    logger.warning("Regional Target HTTP Proxy deletion failed for %s: %s", proxy_name, str(e))

        except GCPAPIError as e:
            logger.error("List Regional Target HTTP Proxies failed: %s", str(e))
        except Exception as e:
            logger.error("Regional Target HTTP Proxies processing failed: %s", str(e), exc_info=True)

        # Continue in next part due to length...

        # ========================================================
        # STEP 5: Delete Global Target HTTPS Proxies
        # ========================================================
        logger.info("=== STEP 5: Discovering Global Target HTTPS Proxies ===")
        try:
            all_global_target_https_proxies = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/targetHttpsProxies".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_global_target_https_proxies.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_global_target_https_proxies"] = len(all_global_target_https_proxies)
            logger.info("Global Target HTTPS Proxies discovered | Count=%d", len(all_global_target_https_proxies))

            for proxy in all_global_target_https_proxies:
                proxy_name = proxy.get("name")
                if not proxy_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/targetHttpsProxies/{}".format(project_id, proxy_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_global_target_https_proxies"].append(proxy_name)
                    logger.info("Global Target HTTPS Proxy deleted: %s", proxy_name)
                except GCPAPIError as e:
                    logger.warning("Global Target HTTPS Proxy deletion failed for %s: %s", proxy_name, str(e))

        except GCPAPIError as e:
            logger.error("List Global Target HTTPS Proxies failed: %s", str(e))
        except Exception as e:
            logger.error("Global Target HTTPS Proxies processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 6: Delete Regional Target HTTPS Proxies
        # ========================================================
        logger.info("=== STEP 6: Discovering Regional Target HTTPS Proxies ===")
        try:
            all_regional_target_https_proxies = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/targetHttpsProxies".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_regional_target_https_proxies.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_regional_target_https_proxies"] = len(all_regional_target_https_proxies)
            logger.info("Regional Target HTTPS Proxies discovered | Count=%d", len(all_regional_target_https_proxies))

            for proxy in all_regional_target_https_proxies:
                proxy_name = proxy.get("name")
                if not proxy_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/targetHttpsProxies/{}".format(project_id, region, proxy_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_regional_target_https_proxies"].append(proxy_name)
                    logger.info("Regional Target HTTPS Proxy deleted: %s", proxy_name)
                except GCPAPIError as e:
                    logger.warning("Regional Target HTTPS Proxy deletion failed for %s: %s", proxy_name, str(e))

        except GCPAPIError as e:
            logger.error("List Regional Target HTTPS Proxies failed: %s", str(e))
        except Exception as e:
            logger.error("Regional Target HTTPS Proxies processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 7: Delete Global Target TCP Proxies
        # ========================================================
        logger.info("=== STEP 7: Discovering Global Target TCP Proxies ===")
        try:
            all_global_target_tcp_proxies = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/targetTcpProxies".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_global_target_tcp_proxies.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_global_target_tcp_proxies"] = len(all_global_target_tcp_proxies)
            logger.info("Global Target TCP Proxies discovered | Count=%d", len(all_global_target_tcp_proxies))

            for proxy in all_global_target_tcp_proxies:
                proxy_name = proxy.get("name")
                if not proxy_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/targetTcpProxies/{}".format(project_id, proxy_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_global_target_tcp_proxies"].append(proxy_name)
                    logger.info("Global Target TCP Proxy deleted: %s", proxy_name)
                except GCPAPIError as e:
                    logger.warning("Global Target TCP Proxy deletion failed for %s: %s", proxy_name, str(e))

        except GCPAPIError as e:
            logger.error("List Global Target TCP Proxies failed: %s", str(e))
        except Exception as e:
            logger.error("Global Target TCP Proxies processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 8: Delete Regional Target TCP Proxies
        # ========================================================
        logger.info("=== STEP 8: Discovering Regional Target TCP Proxies ===")
        try:
            all_regional_target_tcp_proxies = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/targetTcpProxies".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_regional_target_tcp_proxies.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_regional_target_tcp_proxies"] = len(all_regional_target_tcp_proxies)
            logger.info("Regional Target TCP Proxies discovered | Count=%d", len(all_regional_target_tcp_proxies))

            for proxy in all_regional_target_tcp_proxies:
                proxy_name = proxy.get("name")
                if not proxy_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/targetTcpProxies/{}".format(project_id, region, proxy_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_regional_target_tcp_proxies"].append(proxy_name)
                    logger.info("Regional Target TCP Proxy deleted: %s", proxy_name)
                except GCPAPIError as e:
                    logger.warning("Regional Target TCP Proxy deletion failed for %s: %s", proxy_name, str(e))

        except GCPAPIError as e:
            logger.error("List Regional Target TCP Proxies failed: %s", str(e))
        except Exception as e:
            logger.error("Regional Target TCP Proxies processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 9: Delete Global URL Maps
        # ========================================================
        logger.info("=== STEP 9: Discovering Global URL Maps ===")
        try:
            all_global_url_maps = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/urlMaps".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_global_url_maps.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_global_url_maps"] = len(all_global_url_maps)
            logger.info("Global URL Maps discovered | Count=%d", len(all_global_url_maps))

            for url_map in all_global_url_maps:
                url_map_name = url_map.get("name")
                if not url_map_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/urlMaps/{}".format(project_id, url_map_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_global_url_maps"].append(url_map_name)
                    logger.info("Global URL Map deleted: %s", url_map_name)
                except GCPAPIError as e:
                    logger.warning("Global URL Map deletion failed for %s: %s", url_map_name, str(e))

        except GCPAPIError as e:
            logger.error("List Global URL Maps failed: %s", str(e))
        except Exception as e:
            logger.error("Global URL Maps processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 10: Delete Regional URL Maps
        # ========================================================
        logger.info("=== STEP 10: Discovering Regional URL Maps ===")
        try:
            all_regional_url_maps = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/urlMaps".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_regional_url_maps.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_regional_url_maps"] = len(all_regional_url_maps)
            logger.info("Regional URL Maps discovered | Count=%d", len(all_regional_url_maps))

            for url_map in all_regional_url_maps:
                url_map_name = url_map.get("name")
                if not url_map_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/urlMaps/{}".format(project_id, region, url_map_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_regional_url_maps"].append(url_map_name)
                    logger.info("Regional URL Map deleted: %s", url_map_name)
                except GCPAPIError as e:
                    logger.warning("Regional URL Map deletion failed for %s: %s", url_map_name, str(e))

        except GCPAPIError as e:
            logger.error("List Regional URL Maps failed: %s", str(e))
        except Exception as e:
            logger.error("Regional URL Maps processing failed: %s", str(e), exc_info=True)

        # Wait for URL Maps to propagate
        if len(deleted_resources["deleted_global_url_maps"]) > 0 or len(deleted_resources["deleted_regional_url_maps"]) > 0:
            logger.info("Waiting 5 seconds for URL Map deletions to propagate")
            time.sleep(5)

        # ========================================================
        # STEP 11: Delete Global Backend Services
        # ========================================================
        logger.info("=== STEP 11: Discovering Global Backend Services ===")
        try:
            all_global_backend_services = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/backendServices".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_global_backend_services.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_global_backend_services"] = len(all_global_backend_services)
            logger.info("Global Backend Services discovered | Count=%d", len(all_global_backend_services))

            for backend_service in all_global_backend_services:
                backend_service_name = backend_service.get("name")
                if not backend_service_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/backendServices/{}".format(project_id, backend_service_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_global_backend_services"].append(backend_service_name)
                    logger.info("Global Backend Service deleted: %s", backend_service_name)
                except GCPAPIError as e:
                    logger.warning("Global Backend Service deletion failed for %s: %s", backend_service_name, str(e))

        except GCPAPIError as e:
            logger.error("List Global Backend Services failed: %s", str(e))
        except Exception as e:
            logger.error("Global Backend Services processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 12: Delete Regional Backend Services
        # ========================================================
        logger.info("=== STEP 12: Discovering Regional Backend Services ===")
        try:
            all_regional_backend_services = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/backendServices".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_regional_backend_services.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_regional_backend_services"] = len(all_regional_backend_services)
            logger.info("Regional Backend Services discovered | Count=%d", len(all_regional_backend_services))

            for backend_service in all_regional_backend_services:
                backend_service_name = backend_service.get("name")
                if not backend_service_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/backendServices/{}".format(project_id, region, backend_service_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_regional_backend_services"].append(backend_service_name)
                    logger.info("Regional Backend Service deleted: %s", backend_service_name)
                except GCPAPIError as e:
                    logger.warning("Regional Backend Service deletion failed for %s: %s", backend_service_name, str(e))

        except GCPAPIError as e:
            logger.error("List Regional Backend Services failed: %s", str(e))
        except Exception as e:
            logger.error("Regional Backend Services processing failed: %s", str(e), exc_info=True)

        # Wait for Backend Services to propagate
        if len(deleted_resources["deleted_global_backend_services"]) > 0 or len(deleted_resources["deleted_regional_backend_services"]) > 0:
            logger.info("Waiting 20 seconds for Backend Service deletions to propagate")
            time.sleep(20)

        # ========================================================
        # STEP 13: Delete Backend Buckets
        # ========================================================
        logger.info("=== STEP 13: Discovering Backend Buckets ===")
        try:
            all_backend_buckets = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/backendBuckets".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_backend_buckets.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_backend_buckets"] = len(all_backend_buckets)
            logger.info("Backend Buckets discovered | Count=%d", len(all_backend_buckets))

            for backend_bucket in all_backend_buckets:
                backend_bucket_name = backend_bucket.get("name")
                if not backend_bucket_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/backendBuckets/{}".format(project_id, backend_bucket_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_backend_buckets"].append(backend_bucket_name)
                    logger.info("Backend Bucket deleted: %s", backend_bucket_name)
                except GCPAPIError as e:
                    logger.warning("Backend Bucket deletion failed for %s: %s", backend_bucket_name, str(e))

        except GCPAPIError as e:
            logger.error("List Backend Buckets failed: %s", str(e))
        except Exception as e:
            logger.error("Backend Buckets processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 14: Delete Global Health Checks
        # ========================================================
        logger.info("=== STEP 14: Discovering Global Health Checks ===")
        try:
            all_global_health_checks = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/healthChecks".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_global_health_checks.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_global_health_checks"] = len(all_global_health_checks)
            logger.info("Global Health Checks discovered | Count=%d", len(all_global_health_checks))

            for health_check in all_global_health_checks:
                health_check_name = health_check.get("name")
                if not health_check_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/healthChecks/{}".format(project_id, health_check_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_global_health_checks"].append(health_check_name)
                    logger.info("Global Health Check deleted: %s", health_check_name)
                except GCPAPIError as e:
                    logger.warning("Global Health Check deletion failed for %s: %s", health_check_name, str(e))

        except GCPAPIError as e:
            logger.error("List Global Health Checks failed: %s", str(e))
        except Exception as e:
            logger.error("Global Health Checks processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 15: Delete Regional Health Checks
        # ========================================================
        logger.info("=== STEP 15: Discovering Regional Health Checks ===")
        try:
            all_regional_health_checks = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/healthChecks".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_regional_health_checks.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_regional_health_checks"] = len(all_regional_health_checks)
            logger.info("Regional Health Checks discovered | Count=%d", len(all_regional_health_checks))

            for health_check in all_regional_health_checks:
                health_check_name = health_check.get("name")
                if not health_check_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/healthChecks/{}".format(project_id, region, health_check_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_regional_health_checks"].append(health_check_name)
                    logger.info("Regional Health Check deleted: %s", health_check_name)
                except GCPAPIError as e:
                    logger.warning("Regional Health Check deletion failed for %s: %s", health_check_name, str(e))

        except GCPAPIError as e:
            logger.error("List Regional Health Checks failed: %s", str(e))
        except Exception as e:
            logger.error("Regional Health Checks processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 16: Delete Instance Groups in Zones
        # ========================================================
        logger.info("=== STEP 16: Discovering Instance Groups in Zones ===")
        
        # First, get all zones in the region
        all_zones = []
        try:
            page_token = None
            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/zones".format(project_id),
                    params=params,
                )

                for zone in response.get("items", []):
                    zone_name = zone.get("name", "")
                    if zone_name.startswith("{}-".format(region)):
                        all_zones.append(zone_name)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            logger.info("Zones discovered in region %s | Count=%d", region, len(all_zones))

        except GCPAPIError as e:
            logger.error("List Zones failed: %s", str(e))
        except Exception as e:
            logger.error("Zone discovery failed: %s", str(e), exc_info=True)

        # Now delete instance groups in each zone
        for zone_name in all_zones:
            try:
                all_instance_groups = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/zones/{}/instanceGroups".format(project_id, zone_name),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_instance_groups.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_instance_groups"] += len(all_instance_groups)
                
                if len(all_instance_groups) > 0:
                    logger.info("Instance Groups discovered in zone %s | Count=%d", zone_name, len(all_instance_groups))

                for instance_group in all_instance_groups:
                    instance_group_name = instance_group.get("name")
                    if not instance_group_name:
                        continue

                    # Try to delete as regular instance group first
                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/zones/{}/instanceGroups/{}".format(project_id, zone_name, instance_group_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_instance_groups"].append({
                            "name": instance_group_name,
                            "zone": zone_name
                        })
                        logger.info("Instance Group deleted: %s in zone %s", instance_group_name, zone_name)
                    except GCPAPIError as e:
                        # If it's managed by instance group manager, delete the manager
                        if "managed by" in str(e.message).lower():
                            try:
                                compute_client.request(
                                    "DELETE",
                                    "projects/{}/zones/{}/instanceGroupManagers/{}".format(project_id, zone_name, instance_group_name),
                                    params={"requestId": str(uuid.uuid4())},
                                )
                                deleted_resources["deleted_instance_group_managers"].append({
                                    "name": instance_group_name,
                                    "zone": zone_name
                                })
                                deleted_resources["total_instance_group_managers"] += 1
                                logger.info("Instance Group Manager deleted: %s in zone %s", instance_group_name, zone_name)
                            except GCPAPIError as mgr_error:
                                logger.warning("Instance Group Manager deletion failed for %s in zone %s: %s", 
                                             instance_group_name, zone_name, str(mgr_error))
                        else:
                            logger.warning("Instance Group deletion failed for %s in zone %s: %s", 
                                         instance_group_name, zone_name, str(e))

            except GCPAPIError as e:
                logger.error("List Instance Groups failed in zone %s: %s", zone_name, str(e))
            except Exception as e:
                logger.error("Instance Groups processing failed in zone %s: %s", zone_name, str(e), exc_info=True)

        # ========================================================
        # STEP 17: Delete SSL Certificates
        # ========================================================
        logger.info("=== STEP 17: Discovering SSL Certificates ===")
        try:
            all_ssl_certificates = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/global/sslCertificates".format(project_id),
                    params=params,
                )

                items = response.get("items", [])
                all_ssl_certificates.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_ssl_certificates"] = len(all_ssl_certificates)
            logger.info("SSL Certificates discovered | Count=%d", len(all_ssl_certificates))

            for ssl_cert in all_ssl_certificates:
                ssl_cert_name = ssl_cert.get("name")
                if not ssl_cert_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/sslCertificates/{}".format(project_id, ssl_cert_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_ssl_certificates"].append(ssl_cert_name)
                    logger.info("SSL Certificate deleted: %s", ssl_cert_name)
                except GCPAPIError as e:
                    logger.warning("SSL Certificate deletion failed for %s: %s", ssl_cert_name, str(e))

        except GCPAPIError as e:
            logger.error("List SSL Certificates failed: %s", str(e))
        except Exception as e:
            logger.error("SSL Certificates processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 18: Delete Firewall Rules
        # ========================================================
        logger.info("=== STEP 18: Discovering Firewall Rules ===")
        try:
            all_firewall_rules = []
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
                all_firewall_rules.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_firewall_rules"] = len(all_firewall_rules)
            logger.info("Firewall Rules discovered | Count=%d", len(all_firewall_rules))

            for firewall_rule in all_firewall_rules:
                firewall_rule_name = firewall_rule.get("name")
                if not firewall_rule_name:
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/global/firewalls/{}".format(project_id, firewall_rule_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_firewall_rules"].append(firewall_rule_name)
                    logger.info("Firewall Rule deleted: %s", firewall_rule_name)
                except GCPAPIError as e:
                    logger.warning("Firewall Rule deletion failed for %s: %s", firewall_rule_name, str(e))

        except GCPAPIError as e:
            logger.error("List Firewall Rules failed: %s", str(e))
        except Exception as e:
            logger.error("Firewall Rules processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 19: Delete Static IP Addresses (Regional)
        # ========================================================
        logger.info("=== STEP 19: Discovering Static IP Addresses ===")
        try:
            all_addresses = []
            page_token = None

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/regions/{}/addresses".format(project_id, region),
                    params=params,
                )

                items = response.get("items", [])
                all_addresses.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_addresses"] = len(all_addresses)
            logger.info("Static IP Addresses discovered | Count=%d", len(all_addresses))

            for address in all_addresses:
                address_name = address.get("name")
                if not address_name:
                    continue

                # Only delete if not in use
                if address.get("status") == "IN_USE":
                    logger.info("Skipping Static IP Address %s (IN_USE)", address_name)
                    continue

                try:
                    compute_client.request(
                        "DELETE",
                        "projects/{}/regions/{}/addresses/{}".format(project_id, region, address_name),
                        params={"requestId": str(uuid.uuid4())},
                    )
                    deleted_resources["deleted_addresses"].append(address_name)
                    logger.info("Static IP Address deleted: %s", address_name)
                except GCPAPIError as e:
                    logger.warning("Static IP Address deletion failed for %s: %s", address_name, str(e))

        except GCPAPIError as e:
            logger.error("List Static IP Addresses failed: %s", str(e))
        except Exception as e:
            logger.error("Static IP Addresses processing failed: %s", str(e), exc_info=True)

        # ------------------------------------------------------------
        # Workflow completed successfully
        # ------------------------------------------------------------
        logger.info("GCP Load Balancer cleanup COMPLETED | Project=%s | Region=%s | "
                   "GlobalForwardingRules=%d | RegionalForwardingRules=%d | "
                   "GlobalTargetHTTPProxies=%d | RegionalTargetHTTPProxies=%d | "
                   "GlobalTargetHTTPSProxies=%d | RegionalTargetHTTPSProxies=%d | "
                   "GlobalTargetTCPProxies=%d | RegionalTargetTCPProxies=%d | "
                   "GlobalURLMaps=%d | RegionalURLMaps=%d | "
                   "GlobalBackendServices=%d | RegionalBackendServices=%d | "
                   "BackendBuckets=%d | GlobalHealthChecks=%d | RegionalHealthChecks=%d | "
                   "InstanceGroups=%d | InstanceGroupManagers=%d | "
                   "SSLCertificates=%d | FirewallRules=%d | Addresses=%d",
                   project_id, region,
                   len(deleted_resources["deleted_global_forwarding_rules"]),
                   len(deleted_resources["deleted_regional_forwarding_rules"]),
                   len(deleted_resources["deleted_global_target_http_proxies"]),
                   len(deleted_resources["deleted_regional_target_http_proxies"]),
                   len(deleted_resources["deleted_global_target_https_proxies"]),
                   len(deleted_resources["deleted_regional_target_https_proxies"]),
                   len(deleted_resources["deleted_global_target_tcp_proxies"]),
                   len(deleted_resources["deleted_regional_target_tcp_proxies"]),
                   len(deleted_resources["deleted_global_url_maps"]),
                   len(deleted_resources["deleted_regional_url_maps"]),
                   len(deleted_resources["deleted_global_backend_services"]),
                   len(deleted_resources["deleted_regional_backend_services"]),
                   len(deleted_resources["deleted_backend_buckets"]),
                   len(deleted_resources["deleted_global_health_checks"]),
                   len(deleted_resources["deleted_regional_health_checks"]),
                   len(deleted_resources["deleted_instance_groups"]),
                   len(deleted_resources["deleted_instance_group_managers"]),
                   len(deleted_resources["deleted_ssl_certificates"]),
                   len(deleted_resources["deleted_firewall_rules"]),
                   len(deleted_resources["deleted_addresses"]))

        results.append({
            "status": "SUCCESS",
            "action": "LOAD_BALANCER_TERMINATION",
            "project_id": project_id,
            "region": region,
            "data": deleted_resources
        })

    except GCPAPIError as e:
        logger.error("GCP API Error during Load Balancer deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "LOAD_BALANCER_TERMINATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(e.message),
            "http_status": e.status_code,
        })
    except Exception as e:
        logger.error("Unhandled exception during Load Balancer deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "LOAD_BALANCER_TERMINATION",
            "reason": "UnhandledException",
            "project_id": project_id,
            "region": region,
            "detail": str(e),
        })

    return results[0] if results else {
        "status": "FAILED",
        "action": "LOAD_BALANCER_TERMINATION",
        "reason": "NoResults"
    }
