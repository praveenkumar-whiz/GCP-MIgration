import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-vpc-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP VPC Network Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP VPC Network cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name / region (required)
    - gcp_access_token / access_token (required)
    
    IMPORTANT: VPC networks can only be deleted if ALL resources using them are deleted first.
    This includes: VM instances, load balancers, Cloud SQL, GKE clusters, Serverless VPC connectors, etc.
    If a VPC deletion fails with "already being used by" error, you must delete those resources first.
    
    For proper cleanup order, run VM termination BEFORE VPC termination.
    """
    cleanup_event = spec
    logger.info("GCP VPC Network Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "VPC_NETWORK_TERMINATION",
            "reason": "MissingRegion",
        }

    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "VPC_NETWORK_TERMINATION",
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

    for region in regions:
        deleted_resources = {
            "deleted_addresses": [],
            "deleted_global_addresses": [],
            "deleted_packet_mirrorings": [],
            "deleted_vpn_tunnels": [],
            "deleted_vpn_gateways": [],
            "deleted_target_vpn_gateways": [],
            "deleted_network_attachments": [],
            "deleted_firewalls": [],
            "deleted_forwarding_rules": [],
            "deleted_subnetworks": [],
            "deleted_networks": [],
            "created_default_vpc": False,
            "created_default_firewall": False,
            "total_addresses": 0,
            "total_global_addresses": 0,
            "total_packet_mirrorings": 0,
            "total_vpn_tunnels": 0,
            "total_vpn_gateways": 0,
            "total_target_vpn_gateways": 0,
            "total_network_attachments": 0,
            "total_firewalls": 0,
            "total_forwarding_rules": 0,
            "total_subnetworks": 0,
            "total_networks": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connection to GCP Compute Service
            # ----------------------------------------------------------------
            compute_client = GCPClientFactory.create(
                "compute",
                creds=client_creds,
            )

            logger.info("Connected to GCP Compute service | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP VPC NETWORK FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: Delete Regional IP Addresses
            # ========================================================
            logger.info("=== STEP 1: Discovering Regional IP Addresses ===")
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
                logger.info("Regional IP Addresses discovered | Count=%d", len(all_addresses))

                for address in all_addresses:
                    address_name = address.get("name")
                    if not address_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/addresses/{}".format(project_id, region, address_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_addresses"].append(address_name)
                        logger.info("Regional IP Address deleted: %s", address_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Regional IP Address",
                            "resource_name": address_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Regional IP Address deletion failed for %s: %s", address_name, str(e))

            except GCPAPIError as e:
                logger.error("List Regional IP Addresses failed: %s", str(e))
            except Exception as e:
                logger.error("Regional IP Addresses processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 2: Delete Global IP Addresses
            # ========================================================
            logger.info("=== STEP 2: Discovering Global IP Addresses ===")
            try:
                all_global_addresses = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/addresses".format(project_id),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_global_addresses.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_global_addresses"] = len(all_global_addresses)
                logger.info("Global IP Addresses discovered | Count=%d", len(all_global_addresses))

                for address in all_global_addresses:
                    address_name = address.get("name")
                    if not address_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/global/addresses/{}".format(project_id, address_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_global_addresses"].append(address_name)
                        logger.info("Global IP Address deleted: %s", address_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Global IP Address",
                            "resource_name": address_name,
                            "error": str(e)
                        })
                        logger.warning("Global IP Address deletion failed for %s: %s", address_name, str(e))

            except GCPAPIError as e:
                logger.error("List Global IP Addresses failed: %s", str(e))
            except Exception as e:
                logger.error("Global IP Addresses processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 3: Delete Packet Mirrorings
            # ========================================================
            logger.info("=== STEP 3: Discovering Packet Mirrorings ===")
            try:
                all_packet_mirrorings = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/packetMirrorings".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_packet_mirrorings.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_packet_mirrorings"] = len(all_packet_mirrorings)
                logger.info("Packet Mirrorings discovered | Count=%d", len(all_packet_mirrorings))

                for packet_mirroring in all_packet_mirrorings:
                    packet_mirroring_name = packet_mirroring.get("name")
                    if not packet_mirroring_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/packetMirrorings/{}".format(project_id, region, packet_mirroring_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_packet_mirrorings"].append(packet_mirroring_name)
                        logger.info("Packet Mirroring deleted: %s", packet_mirroring_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Packet Mirroring",
                            "resource_name": packet_mirroring_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Packet Mirroring deletion failed for %s: %s", packet_mirroring_name, str(e))

            except GCPAPIError as e:
                logger.error("List Packet Mirrorings failed: %s", str(e))
            except Exception as e:
                logger.error("Packet Mirrorings processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 4: Delete VPN Tunnels
            # ========================================================
            logger.info("=== STEP 4: Discovering VPN Tunnels ===")
            try:
                all_vpn_tunnels = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/vpnTunnels".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_vpn_tunnels.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_vpn_tunnels"] = len(all_vpn_tunnels)
                logger.info("VPN Tunnels discovered | Count=%d", len(all_vpn_tunnels))

                for vpn_tunnel in all_vpn_tunnels:
                    vpn_tunnel_name = vpn_tunnel.get("name")
                    if not vpn_tunnel_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/vpnTunnels/{}".format(project_id, region, vpn_tunnel_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_vpn_tunnels"].append(vpn_tunnel_name)
                        logger.info("VPN Tunnel deleted: %s", vpn_tunnel_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "VPN Tunnel",
                            "resource_name": vpn_tunnel_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("VPN Tunnel deletion failed for %s: %s", vpn_tunnel_name, str(e))

            except GCPAPIError as e:
                logger.error("List VPN Tunnels failed: %s", str(e))
            except Exception as e:
                logger.error("VPN Tunnels processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 5: Delete VPN Gateways
            # ========================================================
            logger.info("=== STEP 5: Discovering VPN Gateways ===")
            try:
                all_vpn_gateways = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/vpnGateways".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_vpn_gateways.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_vpn_gateways"] = len(all_vpn_gateways)
                logger.info("VPN Gateways discovered | Count=%d", len(all_vpn_gateways))

                for vpn_gateway in all_vpn_gateways:
                    vpn_gateway_name = vpn_gateway.get("name")
                    if not vpn_gateway_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/vpnGateways/{}".format(project_id, region, vpn_gateway_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_vpn_gateways"].append(vpn_gateway_name)
                        logger.info("VPN Gateway deleted: %s", vpn_gateway_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "VPN Gateway",
                            "resource_name": vpn_gateway_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("VPN Gateway deletion failed for %s: %s", vpn_gateway_name, str(e))

            except GCPAPIError as e:
                logger.error("List VPN Gateways failed: %s", str(e))
            except Exception as e:
                logger.error("VPN Gateways processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 6: Delete Target VPN Gateways
            # ========================================================
            logger.info("=== STEP 6: Discovering Target VPN Gateways ===")
            try:
                all_target_vpn_gateways = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/targetVpnGateways".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_target_vpn_gateways.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_target_vpn_gateways"] = len(all_target_vpn_gateways)
                logger.info("Target VPN Gateways discovered | Count=%d", len(all_target_vpn_gateways))

                for target_vpn_gateway in all_target_vpn_gateways:
                    target_vpn_gateway_name = target_vpn_gateway.get("name")
                    if not target_vpn_gateway_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/targetVpnGateways/{}".format(project_id, region, target_vpn_gateway_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_target_vpn_gateways"].append(target_vpn_gateway_name)
                        logger.info("Target VPN Gateway deleted: %s", target_vpn_gateway_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Target VPN Gateway",
                            "resource_name": target_vpn_gateway_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Target VPN Gateway deletion failed for %s: %s", target_vpn_gateway_name, str(e))

            except GCPAPIError as e:
                logger.error("List Target VPN Gateways failed: %s", str(e))
            except Exception as e:
                logger.error("Target VPN Gateways processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 7: Delete Network Attachments
            # ========================================================
            logger.info("=== STEP 7: Discovering Network Attachments ===")
            try:
                all_network_attachments = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/networkAttachments".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_network_attachments.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_network_attachments"] = len(all_network_attachments)
                logger.info("Network Attachments discovered | Count=%d", len(all_network_attachments))

                for network_attachment in all_network_attachments:
                    network_attachment_name = network_attachment.get("name")
                    if not network_attachment_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/networkAttachments/{}".format(project_id, region, network_attachment_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_network_attachments"].append(network_attachment_name)
                        logger.info("Network Attachment deleted: %s", network_attachment_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Network Attachment",
                            "resource_name": network_attachment_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Network Attachment deletion failed for %s: %s", network_attachment_name, str(e))

            except GCPAPIError as e:
                logger.error("List Network Attachments failed: %s", str(e))
            except Exception as e:
                logger.error("Network Attachments processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 8: Delete Firewall Rules (skip default-*)
            # ========================================================
            logger.info("=== STEP 8: Discovering Firewall Rules ===")
            try:
                all_firewalls = []
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
                    all_firewalls.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_firewalls"] = len(all_firewalls)
                logger.info("Firewall Rules discovered | Count=%d", len(all_firewalls))

                for firewall in all_firewalls:
                    firewall_name = firewall.get("name")
                    if not firewall_name:
                        continue

                    # Skip default firewall rules
                    if firewall_name.startswith("default-"):
                        logger.info("Skipping default firewall rule: %s", firewall_name)
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/global/firewalls/{}".format(project_id, firewall_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_firewalls"].append(firewall_name)
                        logger.info("Firewall Rule deleted: %s", firewall_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Firewall Rule",
                            "resource_name": firewall_name,
                            "error": str(e)
                        })
                        logger.warning("Firewall Rule deletion failed for %s: %s", firewall_name, str(e))

            except GCPAPIError as e:
                logger.error("List Firewall Rules failed: %s", str(e))
            except Exception as e:
                logger.error("Firewall Rules processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 9: Delete Subnetworks (only custom/non-auto subnetworks)
            # Note: Auto-mode subnetworks and routes will be deleted automatically with the network
            # ========================================================
            logger.info("=== STEP 9: Discovering Subnetworks ===")
            try:
                all_subnetworks = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/subnetworks".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_subnetworks.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_subnetworks"] = len(all_subnetworks)
                logger.info("Subnetworks discovered | Count=%d", len(all_subnetworks))

                for subnetwork in all_subnetworks:
                    subnetwork_name = subnetwork.get("name")
                    if not subnetwork_name:
                        continue

                    # Check if subnetwork is used by load balancer (REGIONAL_MANAGED_PROXY)
                    if subnetwork.get("purpose") == "REGIONAL_MANAGED_PROXY":
                        logger.info("Subnetwork %s is REGIONAL_MANAGED_PROXY, checking forwarding rules...", subnetwork_name)
                        
                        # Delete associated forwarding rules
                        try:
                            fr_page_token = None
                            while True:
                                fr_params = {}
                                if fr_page_token:
                                    fr_params["pageToken"] = fr_page_token

                                fr_response = compute_client.request(
                                    "GET",
                                    "projects/{}/regions/{}/forwardingRules".format(project_id, region),
                                    params=fr_params,
                                )

                                fr_items = fr_response.get("items", [])
                                for forwarding_rule in fr_items:
                                    forwarding_rule_name = forwarding_rule.get("name")
                                    if not forwarding_rule_name:
                                        continue

                                    try:
                                        compute_client.request(
                                            "DELETE",
                                            "projects/{}/regions/{}/forwardingRules/{}".format(project_id, region, forwarding_rule_name),
                                            params={"requestId": str(uuid.uuid4())},
                                        )
                                        deleted_resources["deleted_forwarding_rules"].append(forwarding_rule_name)
                                        logger.info("Forwarding Rule deleted: %s", forwarding_rule_name)
                                    except GCPAPIError as e:
                                        deleted_resources["failed_deletions"].append({
                                            "resource_type": "Forwarding Rule",
                                            "resource_name": forwarding_rule_name,
                                            "region": region,
                                            "error": str(e)
                                        })
                                        logger.warning("Forwarding Rule deletion failed for %s: %s", forwarding_rule_name, str(e))

                                fr_page_token = fr_response.get("nextPageToken")
                                if not fr_page_token:
                                    break

                        except GCPAPIError as e:
                            logger.warning("List Forwarding Rules failed: %s", str(e))

                    # Try to delete subnetwork (will fail for auto-mode subnets, which is expected)
                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/subnetworks/{}".format(project_id, region, subnetwork_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_subnetworks"].append(subnetwork_name)
                        logger.info("Subnetwork deleted: %s", subnetwork_name)
                    except GCPAPIError as e:
                        # Auto-mode subnetworks cannot be deleted individually - they'll be deleted with the network
                        if "auto subnetwork" in str(e.message).lower() or "auto subnet mode" in str(e.message).lower():
                            logger.info("Skipping auto-mode subnetwork %s (will be deleted with network)", subnetwork_name)
                        else:
                            deleted_resources["failed_deletions"].append({
                                "resource_type": "Subnetwork",
                                "resource_name": subnetwork_name,
                                "region": region,
                                "error": str(e)
                            })
                            logger.warning("Subnetwork deletion failed for %s: %s", subnetwork_name, str(e))

                # Wait for custom subnetwork deletions to complete
                if deleted_resources["deleted_subnetworks"]:
                    logger.info("Waiting for subnetwork deletions to complete...")
                    time.sleep(20)

            except GCPAPIError as e:
                logger.error("List Subnetworks failed: %s", str(e))
            except Exception as e:
                logger.error("Subnetworks processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 10: Delete Custom Routes (skip default routes)
            # Note: Default routes are automatically managed by GCP and deleted with the network
            # Custom routes must be deleted before the network can be deleted
            # ========================================================
            logger.info("=== STEP 10: Discovering Routes ===")
            try:
                all_routes = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/routes".format(project_id),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_routes.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Routes discovered | Count=%d", len(all_routes))

                deleted_routes = []
                for route in all_routes:
                    route_name = route.get("name")
                    if not route_name:
                        continue

                    # Skip default routes (they start with "default-route-" or are named "default-route")
                    # These are automatically managed by GCP
                    if route_name.startswith("default-route-") or route_name == "default-route":
                        logger.info("Skipping default route: %s (managed by GCP)", route_name)
                        continue

                    # Skip peering routes (they start with "peering-route-")
                    # These are auto-generated by VPC peering and cannot be deleted manually
                    # They will be automatically removed when the peering connection is deleted
                    if route_name.startswith("peering-route-"):
                        logger.info("Skipping peering route: %s (auto-managed by VPC peering)", route_name)
                        continue

                    try:
                        delete_response = compute_client.request(
                            "DELETE",
                            "projects/{}/global/routes/{}".format(project_id, route_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        
                        # Wait for the operation to complete
                        operation_name = delete_response.get("name")
                        if operation_name:
                            max_wait_time = 30
                            poll_interval = 2
                            elapsed_time = 0
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = compute_client.request(
                                        "GET",
                                        "projects/{}/global/operations/{}".format(project_id, operation_name),
                                    )
                                    
                                    if operation_status.get("status") == "DONE":
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            errors = error_obj.get("errors", [])
                                            error_messages = [err.get("message", str(err)) for err in errors]
                                            raise GCPAPIError("; ".join(error_messages))
                                        break
                                    
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                except GCPAPIError:
                                    raise
                        
                        deleted_routes.append(route_name)
                        logger.info("Route deleted: %s", route_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Route",
                            "resource_name": route_name,
                            "error": str(e)
                        })
                        logger.warning("Route deletion failed for %s: %s", route_name, str(e))

                if deleted_routes:
                    logger.info("Custom routes deleted | Count=%d", len(deleted_routes))
                    time.sleep(10)

            except GCPAPIError as e:
                logger.error("List Routes failed: %s", str(e))
            except Exception as e:
                logger.error("Routes processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 11: Delete Networks (including default)
            # Note: Deleting a network automatically deletes its default routes and auto-mode subnetworks
            # ========================================================
            logger.info("=== STEP 11: Discovering Networks ===")
            try:
                all_networks = []
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
                    all_networks.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_networks"] = len(all_networks)
                logger.info("Networks discovered | Count=%d", len(all_networks))

                for network in all_networks:
                    network_name = network.get("name")
                    if not network_name:
                        continue

                    try:
                        # Initiate network deletion
                        delete_response = compute_client.request(
                            "DELETE",
                            "projects/{}/global/networks/{}".format(project_id, network_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        
                        # Wait for the operation to complete
                        operation_name = delete_response.get("name")
                        if operation_name:
                            logger.info("Waiting for network deletion operation: %s", operation_name)
                            
                            # Poll the operation status (max 60 seconds)
                            max_wait_time = 60
                            poll_interval = 2
                            elapsed_time = 0
                            operation_done = False
                            operation_error = None
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = compute_client.request(
                                        "GET",
                                        "projects/{}/global/operations/{}".format(project_id, operation_name),
                                    )
                                    
                                    status = operation_status.get("status")
                                    if status == "DONE":
                                        operation_done = True
                                        # Check for errors in the operation
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            errors = error_obj.get("errors", [])
                                            if errors:
                                                error_messages = [err.get("message", str(err)) for err in errors]
                                                operation_error = "; ".join(error_messages)
                                        break
                                    
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                    
                                except GCPAPIError as poll_error:
                                    logger.warning("Error polling operation status: %s", str(poll_error))
                                    break
                            
                            if not operation_done:
                                logger.warning("Network deletion operation timed out after %ds: %s", max_wait_time, network_name)
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "VPC Network",
                                    "resource_name": network_name,
                                    "error": "Operation timed out after {}s".format(max_wait_time)
                                })
                            elif operation_error:
                                logger.error("VPC Network deletion failed for %s: %s", network_name, operation_error)
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "VPC Network",
                                    "resource_name": network_name,
                                    "error": operation_error
                                })
                            else:
                                deleted_resources["deleted_networks"].append(network_name)
                                logger.info("Network deleted successfully: %s", network_name)
                        else:
                            # No operation returned, assume immediate success (unlikely for DELETE)
                            deleted_resources["deleted_networks"].append(network_name)
                            logger.info("Network deleted: %s", network_name)
                            
                    except GCPAPIError as e:
                        # Capture detailed error including which resource is blocking deletion
                        error_detail = str(e.message) if hasattr(e, 'message') else str(e)
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "VPC Network",
                            "resource_name": network_name,
                            "error": error_detail,
                            "http_status": e.status_code if hasattr(e, 'status_code') else None
                        })
                        logger.error("VPC Network deletion failed for %s: %s", network_name, error_detail)

                # Additional wait for network deletions to fully propagate (including auto-subnets)
                if deleted_resources["deleted_networks"]:
                    logger.info("Waiting for 15s for VPC network deletions to fully propagate...")
                    time.sleep(15)

            except GCPAPIError as e:
                logger.error("List Networks failed: %s", str(e))
            except Exception as e:
                logger.error("Networks processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 12: Create Default VPC 
            # ========================================================
            logger.info("=== STEP 12: Create Default Network ===")
            default_vpc_created = False
            
            # Check if default network exists
            default_exists = False
            try:
                compute_client.request(
                    "GET",
                    "projects/{}/global/networks/default".format(project_id),
                )
                default_exists = True
                logger.info("Default VPC network already exists")
            except GCPAPIError as e:
                if e.status_code == 404:
                    logger.info("Default VPC network does not exist, will create it")
                    default_exists = False
                else:
                    logger.warning("Error checking default network existence: %s", str(e))
            
            # Create default VPC if it doesn't exist
            if not default_exists:
                try:
                    default_network_body = {
                        "name": "default",
                        "autoCreateSubnetworks": True,
                        "description": "Default network for the project"
                    }
                    
                    create_response = compute_client.request(
                        "POST",
                        "projects/{}/global/networks".format(project_id),
                        json_body=default_network_body,
                    )
                    
                    # Wait for the creation operation to complete
                    operation_name = create_response.get("name")
                    if operation_name:
                        logger.info("Waiting for default network creation operation: %s", operation_name)
                        
                        max_wait_time = 60
                        poll_interval = 2
                        elapsed_time = 0
                        
                        while elapsed_time < max_wait_time:
                            try:
                                operation_status = compute_client.request(
                                    "GET",
                                    "projects/{}/global/operations/{}".format(project_id, operation_name),
                                )
                                
                                if operation_status.get("status") == "DONE":
                                    if "error" in operation_status:
                                        error_obj = operation_status["error"]
                                        errors = error_obj.get("errors", [])
                                        error_messages = [err.get("message", str(err)) for err in errors]
                                        raise GCPAPIError("; ".join(error_messages))
                                    break
                                
                                time.sleep(poll_interval)
                                elapsed_time += poll_interval
                            except GCPAPIError:
                                raise
                    
                    logger.info("New default VPC network created successfully")
                    default_vpc_created = True
                    deleted_resources["created_default_vpc"] = True
                    
                    # Wait for default network to fully propagate
                    time.sleep(5)
                    
                except GCPAPIError as e:
                    if e.status_code == 409:
                        logger.warning("Default network already exists (409 conflict) - may have been created by another process")
                        default_vpc_created = True
                        deleted_resources["created_default_vpc"] = True
                    else:
                        logger.error("Default network creation failed: %s", str(e))
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Default VPC Network Creation",
                            "resource_name": "default",
                            "error": str(e)
                        })
                except Exception as e:
                    logger.error("Default network creation failed: %s", str(e), exc_info=True)
                    deleted_resources["failed_deletions"].append({
                        "resource_type": "Default VPC Network Creation",
                        "resource_name": "default",
                        "error": str(e)
                    })

            # ========================================================
            # STEP 13: Create Allow-All Ingress Firewall Rule for Default VPC
            # ========================================================
            if default_vpc_created:
                logger.info("=== STEP 13: Create Allow-All Ingress Firewall Rule ===")
                
                firewall_rule_name = "allow-all-ingress"
                
                # Check if firewall rule already exists
                firewall_exists = False
                try:
                    compute_client.request(
                        "GET",
                        "projects/{}/global/firewalls/{}".format(project_id, firewall_rule_name),
                    )
                    firewall_exists = True
                    logger.info("Firewall rule '%s' already exists", firewall_rule_name)
                except GCPAPIError as e:
                    if e.status_code == 404:
                        logger.info("Firewall rule '%s' does not exist, will create it", firewall_rule_name)
                        firewall_exists = False
                    else:
                        logger.warning("Error checking firewall rule existence: %s", str(e))
                
                # Create firewall rule if it doesn't exist
                if not firewall_exists:
                    try:
                        firewall_rule_body = {
                            "name": firewall_rule_name,
                            "network": "projects/{}/global/networks/default".format(project_id),
                            "direction": "INGRESS",
                            "priority": 1000,
                            "allowed": [
                                {
                                    "IPProtocol": "all"
                                }
                            ],
                            "sourceRanges": ["0.0.0.0/0"],
                            "description": "TEMP: allow all inbound traffic"
                        }
                        
                        create_firewall_response = compute_client.request(
                            "POST",
                            "projects/{}/global/firewalls".format(project_id),
                            json_body=firewall_rule_body,
                        )
                        
                        # Wait for the creation operation to complete
                        operation_name = create_firewall_response.get("name")
                        if operation_name:
                            logger.info("Waiting for firewall rule creation operation: %s", operation_name)
                            
                            max_wait_time = 10
                            poll_interval = 2
                            elapsed_time = 0
                            
                            while elapsed_time < max_wait_time:
                                try:
                                    operation_status = compute_client.request(
                                        "GET",
                                        "projects/{}/global/operations/{}".format(project_id, operation_name),
                                    )
                                    
                                    if operation_status.get("status") == "DONE":
                                        if "error" in operation_status:
                                            error_obj = operation_status["error"]
                                            errors = error_obj.get("errors", [])
                                            error_messages = [err.get("message", str(err)) for err in errors]
                                            raise GCPAPIError("; ".join(error_messages))
                                        break
                                    
                                    time.sleep(poll_interval)
                                    elapsed_time += poll_interval
                                except GCPAPIError:
                                    raise
                        
                        logger.info("Firewall rule '%s' created successfully on default VPC", firewall_rule_name)
                        deleted_resources["created_default_firewall"] = True
                        
                    except GCPAPIError as e:
                        if e.status_code == 409:
                            logger.warning("Firewall rule '%s' already exists (409 conflict) - may have been created by another process", firewall_rule_name)
                            deleted_resources["created_default_firewall"] = True
                        else:
                            logger.error("Firewall rule creation failed: %s", str(e))
                            deleted_resources["failed_deletions"].append({
                                "resource_type": "Default Firewall Rule Creation",
                                "resource_name": firewall_rule_name,
                                "error": str(e)
                            })
                    except Exception as e:
                        logger.error("Firewall rule creation failed: %s", str(e), exc_info=True)
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Default Firewall Rule Creation",
                            "resource_name": firewall_rule_name,
                            "error": str(e)
                        })
                else:
                    deleted_resources["created_default_firewall"] = True

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP VPC Network cleanup COMPLETED | Project=%s | Region=%s | "
                       "Addresses=%d | GlobalAddresses=%d | PacketMirrorings=%d | "
                       "VPNTunnels=%d | VPNGateways=%d | TargetVPNGateways=%d | "
                       "NetworkAttachments=%d | Firewalls=%d | ForwardingRules=%d | "
                       "Subnetworks=%d | Networks=%d",
                       project_id, region,
                       len(deleted_resources["deleted_addresses"]),
                       len(deleted_resources["deleted_global_addresses"]),
                       len(deleted_resources["deleted_packet_mirrorings"]),
                       len(deleted_resources["deleted_vpn_tunnels"]),
                       len(deleted_resources["deleted_vpn_gateways"]),
                       len(deleted_resources["deleted_target_vpn_gateways"]),
                       len(deleted_resources["deleted_network_attachments"]),
                       len(deleted_resources["deleted_firewalls"]),
                       len(deleted_resources["deleted_forwarding_rules"]),
                       len(deleted_resources["deleted_subnetworks"]),
                       len(deleted_resources["deleted_networks"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "VPC_NETWORK_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "VPC_NETWORK_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during VPC Network deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "VPC_NETWORK_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during VPC Network deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "VPC_NETWORK_TERMINATION",
                "reason": "UnhandledException",
                "project_id": project_id,
                "region": region,
                "detail": str(e),
            })

    return results
