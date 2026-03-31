import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-vm-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Compute Engine Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Compute Engine cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Compute Engine Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "COMPUTE_ENGINE_TERMINATION",
            "reason": "MissingRegion",
        }

    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "COMPUTE_ENGINE_TERMINATION",
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
            "deleted_regional_autoscalers": [],
            "deleted_regional_instance_groups": [],
            "deleted_instance_groups": [],
            "deleted_instance_group_managers": [],
            "deleted_instances": [],
            "deleted_disks": [],
            "deleted_regional_disks": [],
            "deleted_snapshots": [],
            "deleted_instant_snapshots": [],
            "deleted_snapshot_schedules": [],
            "deleted_regional_instance_templates": [],
            "deleted_instance_templates": [],
            "deleted_images": [],
            "deleted_machine_images": [],
            "deleted_security_policies": [],
            "total_regional_autoscalers": 0,
            "total_regional_instance_groups": 0,
            "total_instance_groups": 0,
            "total_instance_group_managers": 0,
            "total_instances": 0,
            "total_disks": 0,
            "total_regional_disks": 0,
            "total_snapshots": 0,
            "total_instant_snapshots": 0,
            "total_snapshot_schedules": 0,
            "total_regional_instance_templates": 0,
            "total_instance_templates": 0,
            "total_images": 0,
            "total_machine_images": 0,
            "total_security_policies": 0,
            "failed_deletions": [],
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

            logger.info("=== GCP COMPUTE ENGINE FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: Delete Regional Instance Groups & Autoscalers
            # ========================================================
            logger.info("=== STEP 1: Discovering Regional Instance Groups ===")
            try:
                all_regional_instance_groups = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/instanceGroupManagers".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_regional_instance_groups.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_regional_instance_groups"] = len(all_regional_instance_groups)
                logger.info("Regional Instance Groups discovered | Count=%d", len(all_regional_instance_groups))

                for instance_group in all_regional_instance_groups:
                    instance_group_name = instance_group.get("name")
                    if not instance_group_name:
                        continue

                    # Try to delete autoscaler first
                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/autoscalers/{}".format(project_id, region, instance_group_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_regional_autoscalers"].append(instance_group_name)
                        deleted_resources["total_regional_autoscalers"] += 1
                        logger.info("Regional Autoscaler deleted: %s", instance_group_name)
                    except GCPAPIError as e:
                        # Autoscaler might not exist, continue
                        logger.debug("Autoscaler not found or already deleted: %s", instance_group_name)

                    # Delete Regional Instance Group Manager
                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/instanceGroupManagers/{}".format(project_id, region, instance_group_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_regional_instance_groups"].append(instance_group_name)
                        logger.info("Regional Instance Group Manager deleted: %s", instance_group_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Regional Instance Group Manager",
                            "resource_name": instance_group_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Regional Instance Group Manager deletion failed for %s: %s", instance_group_name, str(e))

            except GCPAPIError as e:
                logger.error("List Regional Instance Groups failed: %s", str(e))
            except Exception as e:
                logger.error("Regional Instance Groups processing failed: %s", str(e), exc_info=True)

            # Wait for propagation only if resources were deleted
            if len(deleted_resources["deleted_regional_instance_groups"]) > 0 or deleted_resources["total_regional_autoscalers"] > 0:
                logger.info("Waiting 10 seconds for regional instance group deletions to propagate")
                time.sleep(10)

            # ========================================================
            # STEP 2: List All Zones in Region
            # ========================================================
            logger.info("=== STEP 2: Discovering Zones in Region ===")
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

            # ========================================================
            # STEP 3: Delete Instance Groups in Each Zone
            # ========================================================
            logger.info("=== STEP 3: Discovering Instance Groups in Zones ===")
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
                                    deleted_resources["failed_deletions"].append({
                                        "resource_type": "Instance Group Manager",
                                        "resource_name": instance_group_name,
                                        "zone": zone_name,
                                        "error": str(mgr_error)
                                    })
                                    logger.warning("Instance Group Manager deletion failed for %s in zone %s: %s", 
                                                 instance_group_name, zone_name, str(mgr_error))
                            else:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Instance Group",
                                    "resource_name": instance_group_name,
                                    "zone": zone_name,
                                    "error": str(e)
                                })
                                logger.warning("Instance Group deletion failed for %s in zone %s: %s", 
                                             instance_group_name, zone_name, str(e))

                except GCPAPIError as e:
                    logger.error("List Instance Groups failed in zone %s: %s", zone_name, str(e))
                except Exception as e:
                    logger.error("Instance Groups processing failed in zone %s: %s", zone_name, str(e), exc_info=True)

            # Wait for propagation only if resources were deleted
            if len(deleted_resources["deleted_instance_groups"]) > 0 or deleted_resources["total_instance_group_managers"] > 0:
                logger.info("Waiting 10 seconds for instance group deletions to propagate")
                time.sleep(10)

            # ========================================================
            # STEP 4: Delete VM Instances in Each Zone
            # ========================================================
            logger.info("=== STEP 4: Discovering VM Instances in Zones ===")
            for zone_name in all_zones:
                try:
                    all_instances = []
                    page_token = None

                    while True:
                        params = {}
                        if page_token:
                            params["pageToken"] = page_token

                        response = compute_client.request(
                            "GET",
                            "projects/{}/zones/{}/instances".format(project_id, zone_name),
                            params=params,
                        )

                        items = response.get("items", [])
                        all_instances.extend(items)

                        page_token = response.get("nextPageToken")
                        if not page_token:
                            break

                    deleted_resources["total_instances"] += len(all_instances)
                    logger.info("VM Instances discovered in zone %s | Count=%d", zone_name, len(all_instances))

                    for instance in all_instances:
                        instance_name = instance.get("name")
                        if not instance_name:
                            continue

                        # Check if deletion protection is enabled
                        deletion_protected = instance.get("deletionProtection", False)
                        
                        # Disable deletion protection if enabled
                        if deletion_protected:
                            try:
                                compute_client.request(
                                    "POST",
                                    "projects/{}/zones/{}/instances/{}/setDeletionProtection".format(project_id, zone_name, instance_name),
                                    params={
                                        "deletionProtection": "false",
                                        "requestId": str(uuid.uuid4())
                                    },
                                )
                                logger.info("Deletion protection disabled for VM Instance: %s in zone %s", instance_name, zone_name)
                            except GCPAPIError as e:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "VM Instance (Disable Protection)",
                                    "resource_name": instance_name,
                                    "zone": zone_name,
                                    "error": str(e)
                                })
                                logger.warning("Failed to disable deletion protection for %s in zone %s: %s", 
                                             instance_name, zone_name, str(e))
                                continue

                        # Delete VM Instance
                        try:
                            compute_client.request(
                                "DELETE",
                                "projects/{}/zones/{}/instances/{}".format(project_id, zone_name, instance_name),
                                params={"requestId": str(uuid.uuid4())},
                            )
                            deleted_resources["deleted_instances"].append({
                                "name": instance_name,
                                "zone": zone_name
                            })
                            logger.info("VM Instance deleted: %s in zone %s", instance_name, zone_name)
                        except GCPAPIError as e:
                            deleted_resources["failed_deletions"].append({
                                "resource_type": "VM Instance",
                                "resource_name": instance_name,
                                "zone": zone_name,
                                "error": str(e)
                            })
                            logger.warning("VM Instance deletion failed for %s in zone %s: %s", 
                                         instance_name, zone_name, str(e))

                except GCPAPIError as e:
                    logger.error("List VM Instances failed in zone %s: %s", zone_name, str(e))
                except Exception as e:
                    logger.error("VM Instances processing failed in zone %s: %s", zone_name, str(e), exc_info=True)

            # Wait for instance terminations to propagate only if instances were deleted
            if len(deleted_resources["deleted_instances"]) > 0:
                logger.info("Waiting 30 seconds for VM instance terminations to propagate")
                time.sleep(30)

            # ========================================================
            # STEP 5: Delete Disks in Each Zone
            # ========================================================
            logger.info("=== STEP 5: Discovering Disks in Zones ===")
            for zone_name in all_zones:
                try:
                    all_disks = []
                    page_token = None

                    while True:
                        params = {}
                        if page_token:
                            params["pageToken"] = page_token

                        response = compute_client.request(
                            "GET",
                            "projects/{}/zones/{}/disks".format(project_id, zone_name),
                            params=params,
                        )

                        items = response.get("items", [])
                        all_disks.extend(items)

                        page_token = response.get("nextPageToken")
                        if not page_token:
                            break

                    deleted_resources["total_disks"] += len(all_disks)
                    logger.info("Disks discovered in zone %s | Count=%d", zone_name, len(all_disks))

                    for disk in all_disks:
                        disk_name = disk.get("name")
                        if not disk_name:
                            continue

                        try:
                            compute_client.request(
                                "DELETE",
                                "projects/{}/zones/{}/disks/{}".format(project_id, zone_name, disk_name),
                                params={"requestId": str(uuid.uuid4())},
                            )
                            deleted_resources["deleted_disks"].append({
                                "name": disk_name,
                                "zone": zone_name
                            })
                            logger.info("Disk deleted: %s in zone %s", disk_name, zone_name)
                        except GCPAPIError as e:
                            deleted_resources["failed_deletions"].append({
                                "resource_type": "Disk",
                                "resource_name": disk_name,
                                "zone": zone_name,
                                "error": str(e)
                            })
                            logger.warning("Disk deletion failed for %s in zone %s: %s", 
                                         disk_name, zone_name, str(e))

                except GCPAPIError as e:
                    logger.error("List Disks failed in zone %s: %s", zone_name, str(e))
                except Exception as e:
                    logger.error("Disks processing failed in zone %s: %s", zone_name, str(e), exc_info=True)

            # ========================================================
            # STEP 6: Delete Regional Disks
            # ========================================================
            logger.info("=== STEP 6: Discovering Regional Disks ===")
            try:
                all_regional_disks = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/disks".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_regional_disks.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_regional_disks"] = len(all_regional_disks)
                logger.info("Regional Disks discovered | Count=%d", len(all_regional_disks))

                for disk in all_regional_disks:
                    disk_name = disk.get("name")
                    if not disk_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/disks/{}".format(project_id, region, disk_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_regional_disks"].append(disk_name)
                        logger.info("Regional Disk deleted: %s", disk_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Regional Disk",
                            "resource_name": disk_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Regional Disk deletion failed for %s: %s", disk_name, str(e))

            except GCPAPIError as e:
                logger.error("List Regional Disks failed: %s", str(e))
            except Exception as e:
                logger.error("Regional Disks processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 7: Delete Snapshots (Standard & Archive)
            # ========================================================
            logger.info("=== STEP 7: Discovering Snapshots (Standard & Archive) ===")
            try:
                all_snapshots = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/snapshots".format(project_id),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_snapshots.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_snapshots"] = len(all_snapshots)
                logger.info("Snapshots discovered (Standard & Archive) | Count=%d", len(all_snapshots))

                for snapshot in all_snapshots:
                    snapshot_name = snapshot.get("name")
                    if not snapshot_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/global/snapshots/{}".format(project_id, snapshot_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_snapshots"].append(snapshot_name)
                        logger.info("Snapshot deleted: %s", snapshot_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Snapshot",
                            "resource_name": snapshot_name,
                            "error": str(e)
                        })
                        logger.warning("Snapshot deletion failed for %s: %s", snapshot_name, str(e))

            except GCPAPIError as e:
                logger.error("List Snapshots failed: %s", str(e))
            except Exception as e:
                logger.error("Snapshots processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 7A: Delete Instant Snapshots (Zonal)
            # ========================================================
            logger.info("=== STEP 7A: Discovering Instant Snapshots (Zonal) ===")
            
            for zone_name in all_zones:
                try:
                    all_instant_snapshots = []
                    page_token = None

                    while True:
                        params = {}
                        if page_token:
                            params["pageToken"] = page_token

                        response = compute_client.request(
                            "GET",
                            "projects/{}/zones/{}/instantSnapshots".format(project_id, zone_name),
                            params=params,
                        )

                        items = response.get("items", [])
                        all_instant_snapshots.extend(items)

                        page_token = response.get("nextPageToken")
                        if not page_token:
                            break

                    deleted_resources["total_instant_snapshots"] += len(all_instant_snapshots)
                    
                    if len(all_instant_snapshots) > 0:
                        logger.info("Instant Snapshots discovered in zone %s | Count=%d", zone_name, len(all_instant_snapshots))

                    for instant_snapshot in all_instant_snapshots:
                        instant_snapshot_name = instant_snapshot.get("name")
                        if not instant_snapshot_name:
                            continue

                        try:
                            compute_client.request(
                                "DELETE",
                                "projects/{}/zones/{}/instantSnapshots/{}".format(project_id, zone_name, instant_snapshot_name),
                                params={"requestId": str(uuid.uuid4())},
                            )
                            deleted_resources["deleted_instant_snapshots"].append({
                                "name": instant_snapshot_name,
                                "zone": zone_name
                            })
                            logger.info("Instant Snapshot deleted: %s in zone %s", instant_snapshot_name, zone_name)
                        except GCPAPIError as e:
                            deleted_resources["failed_deletions"].append({
                                "resource_type": "Instant Snapshot",
                                "resource_name": instant_snapshot_name,
                                "zone": zone_name,
                                "error": str(e)
                            })
                            logger.warning("Instant Snapshot deletion failed for %s in zone %s: %s", 
                                         instant_snapshot_name, zone_name, str(e))

                except GCPAPIError as e:
                    logger.error("List Instant Snapshots failed in zone %s: %s", zone_name, str(e))
                except Exception as e:
                    logger.error("Instant Snapshots processing failed in zone %s: %s", zone_name, str(e), exc_info=True)

            logger.info("Total Instant Snapshots | Discovered=%d | Deleted=%d", 
                       deleted_resources["total_instant_snapshots"], 
                       len(deleted_resources["deleted_instant_snapshots"]))

            # ========================================================
            # STEP 7B: Delete Snapshot Schedules (Resource Policies)
            # ========================================================
            logger.info("=== STEP 7B: Discovering Snapshot Schedules (Resource Policies) ===")
            try:
                all_resource_policies = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/resourcePolicies".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_resource_policies.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                # Filter only snapshot schedule policies
                snapshot_schedule_policies = []
                for policy in all_resource_policies:
                    # Check if policy has snapshotSchedulePolicy field
                    if policy.get("snapshotSchedulePolicy"):
                        snapshot_schedule_policies.append(policy)

                deleted_resources["total_snapshot_schedules"] = len(snapshot_schedule_policies)
                logger.info("Snapshot Schedules discovered | Count=%d", len(snapshot_schedule_policies))

                for policy in snapshot_schedule_policies:
                    policy_name = policy.get("name")
                    if not policy_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/resourcePolicies/{}".format(project_id, region, policy_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_snapshot_schedules"].append(policy_name)
                        logger.info("Snapshot Schedule deleted: %s", policy_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Snapshot Schedule",
                            "resource_name": policy_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Snapshot Schedule deletion failed for %s: %s", policy_name, str(e))

            except GCPAPIError as e:
                logger.error("List Snapshot Schedules failed: %s", str(e))
            except Exception as e:
                logger.error("Snapshot Schedules processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 8: Delete Regional Instance Templates
            # ========================================================
            logger.info("=== STEP 8: Discovering Regional Instance Templates ===")
            try:
                all_regional_templates = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/regions/{}/instanceTemplates".format(project_id, region),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_regional_templates.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_regional_instance_templates"] = len(all_regional_templates)
                logger.info("Regional Instance Templates discovered | Count=%d", len(all_regional_templates))

                # Wait before deleting templates only if templates exist
                if len(all_regional_templates) > 0:
                    logger.info("Waiting 10 seconds before deleting regional instance templates")
                    time.sleep(10)

                for template in all_regional_templates:
                    template_name = template.get("name")
                    if not template_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/regions/{}/instanceTemplates/{}".format(project_id, region, template_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_regional_instance_templates"].append(template_name)
                        logger.info("Regional Instance Template deleted: %s", template_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Regional Instance Template",
                            "resource_name": template_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Regional Instance Template deletion failed for %s: %s", template_name, str(e))

            except GCPAPIError as e:
                logger.error("List Regional Instance Templates failed: %s", str(e))
            except Exception as e:
                logger.error("Regional Instance Templates processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 9: Delete Global Instance Templates
            # ========================================================
            logger.info("=== STEP 9: Discovering Global Instance Templates ===")
            try:
                all_instance_templates = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/instanceTemplates".format(project_id),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_instance_templates.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_instance_templates"] = len(all_instance_templates)
                logger.info("Global Instance Templates discovered | Count=%d", len(all_instance_templates))

                for template in all_instance_templates:
                    template_name = template.get("name")
                    if not template_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/global/instanceTemplates/{}".format(project_id, template_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_instance_templates"].append(template_name)
                        logger.info("Global Instance Template deleted: %s", template_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Global Instance Template",
                            "resource_name": template_name,
                            "error": str(e)
                        })
                        logger.warning("Global Instance Template deletion failed for %s: %s", template_name, str(e))

            except GCPAPIError as e:
                logger.error("List Global Instance Templates failed: %s", str(e))
            except Exception as e:
                logger.error("Global Instance Templates processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 10: Delete Compute Images
            # ========================================================
            logger.info("=== STEP 10: Discovering Compute Images ===")
            try:
                all_images = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/images".format(project_id),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_images.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_images"] = len(all_images)
                logger.info("Compute Images discovered | Count=%d", len(all_images))

                for image in all_images:
                    image_name = image.get("name")
                    if not image_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/global/images/{}".format(project_id, image_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_images"].append(image_name)
                        logger.info("Compute Image deleted: %s", image_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Compute Image",
                            "resource_name": image_name,
                            "error": str(e)
                        })
                        logger.warning("Compute Image deletion failed for %s: %s", image_name, str(e))

            except GCPAPIError as e:
                logger.error("List Compute Images failed: %s", str(e))
            except Exception as e:
                logger.error("Compute Images processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 11: Delete Machine Images
            # ========================================================
            logger.info("=== STEP 11: Discovering Machine Images ===")
            try:
                all_machine_images = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/machineImages".format(project_id),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_machine_images.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_machine_images"] = len(all_machine_images)
                logger.info("Machine Images discovered | Count=%d", len(all_machine_images))

                for machine_image in all_machine_images:
                    machine_image_name = machine_image.get("name")
                    if not machine_image_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/global/machineImages/{}".format(project_id, machine_image_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_machine_images"].append(machine_image_name)
                        logger.info("Machine Image deleted: %s", machine_image_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Machine Image",
                            "resource_name": machine_image_name,
                            "error": str(e)
                        })
                        logger.warning("Machine Image deletion failed for %s: %s", machine_image_name, str(e))

            except GCPAPIError as e:
                logger.error("List Machine Images failed: %s", str(e))
            except Exception as e:
                logger.error("Machine Images processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 12: Delete Cloud Security Policies
            # ========================================================
            logger.info("=== STEP 12: Discovering Cloud Security Policies ===")
            deleted_resources["deleted_security_policies"] = []
            deleted_resources["total_security_policies"] = 0
            
            try:
                all_security_policies = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = compute_client.request(
                        "GET",
                        "projects/{}/global/securityPolicies".format(project_id),
                        params=params,
                    )

                    items = response.get("items", [])
                    all_security_policies.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_security_policies"] = len(all_security_policies)
                logger.info("Cloud Security Policies discovered | Count=%d", len(all_security_policies))

                for policy in all_security_policies:
                    policy_name = policy.get("name")
                    if not policy_name:
                        continue

                    try:
                        compute_client.request(
                            "DELETE",
                            "projects/{}/global/securityPolicies/{}".format(project_id, policy_name),
                            params={"requestId": str(uuid.uuid4())},
                        )
                        deleted_resources["deleted_security_policies"].append(policy_name)
                        logger.info("Cloud Security Policy deleted: %s", policy_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Cloud Security Policy",
                            "resource_name": policy_name,
                            "error": str(e)
                        })
                        logger.warning("Cloud Security Policy deletion failed for %s: %s", policy_name, str(e))

            except GCPAPIError as e:
                logger.error("List Cloud Security Policies failed: %s", str(e))
            except Exception as e:
                logger.error("Cloud Security Policies processing failed: %s", str(e), exc_info=True)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Compute Engine cleanup COMPLETED | Project=%s | Region=%s | "
                       "RegionalAutoscalers=%d | RegionalInstanceGroups=%d | InstanceGroups=%d | "
                       "InstanceGroupManagers=%d | Instances=%d | Disks=%d | RegionalDisks=%d | "
                       "Snapshots=%d | InstantSnapshots=%d | SnapshotSchedules=%d | "
                       "RegionalInstanceTemplates=%d | InstanceTemplates=%d | "
                       "Images=%d | MachineImages=%d | SecurityPolicies=%d",
                       project_id, region,
                       len(deleted_resources["deleted_regional_autoscalers"]),
                       len(deleted_resources["deleted_regional_instance_groups"]),
                       len(deleted_resources["deleted_instance_groups"]),
                       len(deleted_resources["deleted_instance_group_managers"]),
                       len(deleted_resources["deleted_instances"]),
                       len(deleted_resources["deleted_disks"]),
                       len(deleted_resources["deleted_regional_disks"]),
                       len(deleted_resources["deleted_snapshots"]),
                       len(deleted_resources["deleted_instant_snapshots"]),
                       len(deleted_resources["deleted_snapshot_schedules"]),
                       len(deleted_resources["deleted_regional_instance_templates"]),
                       len(deleted_resources["deleted_instance_templates"]),
                       len(deleted_resources["deleted_images"]),
                       len(deleted_resources["deleted_machine_images"]),
                       len(deleted_resources["deleted_security_policies"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "COMPUTE_ENGINE_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "COMPUTE_ENGINE_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Compute Engine deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "COMPUTE_ENGINE_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Compute Engine deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "COMPUTE_ENGINE_TERMINATION",
                "reason": "UnhandledException",
                "project_id": project_id,
                "region": region,
                "detail": str(e),
            })

    return results
