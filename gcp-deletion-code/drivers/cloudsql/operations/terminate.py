import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-cloudsql-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Cloud SQL Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Cloud SQL cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name / region (optional - Cloud SQL is global)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Cloud SQL Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Cloud SQL is global, but we support region for consistency
    region = cleanup_event.get("region_name") or cleanup_event.get("region") or "global"

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
        "deleted_instances": [],
        "disabled_protection": [],
        "disabled_backups": [],
        "deleted_backups": [],
        "total_instances": 0,
        "total_backups": 0,
    }

    try:
        # ----------------------------------------------------------------
        # Client connection to GCP Cloud SQL Service
        # ----------------------------------------------------------------
        sqladmin_client = GCPClientFactory.create(
            "sqladmin",
            creds=client_creds,
        )

        logger.info("Connected to GCP Cloud SQL service | Project=%s", project_id)

        logger.info("=== GCP CLOUD SQL FULL CLEANUP STARTED | Project=%s ===", project_id)

        # ========================================================
        # Discover all Cloud SQL Instances
        # ========================================================
        logger.info("Discovering Cloud SQL Instances...")
        
        all_instances = []
        page_token = None

        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token

            response = sqladmin_client.request(
                "GET",
                "projects/{}/instances".format(project_id),
                params=params,
            )

            items = response.get("items", [])
            all_instances.extend(items)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        deleted_resources["total_instances"] = len(all_instances)
        logger.info("Cloud SQL Instances discovered | Count=%d", len(all_instances))

        # ========================================================
        # STEP 1: Disable Deletetion Protection and Backups for All Instances
        # ========================================================
        logger.info("=== STEP 1: Disabling Deletetion Protection and Backups ===")
        
        for instance in all_instances:
            instance_name = instance.get("name")
            if not instance_name:
                continue

            logger.info("Processing instance: %s", instance_name)

            try:
                # Get instance details
                instance_details = sqladmin_client.request(
                    "GET",
                    "projects/{}/instances/{}".format(project_id, instance_name),
                )
                
                instance_settings = instance_details.get("settings", {})
                tier = instance_settings.get("tier")
                settings_version = instance_settings.get("settingsVersion")
                edition = instance_settings.get("edition", "ENTERPRISE")
                deletion_protection = instance_settings.get("deletionProtectionEnabled", False)
                backup_config = instance_settings.get("backupConfiguration", {})
                backup_enabled = backup_config.get("enabled", False)

                # Prepare update body
                update_needed = False
                update_body = {
                    "settings": {
                        "tier": tier,
                        "settingsVersion": settings_version,
                        "edition": edition
                    }
                }

                # Disable deletion protection if enabled
                if deletion_protection:
                    logger.info("Deletion protection enabled for %s, will disable", instance_name)
                    update_body["settings"]["deletionProtectionEnabled"] = False
                    deleted_resources["disabled_protection"].append(instance_name)
                    update_needed = True

                # Disable automated backups if enabled
                if backup_enabled:
                    logger.info("Automated backups enabled for %s, will disable", instance_name)
                    update_body["settings"]["backupConfiguration"] = {
                        "enabled": False
                    }
                    deleted_resources["disabled_backups"].append(instance_name)
                    update_needed = True

                # Apply updates if needed
                if update_needed:
                    sqladmin_client.request(
                        "PATCH",
                        "projects/{}/instances/{}".format(project_id, instance_name),
                        json_body=update_body,
                    )
                    logger.info("Updated instance settings for: %s", instance_name)
                    # Wait for update to complete
                    time.sleep(30)

            except GCPAPIError as e:
                logger.warning("Failed to update instance settings for %s: %s", instance_name, str(e))

        # ========================================================
        # STEP 2: Delete SQL Instances
        # ========================================================
        logger.info("=== STEP 2: Deleting SQL Instances ===")
        
        for instance in all_instances:
            instance_name = instance.get("name")
            if not instance_name:
                continue

            logger.info("Deleting instance: %s", instance_name)

            # Delete the instance
            try:
                sqladmin_client.request(
                    "DELETE",
                    "projects/{}/instances/{}".format(project_id, instance_name),
                )
                deleted_resources["deleted_instances"].append(instance_name)
                logger.info("Cloud SQL Instance deleted: %s", instance_name)
            except GCPAPIError as e:
                logger.warning("Cloud SQL Instance deletion failed for %s: %s", instance_name, str(e))

        # Conditional sleep - only if resources were deleted
        if deleted_resources["deleted_instances"]:
            logger.info("Waiting for instance deletions to complete...")
            time.sleep(5)

        # ========================================================
        # STEP 3: Delete All Project-Level Backups (including from deleted instances)
        # ========================================================
        logger.info("=== STEP 3: Deleting All Project-Level Backups ===")
        
        try:
            # List all backups at project level (includes backups from deleted instances)
            page_token = None
            all_backups = []

            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token

                backup_response = sqladmin_client.request(
                    "GET",
                    "projects/{}/backups".format(project_id),
                    params=params,
                )

                # The response has 'backups' field, not 'items'
                items = backup_response.get("backups", [])
                all_backups.extend(items)

                page_token = backup_response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_backups"] = len(all_backups)
            logger.info("Project-level backups discovered | Count=%d", len(all_backups))

            # Delete all backups
            for backup in all_backups:
                backup_name = backup.get("name")  # Format: projects/{project}/backups/{backup_id}
                instance_name = backup.get("instance", "unknown")
                
                if not backup_name:
                    continue

                try:
                    # Extract backup ID from the name field
                    # Format: projects/{project}/backups/{backup_id}
                    backup_id = backup_name.split("/")[-1] if "/" in backup_name else backup_name
                    
                    # Use the full backup name path for deletion
                    sqladmin_client.request(
                        "DELETE",
                        backup_name,
                    )
                    deleted_resources["deleted_backups"].append("{}/{}".format(instance_name, backup_id))
                    logger.info("Backup deleted: %s (from instance: %s)", backup_id, instance_name)
                except GCPAPIError as e:
                    logger.warning("Backup deletion failed for %s: %s", backup_name, str(e))

        except GCPAPIError as e:
            logger.warning("Failed to list/delete project-level backups: %s", str(e))

        # ------------------------------------------------------------
        # Workflow completed successfully
        # ------------------------------------------------------------
        logger.info("GCP Cloud SQL cleanup COMPLETED | Project=%s | Instances=%d | Backups=%d | ProtectionDisabled=%d | BackupsDisabled=%d",
                   project_id,
                   len(deleted_resources["deleted_instances"]),
                   len(deleted_resources["deleted_backups"]),
                   len(deleted_resources["disabled_protection"]),
                   len(deleted_resources["disabled_backups"]))

        results.append({
            "status": "SUCCESS",
            "action": "CLOUD_SQL_TERMINATION",
            "project_id": project_id,
            "region": region,
            "data": deleted_resources
        })

    except GCPAPIError as e:
        logger.error("GCP API Error during Cloud SQL deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "CLOUD_SQL_TERMINATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(e.message),
            "http_status": e.status_code,
        })
    except Exception as e:
        logger.error("Unhandled exception during Cloud SQL deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "CLOUD_SQL_TERMINATION",
            "reason": "UnhandledException",
            "project_id": project_id,
            "region": region,
            "detail": str(e),
        })

    return results[0] if results else {
        "status": "FAILED",
        "action": "CLOUD_SQL_TERMINATION",
        "reason": "NoResults"
    }
