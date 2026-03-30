import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-monitoring-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Cloud Monitoring Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Cloud Monitoring cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (optional - Cloud Monitoring is global)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Cloud Monitoring Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Cloud Monitoring is global, but we support region for consistency
    region = cleanup_event.get("region_name") or cleanup_event.get("region") or "global"
    
    # Support both single region string and multiple regions list
    regions = [region] if isinstance(region, str) else region
    
    # Ensure regions is a list
    if not isinstance(regions, list) or not regions:
        return {
            "status": "FAILED",
            "action": "CLOUD_MONITORING_TERMINATION",
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
            "deleted_dashboards": [],
            "deleted_alert_policies": [],
            "deleted_notification_channels": [],
            "total_dashboards": 0,
            "total_alert_policies": 0,
            "total_notification_channels": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connections to GCP Cloud Monitoring Services
            # ----------------------------------------------------------------
            monitoring_client = GCPClientFactory.create(
                "monitoring",
                creds=client_creds,
            )

            logger.info("Connected to GCP Cloud Monitoring service | Project=%s | Region=%s", project_id, region)

            logger.info("=== GCP CLOUD MONITORING FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

            # ========================================================
            # STEP 1: Delete Monitoring Dashboards
            # ========================================================
            logger.info("=== STEP 1: Discovering Monitoring Dashboards ===")
            try:
                all_dashboards = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    # Dashboards API uses v1 endpoint (different from v3)
                    # We need to go back from /v3 to root and use /v1
                    # Note: This may fail if Monitoring Dashboards API is not enabled
                    response = monitoring_client.request(
                        "GET",
                        "../v1/projects/{}/dashboards".format(project_id),
                        params=params,
                    )

                    items = response.get("dashboards", [])
                    all_dashboards.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_dashboards"] = len(all_dashboards)
                logger.info("Monitoring Dashboards discovered | Count=%d", len(all_dashboards))

                for dashboard in all_dashboards:
                    dashboard_name = dashboard.get("name")
                    if not dashboard_name:
                        continue

                    try:
                        # Dashboard name already includes the full path, but we need v1 not v3
                        # Extract the path after 'projects/' and rebuild with ../v1/
                        monitoring_client.request(
                            "DELETE",
                            "../v1/{}".format(dashboard_name),
                        )
                        deleted_resources["deleted_dashboards"].append(dashboard_name)
                        logger.info("Monitoring Dashboard deleted: %s", dashboard_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Monitoring Dashboard",
                            "resource_name": dashboard_name,
                            "error": str(e)
                        })
                        logger.warning("Monitoring Dashboard deletion failed for %s: %s", dashboard_name, str(e))

            except GCPAPIError as e:
                # Dashboards API might not be enabled or accessible
                if e.status_code == 403:
                    logger.warning("Dashboards API not accessible (403) - may not be enabled or requires additional permissions: %s", str(e))
                else:
                    logger.error("List Monitoring Dashboards failed: %s", str(e))
            except Exception as e:
                logger.error("Monitoring Dashboards processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 2: Delete Alert Policies
            # ========================================================
            logger.info("=== STEP 2: Discovering Alert Policies ===")
            try:
                all_alert_policies = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = monitoring_client.request(
                        "GET",
                        "projects/{}/alertPolicies".format(project_id),
                        params=params,
                    )

                    items = response.get("alertPolicies", [])
                    all_alert_policies.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_alert_policies"] = len(all_alert_policies)
                logger.info("Alert Policies discovered | Count=%d", len(all_alert_policies))

                for alert_policy in all_alert_policies:
                    alert_policy_name = alert_policy.get("name")
                    if not alert_policy_name:
                        continue

                    try:
                        monitoring_client.request(
                            "DELETE",
                            "{}".format(alert_policy_name),
                        )
                        deleted_resources["deleted_alert_policies"].append(alert_policy_name)
                        logger.info("Alert Policy deleted: %s", alert_policy_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Alert Policy",
                            "resource_name": alert_policy_name,
                            "error": str(e)
                        })
                        logger.warning("Alert Policy deletion failed for %s: %s", alert_policy_name, str(e))

            except GCPAPIError as e:
                logger.error("List Alert Policies failed: %s", str(e))
            except Exception as e:
                logger.error("Alert Policies processing failed: %s", str(e), exc_info=True)

            # ========================================================
            # STEP 3: Delete Notification Channels
            # ========================================================
            logger.info("=== STEP 3: Discovering Notification Channels ===")
            try:
                all_notification_channels = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = monitoring_client.request(
                        "GET",
                        "projects/{}/notificationChannels".format(project_id),
                        params=params,
                    )

                    items = response.get("notificationChannels", [])
                    all_notification_channels.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_notification_channels"] = len(all_notification_channels)
                logger.info("Notification Channels discovered | Count=%d", len(all_notification_channels))

                for notification_channel in all_notification_channels:
                    notification_channel_name = notification_channel.get("name")
                    if not notification_channel_name:
                        continue

                    try:
                        # Add force parameter to delete channels even if referenced by alert policies
                        monitoring_client.request(
                            "DELETE",
                            "{}".format(notification_channel_name),
                            params={"force": "true"},
                        )
                        deleted_resources["deleted_notification_channels"].append(notification_channel_name)
                        logger.info("Notification Channel deleted: %s", notification_channel_name)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Notification Channel",
                            "resource_name": notification_channel_name,
                            "error": str(e)
                        })
                        logger.warning("Notification Channel deletion failed for %s: %s", notification_channel_name, str(e))

            except GCPAPIError as e:
                logger.error("List Notification Channels failed: %s", str(e))
            except Exception as e:
                logger.error("Notification Channels processing failed: %s", str(e), exc_info=True)

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Cloud Monitoring cleanup COMPLETED | Project=%s | Region=%s | "
                       "Dashboards=%d | AlertPolicies=%d | NotificationChannels=%d",
                       project_id, region,
                       len(deleted_resources["deleted_dashboards"]),
                       len(deleted_resources["deleted_alert_policies"]),
                       len(deleted_resources["deleted_notification_channels"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "CLOUD_MONITORING_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "CLOUD_MONITORING_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Cloud Monitoring deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "CLOUD_MONITORING_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Cloud Monitoring deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "CLOUD_MONITORING_TERMINATION",
                "reason": "UnhandledException",
                "project_id": project_id,
                "region": region,
                "detail": str(e),
            })

    return results
