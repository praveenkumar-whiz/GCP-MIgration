import json
import logging
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-eventarc-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Eventarc Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Eventarc cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region_name (required)
    - gcp_access_token / access_token (required)
    """
    cleanup_event = spec
    logger.info("GCP Eventarc Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'region' and 'region_name' keys
    region = cleanup_event.get("region_name") or cleanup_event.get("region")
    if not region:
        return {
            "status": "FAILED",
            "action": "EVENTARC_TERMINATION",
            "reason": "MissingRegion",
        }

    # Ensure region is a string
    if not isinstance(region, str):
        return {
            "status": "FAILED",
            "action": "EVENTARC_TERMINATION",
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
        "deleted_triggers": [],
        "deleted_channels": [],
        "deleted_message_buses": [],
        "deleted_pipelines": [],
        "total_triggers": 0,
        "total_channels": 0,
        "total_message_buses": 0,
        "total_pipelines": 0,
    }

    try:
        # ----------------------------------------------------------------
        # Client connection to GCP Eventarc Service
        # ----------------------------------------------------------------
        eventarc_client = GCPClientFactory.create(
            "eventarc",
            creds=client_creds,
        )

        logger.info("Connected to GCP Eventarc services | Project=%s | Region=%s", project_id, region)

        logger.info("=== GCP EVENTARC FULL CLEANUP STARTED | Project=%s | Region=%s ===", project_id, region)

        # ========================================================
        # STEP 1: Delete Triggers (Standard)
        # ========================================================
        logger.info("=== STEP 1: Discovering Eventarc Triggers ===")
        try:
            all_triggers = []
            page_token = None

            parent = "projects/{}/locations/{}".format(project_id, region)

            while True:
                params = {
                    "pageSize": 100
                }
                if page_token:
                    params["pageToken"] = page_token

                response = eventarc_client.request(
                    "GET",
                    "{}/triggers".format(parent),
                    params=params,
                )

                items = response.get("triggers", [])
                all_triggers.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_triggers"] = len(all_triggers)
            logger.info("Eventarc Triggers discovered | Count=%d", len(all_triggers))

            for trigger in all_triggers:
                trigger_name = trigger.get("name")
                if not trigger_name:
                    continue

                try:
                    eventarc_client.request(
                        "DELETE",
                        trigger_name,
                        params={"validateOnly": "false"},
                    )
                    deleted_resources["deleted_triggers"].append(trigger_name)
                    logger.info("Eventarc Trigger deleted: %s", trigger_name)
                except GCPAPIError as e:
                    logger.warning("Eventarc Trigger deletion failed for %s: %s", trigger_name, str(e))

        except GCPAPIError as e:
            logger.error("List Eventarc Triggers failed: %s", str(e))
        except Exception as e:
            logger.error("Eventarc Triggers processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 2: Delete Channels (Standard)
        # ========================================================
        logger.info("=== STEP 2: Discovering Eventarc Channels ===")
        try:
            all_channels = []
            page_token = None

            parent = "projects/{}/locations/{}".format(project_id, region)

            while True:
                params = {
                    "pageSize": 100
                }
                if page_token:
                    params["pageToken"] = page_token

                response = eventarc_client.request(
                    "GET",
                    "{}/channels".format(parent),
                    params=params,
                )

                items = response.get("channels", [])
                all_channels.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_channels"] = len(all_channels)
            logger.info("Eventarc Channels discovered | Count=%d", len(all_channels))

            for channel in all_channels:
                channel_name = channel.get("name")
                if not channel_name:
                    continue

                try:
                    eventarc_client.request(
                        "DELETE",
                        channel_name,
                        params={"validateOnly": "false"},
                    )
                    deleted_resources["deleted_channels"].append(channel_name)
                    logger.info("Eventarc Channel deleted: %s", channel_name)
                except GCPAPIError as e:
                    logger.warning("Eventarc Channel deletion failed for %s: %s", channel_name, str(e))

        except GCPAPIError as e:
            logger.error("List Eventarc Channels failed: %s", str(e))
        except Exception as e:
            logger.error("Eventarc Channels processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 3: Delete Message Buses (Advanced)
        # ========================================================
        logger.info("=== STEP 3: Discovering Eventarc Message Buses ===")
        try:
            all_message_buses = []
            page_token = None

            parent = "projects/{}/locations/{}".format(project_id, region)

            while True:
                params = {
                    "pageSize": 100
                }
                if page_token:
                    params["pageToken"] = page_token

                response = eventarc_client.request(
                    "GET",
                    "{}/messageBuses".format(parent),
                    params=params,
                )

                items = response.get("messageBuses", [])
                all_message_buses.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_message_buses"] = len(all_message_buses)
            logger.info("Eventarc Message Buses discovered | Count=%d", len(all_message_buses))

            for message_bus in all_message_buses:
                message_bus_name = message_bus.get("name")
                if not message_bus_name:
                    continue

                try:
                    eventarc_client.request(
                        "DELETE",
                        message_bus_name,
                        params={"validateOnly": "false"},
                    )
                    deleted_resources["deleted_message_buses"].append(message_bus_name)
                    logger.info("Eventarc Message Bus deleted: %s", message_bus_name)
                except GCPAPIError as e:
                    logger.warning("Eventarc Message Bus deletion failed for %s: %s", message_bus_name, str(e))

        except GCPAPIError as e:
            logger.error("List Eventarc Message Buses failed: %s", str(e))
        except Exception as e:
            logger.error("Eventarc Message Buses processing failed: %s", str(e), exc_info=True)

        # ========================================================
        # STEP 4: Delete Pipelines (Advanced)
        # ========================================================
        logger.info("=== STEP 4: Discovering Eventarc Pipelines ===")
        try:
            all_pipelines = []
            page_token = None

            parent = "projects/{}/locations/{}".format(project_id, region)

            while True:
                params = {
                    "pageSize": 100
                }
                if page_token:
                    params["pageToken"] = page_token

                response = eventarc_client.request(
                    "GET",
                    "{}/pipelines".format(parent),
                    params=params,
                )

                items = response.get("pipelines", [])
                all_pipelines.extend(items)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            deleted_resources["total_pipelines"] = len(all_pipelines)
            logger.info("Eventarc Pipelines discovered | Count=%d", len(all_pipelines))

            for pipeline in all_pipelines:
                pipeline_name = pipeline.get("name")
                if not pipeline_name:
                    continue

                try:
                    eventarc_client.request(
                        "DELETE",
                        pipeline_name,
                    )
                    deleted_resources["deleted_pipelines"].append(pipeline_name)
                    logger.info("Eventarc Pipeline deleted: %s", pipeline_name)
                except GCPAPIError as e:
                    logger.warning("Eventarc Pipeline deletion failed for %s: %s", pipeline_name, str(e))

        except GCPAPIError as e:
            logger.error("List Eventarc Pipelines failed: %s", str(e))
        except Exception as e:
            logger.error("Eventarc Pipelines processing failed: %s", str(e), exc_info=True)

        # ------------------------------------------------------------
        # Workflow completed successfully
        # ------------------------------------------------------------
        logger.info("GCP Eventarc cleanup COMPLETED | Project=%s | Region=%s | "
                   "Triggers=%d | Channels=%d | MessageBuses=%d | Pipelines=%d",
                   project_id, region,
                   len(deleted_resources["deleted_triggers"]),
                   len(deleted_resources["deleted_channels"]),
                   len(deleted_resources["deleted_message_buses"]),
                   len(deleted_resources["deleted_pipelines"]))

        results.append({
            "status": "SUCCESS",
            "action": "EVENTARC_TERMINATION",
            "project_id": project_id,
            "region": region,
            "data": deleted_resources
        })

    except GCPAPIError as e:
        logger.error("GCP API Error during Eventarc deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "EVENTARC_TERMINATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(e.message),
            "http_status": e.status_code,
        })
    except Exception as e:
        logger.error("Unhandled exception during Eventarc deletion", exc_info=True)
        results.append({
            "status": "FAILED",
            "action": "EVENTARC_TERMINATION",
            "reason": "UnhandledException",
            "project_id": project_id,
            "region": region,
            "detail": str(e),
        })

    return results[0] if results else {
        "status": "FAILED",
        "action": "EVENTARC_TERMINATION",
        "reason": "NoResults"
    }
