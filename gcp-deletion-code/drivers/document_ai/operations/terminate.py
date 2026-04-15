import json
import logging
import time
import uuid
from typing import Any, Dict
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-document-ai-terminate")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# GCP Document AI Deletion Function
# -------------------------------------------------------------------
def terminate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker-only GCP Document AI cleanup routine.
    This is intentionally synchronous and long-running, executed by worker pod.

    Expected spec keys:
    - project_id (required)
    - region (required) - US-based regions only (e.g., 'us-central1', 'us-east1')
    - gcp_access_token / access_token (required)
    
    Note: Currently supports US-based regions only
    
    Deletes:
    - All processors (including custom processors)
    - All processor versions (except default and pretrained versions)
    """
    cleanup_event = spec
    logger.info("GCP Document AI Deletion triggered")
    logger.info("Incoming event: %s", json.dumps(cleanup_event))

    # Support both 'location' and 'region' keys
    location = cleanup_event.get("location") or cleanup_event.get("region")
    if not location:
        return {
            "status": "FAILED",
            "action": "DOCUMENT_AI_TERMINATION",
            "reason": "MissingLocation",
        }

    # Support both single location string and multiple locations list
    locations = [location] if isinstance(location, str) else location
    
    # Ensure locations is a list
    if not isinstance(locations, list) or not locations:
        return {
            "status": "FAILED",
            "action": "DOCUMENT_AI_TERMINATION",
            "reason": "InvalidLocationFormat"
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

    for region in locations:
        deleted_resources = {
            "deleted_processor_versions": [],
            "deleted_processors": [],
            "total_processor_versions": 0,
            "total_processors": 0,
            "failed_deletions": [],
        }

        try:
            # ----------------------------------------------------------------
            # Client connection to GCP Document AI Service
            # Note: Currently supports US-based regions only
            # ----------------------------------------------------------------
            documentai_client = GCPClientFactory.create(
                "documentai",
                creds=client_creds,
            )

            # Override endpoint for US regions
            # Document AI location is "us" for all US-based regions
            location = "us"
            if region.startswith("us-"):
                documentai_client.base_url = "https://us-documentai.googleapis.com/v1"

            logger.info("Connected to GCP Document AI services | Project=%s | Region=%s | Location=%s", 
                       project_id, region, location)

            logger.info("=== GCP DOCUMENT AI FULL CLEANUP STARTED | Project=%s | Region=%s | Location=%s ===", 
                       project_id, region, location)

            # ========================================================
            # STEP 1: List All Processors
            # ========================================================
            logger.info("=== STEP 1: Discovering Processors ===")
            try:
                all_processors = []
                page_token = None

                while True:
                    params = {}
                    if page_token:
                        params["pageToken"] = page_token

                    response = documentai_client.request(
                        "GET",
                        "projects/{}/locations/{}/processors".format(project_id, location),
                        params=params,
                    )

                    items = response.get("processors", [])
                    all_processors.extend(items)

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                deleted_resources["total_processors"] = len(all_processors)
                logger.info("Processors discovered | Count=%d", len(all_processors))

                # ========================================================
                # STEP 2: Delete Processor Versions (except default and pretrained)
                # ========================================================
                logger.info("=== STEP 2: Discovering and Deleting Processor Versions ===")
                
                for processor in all_processors:
                    processor_name = processor.get("name")
                    processor_display_name = processor.get("displayName", "")
                    processor_type = processor.get("type", "")
                    
                    if not processor_name:
                        continue

                    # Extract processor ID from name
                    # Format: projects/{project}/locations/{location}/processors/{processor_id}
                    processor_id = processor_name.split("/")[-1]
                    
                    logger.info("Processing processor: %s (Type: %s)", processor_display_name, processor_type)

                    # List all versions for this processor
                    try:
                        all_versions = []
                        version_page_token = None

                        while True:
                            version_params = {}
                            if version_page_token:
                                version_params["pageToken"] = version_page_token

                            version_response = documentai_client.request(
                                "GET",
                                "{}/processorVersions".format(processor_name),
                                params=version_params,
                            )

                            version_items = version_response.get("processorVersions", [])
                            all_versions.extend(version_items)

                            version_page_token = version_response.get("nextPageToken")
                            if not version_page_token:
                                break

                        logger.info("Processor versions discovered for %s | Count=%d", processor_display_name, len(all_versions))

                        # Get default version
                        default_version = processor.get("defaultProcessorVersion", "")

                        # Delete non-default, non-pretrained versions
                        for version in all_versions:
                            version_name = version.get("name")
                            version_state = version.get("state", "")
                            
                            if not version_name:
                                continue

                            deleted_resources["total_processor_versions"] += 1

                            # Skip default version
                            if version_name == default_version:
                                logger.info("Skipping default version: %s", version_name)
                                continue

                            # Skip pretrained versions (they cannot be deleted)
                            # Pretrained versions typically have state "DEPLOYED" and are Google-managed
                            # Custom versions can be identified by checking if they were created by user
                            google_managed = version.get("googleManaged", False)
                            if google_managed:
                                logger.info("Skipping Google-managed (pretrained) version: %s", version_name)
                                continue

                            # Undeploy version if it's deployed
                            if version_state == "DEPLOYED":
                                try:
                                    logger.info("Undeploying processor version: %s", version_name)
                                    undeploy_operation = documentai_client.request(
                                        "POST",
                                        "{}:undeploy".format(version_name),
                                        json_body={},
                                    )
                                    logger.info("Undeploy operation initiated for version: %s", version_name)
                                    # Wait a bit for undeploy to complete
                                    time.sleep(5)
                                except GCPAPIError as e:
                                    logger.warning("Failed to undeploy version %s: %s", version_name, str(e))
                                    # Continue to try deletion anyway

                            # Delete processor version
                            try:
                                delete_operation = documentai_client.request(
                                    "DELETE",
                                    version_name,
                                )
                                deleted_resources["deleted_processor_versions"].append({
                                    "name": version_name,
                                    "processor": processor_display_name,
                                    "processor_id": processor_id
                                })
                                logger.info("Processor version deleted: %s", version_name)
                            except GCPAPIError as e:
                                deleted_resources["failed_deletions"].append({
                                    "resource_type": "Processor Version",
                                    "resource_name": version_name,
                                    "processor": processor_display_name,
                                    "region": region,
                                    "error": str(e)
                                })
                                logger.warning("Processor version deletion failed for %s: %s", version_name, str(e))

                    except GCPAPIError as e:
                        logger.error("List Processor Versions failed for %s: %s", processor_display_name, str(e))
                    except Exception as e:
                        logger.error("Processor Versions processing failed for %s: %s", processor_display_name, str(e), exc_info=True)

                # Wait for version deletions to propagate
                if len(deleted_resources["deleted_processor_versions"]) > 0:
                    logger.info("Waiting 10 seconds for processor version deletions to propagate")
                    time.sleep(10)

                # ========================================================
                # STEP 3: Delete Processors
                # ========================================================
                logger.info("=== STEP 3: Deleting Processors ===")

                for processor in all_processors:
                    processor_name = processor.get("name")
                    processor_display_name = processor.get("displayName", "")
                    processor_type = processor.get("type", "")
                    
                    if not processor_name:
                        continue

                    # Extract processor ID
                    processor_id = processor_name.split("/")[-1]

                    # Delete processor
                    try:
                        delete_operation = documentai_client.request(
                            "DELETE",
                            processor_name,
                        )
                        deleted_resources["deleted_processors"].append({
                            "name": processor_name,
                            "display_name": processor_display_name,
                            "type": processor_type,
                            "processor_id": processor_id
                        })
                        logger.info("Processor deleted: %s (Type: %s)", processor_display_name, processor_type)
                    except GCPAPIError as e:
                        deleted_resources["failed_deletions"].append({
                            "resource_type": "Processor",
                            "resource_name": processor_name,
                            "display_name": processor_display_name,
                            "region": region,
                            "error": str(e)
                        })
                        logger.warning("Processor deletion failed for %s: %s", processor_display_name, str(e))

            except GCPAPIError as e:
                logger.error("List Processors failed: %s", str(e))
                # If we can't list processors, fail immediately
                results.append({
                    "status": "FAILED",
                    "action": "DOCUMENT_AI_TERMINATION",
                    "reason": "ListProcessorsFailed",
                    "project_id": project_id,
                    "region": region,
                    "detail": str(e.message),
                    "http_status": e.status_code,
                })
                # Skip to next region
                continue
            except Exception as e:
                logger.error("Processors processing failed: %s", str(e), exc_info=True)
                # If we can't process processors, fail immediately
                results.append({
                    "status": "FAILED",
                    "action": "DOCUMENT_AI_TERMINATION",
                    "reason": "ProcessorsProcessingFailed",
                    "project_id": project_id,
                    "region": region,
                    "detail": str(e),
                })
                # Skip to next region
                continue

            # ------------------------------------------------------------
            # Workflow completed successfully
            # ------------------------------------------------------------
            logger.info("GCP Document AI cleanup COMPLETED | Project=%s | Region=%s | Location=%s | "
                       "ProcessorVersions=%d | Processors=%d",
                       project_id, region, location,
                       len(deleted_resources["deleted_processor_versions"]),
                       len(deleted_resources["deleted_processors"]))

            # Check if there were any failed deletions
            if len(deleted_resources["failed_deletions"]) > 0:
                logger.error("Deletion completed with failures | FailedResources=%d", len(deleted_resources["failed_deletions"]))
                results.append({
                    "status": "FAILED",
                    "action": "DOCUMENT_AI_TERMINATION",
                    "reason": "PartialDeletionFailure",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })
            else:
                results.append({
                    "status": "SUCCESS",
                    "action": "DOCUMENT_AI_TERMINATION",
                    "project_id": project_id,
                    "region": region,
                    "data": deleted_resources
                })

        except GCPAPIError as e:
            logger.error("GCP API Error during Document AI deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "DOCUMENT_AI_TERMINATION",
                "reason": "GCPAPIError",
                "project_id": project_id,
                "region": region,
                "detail": str(e.message),
                "http_status": e.status_code,
            })
        except Exception as e:
            logger.error("Unhandled exception during Document AI deletion", exc_info=True)
            results.append({
                "status": "FAILED",
                "action": "DOCUMENT_AI_TERMINATION",
                "reason": "UnhandledException",
                "project_id": project_id,
                "region": region,
                "detail": str(e),
            })

    return results
