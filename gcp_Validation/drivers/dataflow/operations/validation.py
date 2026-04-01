import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-dataflow-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Dataflow validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    
    Supported validation rules:
    - dataflow_job_created: bool
    - dataflow_workbench_instance_created: bool
    - allowed_machine_type: str (e.g., "n1-standard-2")
    - dataflow_pipeline_created: bool
    """
    logger.info("GCP Dataflow Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_DATAFLOW_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_DATAFLOW_VALIDATION",
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

        dataflow_client = GCPClientFactory.create(
            "dataflow",
            creds=client_creds,
        )

        datapipelines_client = GCPClientFactory.create(
            "datapipelines",
            creds=client_creds,
        )

        notebooks_client = GCPClientFactory.create(
            "notebooks",
            creds=client_creds,
        )

        logger.info(
            "Connected to GCP Dataflow | Project=%s | Region=%s",
            project_id, region
        )

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Dataflow Job Created
        # ================================================================
        if "dataflow_job_created" in rules:
            try:
                all_jobs: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {"pageSize": 100}
                    if page_token:
                        params["pageToken"] = page_token

                    response = dataflow_client.request(
                        "GET",
                        "projects/{}/locations/{}/jobs".format(project_id, region),
                        params=params,
                    )
                    all_jobs.extend(response.get("jobs", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s Dataflow job(s)", len(all_jobs))

                passed = (len(all_jobs) > 0) == rules["dataflow_job_created"]
                all_results["dataflow_job_created"] = passed
                if not passed:
                    all_failure_reasons["dataflow_job_created"] = (
                        "No Dataflow job found"
                        if rules["dataflow_job_created"]
                        else "Dataflow job exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check dataflow_job_created: %s", e)
                all_results["dataflow_job_created"] = False
                all_failure_reasons["dataflow_job_created"] = "Dataflow job check error: {}".format(e)

        # ================================================================
        # Dataflow Pipeline Created
        # ================================================================
        if "dataflow_pipeline_created" in rules:
            try:
                all_pipelines: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {"pageSize": 100}
                    if page_token:
                        params["pageToken"] = page_token

                    response = datapipelines_client.request(
                        "GET",
                        "projects/{}/locations/{}/pipelines".format(project_id, region),
                        params=params,
                    )
                    all_pipelines.extend(response.get("pipelines", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s Dataflow pipeline(s)", len(all_pipelines))

                passed = (len(all_pipelines) > 0) == rules["dataflow_pipeline_created"]
                all_results["dataflow_pipeline_created"] = passed
                if not passed:
                    all_failure_reasons["dataflow_pipeline_created"] = (
                        "No Dataflow pipeline found"
                        if rules["dataflow_pipeline_created"]
                        else "Dataflow pipeline exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check dataflow_pipeline_created: %s", e)
                all_results["dataflow_pipeline_created"] = False
                all_failure_reasons["dataflow_pipeline_created"] = "Dataflow pipeline check error: {}".format(e)

        # ================================================================
        # Dataflow Workbench Instance Created
        # ================================================================
        if "dataflow_workbench_instance_created" in rules:
            try:
                # Collect Vertex AI Workbench Instances (same API for Dataflow Workbench)
                # Need to check all zones within the region since instances are zone-specific
                all_instances: List[Dict[str, Any]] = []
                
                # First, get all zones in the region using compute API
                compute_client = GCPClientFactory.create(
                    "compute",
                    creds=client_creds,
                )
                
                zones_response = compute_client.request(
                    "GET",
                    "projects/{}/zones".format(project_id),
                    params={"filter": "name:{}*".format(region)}
                )
                
                zones = [zone["name"] for zone in zones_response.get("items", [])]
                logger.info("Found %s zone(s) in region %s: %s", len(zones), region, zones)
                
                # Query each zone for instances
                for zone in zones:
                    page_token = None
                    while True:
                        params = {"pageSize": 500}
                        if page_token:
                            params["pageToken"] = page_token

                        try:
                            response = notebooks_client.request(
                                "GET",
                                "projects/{}/locations/{}/instances".format(project_id, zone),
                                params=params,
                            )
                            all_instances.extend(response.get("instances", []))
                            
                            page_token = response.get("nextPageToken")
                            if not page_token:
                                break
                        except GCPAPIError as e:
                            # If zone doesn't have instances or API returns 404, continue
                            if e.status_code in [404, 403]:
                                logger.info("No instances found in zone %s or access denied", zone)
                                break
                            raise

                logger.info("Found %s Dataflow Workbench instance(s) across all zones", len(all_instances))

                passed = (len(all_instances) > 0) == rules["dataflow_workbench_instance_created"]
                all_results["dataflow_workbench_instance_created"] = passed
                if not passed:
                    all_failure_reasons["dataflow_workbench_instance_created"] = (
                        "No Dataflow Workbench instance found"
                        if rules["dataflow_workbench_instance_created"]
                        else "Dataflow Workbench instance exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check dataflow_workbench_instance_created: %s", e)
                all_results["dataflow_workbench_instance_created"] = False
                all_failure_reasons["dataflow_workbench_instance_created"] = "Dataflow Workbench check error: {}".format(e)

        # ================================================================
        # Allowed Machine Type (for Workbench Instances)
        # ================================================================
        if "allowed_machine_type" in rules:
            try:
                # Collect Workbench Instances across all zones in the region
                all_instances: List[Dict[str, Any]] = []
                
                # Get all zones in the region using compute API
                compute_client = GCPClientFactory.create(
                    "compute",
                    creds=client_creds,
                )
                
                zones_response = compute_client.request(
                    "GET",
                    "projects/{}/zones".format(project_id),
                    params={"filter": "name:{}*".format(region)}
                )
                
                zones = [zone["name"] for zone in zones_response.get("items", [])]
                logger.info("Checking machine types in %s zone(s) in region %s", len(zones), region)
                
                # Query each zone for instances
                for zone in zones:
                    page_token = None
                    while True:
                        params = {"pageSize": 500}
                        if page_token:
                            params["pageToken"] = page_token

                        try:
                            response = notebooks_client.request(
                                "GET",
                                "projects/{}/locations/{}/instances".format(project_id, zone),
                                params=params,
                            )
                            all_instances.extend(response.get("instances", []))
                            
                            page_token = response.get("nextPageToken")
                            if not page_token:
                                break
                        except GCPAPIError as e:
                            if e.status_code in [404, 403]:
                                logger.info("No instances found in zone %s or access denied", zone)
                                break
                            raise

                expected_machine_type = rules["allowed_machine_type"]
                invalid_instances = []
                checked_instances = []

                for instance in all_instances:
                    # Get machine type from instance
                    # The machineType field contains the full path, extract just the type
                    machine_type_full = instance.get("gceSetup", {}).get("machineType", "")
                    # Extract machine type from path like "zones/us-central1-a/machineTypes/n1-standard-2"
                    machine_type = machine_type_full.split("/")[-1] if machine_type_full else ""
                    
                    instance_info = {
                        "instance_name": instance.get("name"),
                        "location": instance.get("name", "").split("/")[-3] if "/" in instance.get("name", "") else zone,
                        "machine_type": machine_type,
                        "expected": expected_machine_type
                    }
                    checked_instances.append(instance_info)
                    
                    if machine_type and machine_type != expected_machine_type:
                        invalid_instances.append(instance_info)

                logger.info("Checked %s Workbench instance(s) for machine type %s", len(all_instances), expected_machine_type)
                logger.info("Instance machine types found: %s", json.dumps(checked_instances))
                logger.info("Invalid instances (mismatched machine types): %s", json.dumps(invalid_instances))

                passed = len(invalid_instances) == 0
                all_results["allowed_machine_type"] = passed
                if not passed:
                    # Create a simple summary of invalid instances
                    invalid_summary = ", ".join([
                        "{} ({})".format(inst["instance_name"].split("/")[-1], inst["machine_type"])
                        for inst in invalid_instances
                    ])
                    all_failure_reasons["allowed_machine_type"] = (
                        "Found {} Workbench instance(s) with invalid machine type. Expected: {}, Found: {}".format(
                            len(invalid_instances),
                            expected_machine_type,
                            invalid_summary
                        )
                    )
            except Exception as e:
                logger.warning("Failed to check allowed_machine_type: %s", e)
                all_results["allowed_machine_type"] = False
                all_failure_reasons["allowed_machine_type"] = "Machine type check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_DATAFLOW_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_DATAFLOW_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Dataflow validation")
        return {
            "status": "FAILED",
            "action": "GCP_DATAFLOW_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Dataflow validation")
        return {
            "status": "FAILED",
            "action": "GCP_DATAFLOW_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
