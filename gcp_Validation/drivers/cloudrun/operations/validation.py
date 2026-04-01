import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-cloudrun-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Cloud Run validation rule executor.

    Expected spec keys:
    - project_id (required)
    - region (required)
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    
    Supported validation rules:
    - cloud_run_deployment_type: "container" | "function"
    - cloud_run_service_deployed: bool
    - cloud_run_job_created: bool
    - cloud_run_public_access: bool
    - cloud_run_service_account: bool
    """
    logger.info("GCP Cloud Run Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    region = spec.get("region")
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDRUN_VALIDATION",
            "reason": "ProjectIdRequired",
        }

    if not region:
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDRUN_VALIDATION",
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

        cloudrun_client = GCPClientFactory.create(
            "run",
            creds=client_creds,
        )

        logger.info(
            "Connected to GCP Cloud Run | Project=%s | Region=%s",
            project_id, region
        )

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # Cloud Run Deployment Type
        # ================================================================
        if "cloud_run_deployment_type" in rules:
            try:
                # Fetch all Cloud Run services in the region
                all_services: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {"pageSize": 100}
                    if page_token:
                        params["pageToken"] = page_token

                    response = cloudrun_client.request(
                        "GET",
                        "projects/{}/locations/{}/services".format(project_id, region),
                        params=params,
                    )
                    all_services.extend(response.get("services", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s Cloud Run service(s)", len(all_services))

                expected_type = rules["cloud_run_deployment_type"]
                matched = False

                for service in all_services:
                    service_name = service.get("name", "").split("/")[-1]
                    
                    # Debug: Log service structure to understand the response
                    logger.info("Service '%s' keys: %s", service_name, list(service.keys()))
                    
                    # Check if service has buildConfig field (indicates function deployment)
                    # Functions (Gen 2) have buildConfig with functionTarget, sourceLocation, etc.
                    build_config = service.get("buildConfig", {})
                    
                    # Also check labels for function indicators
                    labels = service.get("labels", {})
                    client = service.get("client", "")
                    
                    # Log buildConfig details
                    if build_config:
                        logger.info("Service '%s' buildConfig keys: %s", service_name, list(build_config.keys()))
                        logger.info("Service '%s' buildConfig.functionTarget: %s", 
                                  service_name, build_config.get("functionTarget"))
                    
                    logger.info("Service '%s' - buildConfig exists: %s, labels: %s, client: %s", 
                              service_name, bool(build_config), labels, client)
                    
                    # Determine deployment type based on how it was deployed:
                    # Function: Has buildConfig (deployed from source/inline editor)
                    # Container: No buildConfig OR buildConfig without functionTarget (deployed from container image)
                    # 
                    # Note: The UI shows "Function" for source-based deployments (inline editor)
                    # and "Container" for container image deployments
                    is_function = False
                    
                    if build_config:
                        # Has buildConfig means it was built from source (Function in UI)
                        is_function = True
                        logger.info("Service '%s' identified as Function (has buildConfig - source deployment)", service_name)
                    else:
                        # No buildConfig means it was deployed from container image
                        logger.info("Service '%s' identified as Container (no buildConfig - image deployment)", service_name)
                    
                    if expected_type == "function":
                        # Function deployment
                        if is_function:
                            matched = True
                            logger.info("Service '%s' matches Function deployment type", service_name)
                            break
                    elif expected_type == "container":
                        # Container deployment (not a function)
                        if not is_function:
                            matched = True
                            logger.info("Service '%s' matches Container deployment type", service_name)
                            break

                all_results["cloud_run_deployment_type"] = matched
                if not matched:
                    all_failure_reasons["cloud_run_deployment_type"] = (
                        "No Cloud Run service found with deployment type '{}'".format(expected_type)
                    )
                logger.info("cloud_run_deployment_type check (%s): %s", expected_type, matched)
            except Exception as e:
                logger.warning("Failed to check cloud_run_deployment_type: %s", e)
                all_results["cloud_run_deployment_type"] = False
                all_failure_reasons["cloud_run_deployment_type"] = "Deployment type check error: {}".format(e)

        # ================================================================
        # Cloud Run Service Deployed
        # ================================================================
        if "cloud_run_service_deployed" in rules:
            try:
                all_services: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {"pageSize": 100}
                    if page_token:
                        params["pageToken"] = page_token

                    response = cloudrun_client.request(
                        "GET",
                        "projects/{}/locations/{}/services".format(project_id, region),
                        params=params,
                    )
                    all_services.extend(response.get("services", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s Cloud Run service(s)", len(all_services))

                # Check if services are successfully deployed (have READY condition)
                deployed_services = []
                for service in all_services:
                    service_name = service.get("metadata", {}).get("name", "") or service.get("name", "").split("/")[-1]
                    
                    # Check multiple possible locations for status
                    # Cloud Run API v2 structure
                    conditions = service.get("status", {}).get("conditions", [])
                    
                    # Also check for terminalCondition (v2 API)
                    terminal_condition = service.get("terminalCondition", {})
                    
                    # Check if service is ready
                    is_ready = False
                    
                    # Method 1: Check conditions array
                    for condition in conditions:
                        if condition.get("type") == "Ready" and condition.get("status") == "True":
                            is_ready = True
                            break
                    
                    # Method 2: Check terminalCondition
                    if not is_ready and terminal_condition:
                        if terminal_condition.get("type") == "Ready" and terminal_condition.get("state") == "CONDITION_SUCCEEDED":
                            is_ready = True
                    
                    # Method 3: Check if service has a URL (indicates successful deployment)
                    if not is_ready:
                        uri = service.get("status", {}).get("url") or service.get("uri", "")
                        if uri:
                            is_ready = True
                    
                    if is_ready:
                        deployed_services.append(service_name)
                        logger.info("Service '%s' is deployed and ready", service_name)
                    else:
                        logger.info("Service '%s' exists but not ready. Status: %s", 
                                  service_name, service.get("status", {}))

                logger.info("Found %s deployed Cloud Run service(s): %s", 
                          len(deployed_services), deployed_services)

                passed = (len(deployed_services) > 0) == rules["cloud_run_service_deployed"]
                all_results["cloud_run_service_deployed"] = passed
                if not passed:
                    all_failure_reasons["cloud_run_service_deployed"] = (
                        "No Cloud Run service deployed successfully"
                        if rules["cloud_run_service_deployed"]
                        else "Cloud Run service is deployed but should not be"
                    )
                logger.info("cloud_run_service_deployed check: %s", passed)
            except Exception as e:
                logger.warning("Failed to check cloud_run_service_deployed: %s", e)
                all_results["cloud_run_service_deployed"] = False
                all_failure_reasons["cloud_run_service_deployed"] = "Service deployment check error: {}".format(e)

        # ================================================================
        # Cloud Run Jobs Created
        # ================================================================
        if "cloud_run_job_created" in rules:
            try:
                all_jobs: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {"pageSize": 100}
                    if page_token:
                        params["pageToken"] = page_token

                    response = cloudrun_client.request(
                        "GET",
                        "projects/{}/locations/{}/jobs".format(project_id, region),
                        params=params,
                    )
                    all_jobs.extend(response.get("jobs", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Found %s Cloud Run job(s)", len(all_jobs))

                passed = (len(all_jobs) > 0) == rules["cloud_run_job_created"]
                all_results["cloud_run_job_created"] = passed
                if not passed:
                    all_failure_reasons["cloud_run_job_created"] = (
                        "No Cloud Run job found"
                        if rules["cloud_run_job_created"]
                        else "Cloud Run job exists but should not"
                    )
                logger.info("cloud_run_job_created check: %s", passed)
            except Exception as e:
                logger.warning("Failed to check cloud_run_job_created: %s", e)
                all_results["cloud_run_job_created"] = False
                all_failure_reasons["cloud_run_job_created"] = "Cloud Run job check error: {}".format(e)

        # ================================================================
        # Cloud Run Public Access
        # ================================================================
        if "cloud_run_public_access" in rules:
            try:
                all_services: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {"pageSize": 100}
                    if page_token:
                        params["pageToken"] = page_token

                    response = cloudrun_client.request(
                        "GET",
                        "projects/{}/locations/{}/services".format(project_id, region),
                        params=params,
                    )
                    all_services.extend(response.get("services", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Checking public access for %s Cloud Run service(s)", len(all_services))

                expected_public_access = rules["cloud_run_public_access"]
                services_with_public_access = []

                for service in all_services:
                    service_name = service.get("name", "").split("/")[-1]
                    
                    logger.info("Checking public access for service '%s'", service_name)
                    
                    # Method 1: Check if invokerIamDisabled is true (allows unauthenticated access)
                    invoker_iam_disabled = service.get("invokerIamDisabled", False)
                    
                    if invoker_iam_disabled:
                        services_with_public_access.append(service_name)
                        logger.info("Service '%s' has public access (invokerIamDisabled=true)", service_name)
                        continue
                    
                    # Method 2: Check IAM policy for allUsers
                    logger.info("Checking IAM policy for service '%s'", service_name)
                    
                    try:
                        iam_response = cloudrun_client.request(
                            "GET",
                            "projects/{}/locations/{}/services/{}:getIamPolicy".format(
                                project_id, region, service_name
                            ),
                        )
                        
                        logger.info("IAM response for '%s': %s", service_name, json.dumps(iam_response))
                        
                        bindings = iam_response.get("bindings", [])
                        has_public_access = False
                        
                        for binding in bindings:
                            members = binding.get("members", [])
                            logger.info("Service '%s' IAM binding role '%s' has members: %s", 
                                      service_name, binding.get("role"), members)
                            # Check for allUsers (unauthenticated)
                            if "allUsers" in members:
                                has_public_access = True
                                logger.info("Service '%s' has public access (allUsers in IAM)", service_name)
                                break
                        
                        if has_public_access:
                            services_with_public_access.append(service_name)
                        else:
                            logger.info("Service '%s' does NOT have public access (no allUsers in IAM)", service_name)
                    except GCPAPIError as exc:
                        if exc.status_code == 404:
                            logger.info("Service '%s' has no custom IAM policy (no public access)", service_name)
                        else:
                            logger.warning("Failed to check IAM for service '%s': %s", service_name, exc)
                            logger.warning("Failed to check IAM for service '%s': %s", service_name, exc)

                logger.info("Services with public access: %s", services_with_public_access)

                if expected_public_access:
                    passed = len(services_with_public_access) > 0
                    all_results["cloud_run_public_access"] = passed
                    if not passed:
                        all_failure_reasons["cloud_run_public_access"] = (
                            "No Cloud Run service found with public access"
                        )
                else:
                    passed = len(services_with_public_access) == 0
                    all_results["cloud_run_public_access"] = passed
                    if not passed:
                        all_failure_reasons["cloud_run_public_access"] = (
                            "Cloud Run service(s) have public access but should not: {}".format(
                                ", ".join(services_with_public_access)
                            )
                        )
                
                logger.info("cloud_run_public_access check (expected=%s): %s", expected_public_access, passed)
            except Exception as e:
                logger.warning("Failed to check cloud_run_public_access: %s", e)
                all_results["cloud_run_public_access"] = False
                all_failure_reasons["cloud_run_public_access"] = "Public access check error: {}".format(e)

        # ================================================================
        # Cloud Run Service Account Attached
        # ================================================================
        if "cloud_run_service_account" in rules:
            try:
                all_services: List[Dict[str, Any]] = []
                page_token = None

                while True:
                    params = {"pageSize": 100}
                    if page_token:
                        params["pageToken"] = page_token

                    response = cloudrun_client.request(
                        "GET",
                        "projects/{}/locations/{}/services".format(project_id, region),
                        params=params,
                    )
                    all_services.extend(response.get("services", []))

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break

                logger.info("Checking service accounts for %s Cloud Run service(s)", len(all_services))

                expected_sa_attached = rules["cloud_run_service_account"]
                services_with_custom_sa = []

                for service in all_services:
                    service_name = service.get("name", "").split("/")[-1]
                    
                    # Get service account from template.spec.serviceAccount (v2 API)
                    template = service.get("template", {})
                    service_account = template.get("serviceAccount", "")
                    
                    logger.info("Service '%s' serviceAccount field: '%s'", service_name, service_account)
                    
                    # Check if it's a custom service account (not default Compute Engine SA)
                    # Any service account set means it has a service account attached
                    if service_account:
                        # Check if it's the default compute service account
                        # Default format: PROJECT_NUMBER-compute@developer.gserviceaccount.com
                        if "-compute@developer.gserviceaccount.com" not in service_account:
                            services_with_custom_sa.append(service_name)
                            logger.info("Service '%s' has custom service account: %s", 
                                      service_name, service_account)
                        else:
                            # Even default compute SA counts as "has service account"
                            services_with_custom_sa.append(service_name)
                            logger.info("Service '%s' has default Compute Engine service account: %s", 
                                      service_name, service_account)
                    else:
                        logger.info("Service '%s' has no explicit service account (uses default)", 
                                  service_name)

                logger.info("Services with custom service account: %s", services_with_custom_sa)

                if expected_sa_attached:
                    passed = len(services_with_custom_sa) > 0
                    all_results["cloud_run_service_account"] = passed
                    if not passed:
                        all_failure_reasons["cloud_run_service_account"] = (
                            "No Cloud Run service found with custom service account attached"
                        )
                else:
                    passed = len(services_with_custom_sa) == 0
                    all_results["cloud_run_service_account"] = passed
                    if not passed:
                        all_failure_reasons["cloud_run_service_account"] = (
                            "Cloud Run service(s) have custom service accounts but should not: {}".format(
                                ", ".join(services_with_custom_sa)
                            )
                        )
                
                logger.info("cloud_run_service_account check (expected=%s): %s", expected_sa_attached, passed)
            except Exception as e:
                logger.warning("Failed to check cloud_run_service_account: %s", e)
                all_results["cloud_run_service_account"] = False
                all_failure_reasons["cloud_run_service_account"] = "Service account check error: {}".format(e)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_CLOUDRUN_VALIDATION",
                "project_id": project_id,
                "region": region,
                "data": {
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDRUN_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "region": region,
            "data": {
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Cloud Run validation")
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDRUN_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "region": region,
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Cloud Run validation")
        return {
            "status": "FAILED",
            "action": "GCP_CLOUDRUN_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "region": region,
        }
