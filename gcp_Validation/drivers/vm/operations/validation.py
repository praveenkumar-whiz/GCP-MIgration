import json
import logging
import urllib.request
import urllib.error
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory


# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------

logger = logging.getLogger("gcp-compute-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """GCP Compute Engine validation rule executor.

    Expected spec keys:
    - project_id (required)
    - zone (optional) — if provided, queries that zone only; if omitted, queries all zones via aggregatedList
    - gcp_access_token / access_token (required)
    - validation_config: { ... rule json ... }
    """
    logger.info("GCP Compute Engine Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(spec))

    project_id = spec.get("project_id")
    zone = spec.get("zone") or None          # treat empty string as None
    rules = spec.get("validation_config", {}) or {}

    if not project_id:
        return {
            "status": "FAILED",
            "action": "GCP_COMPUTE_VALIDATION",
            "reason": "ProjectIdRequired",
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
            "Connected to GCP Compute Engine | Project=%s | Zone=%s",
            project_id, zone or "all-zones"
        )

        # --------------------------------------------------
        # Collect Active VM Instances
        # Zone provided  → zones/{zone}/instances
        # Zone omitted   → aggregatedList (all zones)
        # --------------------------------------------------
        active_instances: List[Dict[str, Any]] = []
        page_token = None

        if zone:
            # Single-zone fetch
            while True:
                params = {"maxResults": 500}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/zones/{}/instances".format(project_id, zone),
                    params=params,
                )
                for instance in response.get("items", []):
                    status = instance.get("status", "").upper()
                    if status not in ["TERMINATED", "STOPPING", "SUSPENDING", "SUSPENDED"]:
                        active_instances.append(instance)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break
        else:
            # Aggregated fetch across all zones
            while True:
                params = {"maxResults": 500}
                if page_token:
                    params["pageToken"] = page_token

                response = compute_client.request(
                    "GET",
                    "projects/{}/aggregated/instances".format(project_id),
                    params=params,
                )
                for zone_data in response.get("items", {}).values():
                    for instance in zone_data.get("instances", []):
                        status = instance.get("status", "").upper()
                        if status not in ["TERMINATED", "STOPPING", "SUSPENDING", "SUSPENDED"]:
                            active_instances.append(instance)

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

        logger.info("Found %s active GCP Compute instance(s)", len(active_instances))

        # --------------------------------------------------
        # All validation results and failure reasons collected here
        # --------------------------------------------------
        all_results: Dict[str, bool] = {}
        all_failure_reasons: Dict[str, str] = {}

        # ================================================================
        # VM Instance Existence
        # ================================================================
        if "vm_instance" in rules:
            try:
                passed = (len(active_instances) > 0) == rules["vm_instance"]
                all_results["vm_instance"] = passed
                if not passed:
                    all_failure_reasons["vm_instance"] = (
                        "No VM instance found"
                        if rules["vm_instance"]
                        else "VM instance exists but should not"
                    )
            except Exception as e:
                logger.warning("Failed to check vm_instance: %s", e)
                all_results["vm_instance"] = False
                all_failure_reasons["vm_instance"] = "VM instance check error: {}".format(e)

        # ================================================================
        # VM State
        # Pass the exact GCP status string: RUNNING, TERMINATED, STAGING, etc.
        # ================================================================
        if "vm_state" in rules:
            try:
                expected_state = str(rules["vm_state"]).upper()

                # Fetch ALL instances (including stopped) for state check
                all_vms: List[Dict[str, Any]] = []
                page_token_state = None

                if zone:
                    while True:
                        params = {"maxResults": 500}
                        if page_token_state:
                            params["pageToken"] = page_token_state
                        resp = compute_client.request(
                            "GET",
                            "projects/{}/zones/{}/instances".format(project_id, zone),
                            params=params,
                        )
                        all_vms.extend(resp.get("items", []))
                        page_token_state = resp.get("nextPageToken")
                        if not page_token_state:
                            break
                else:
                    while True:
                        params = {"maxResults": 500}
                        if page_token_state:
                            params["pageToken"] = page_token_state
                        resp = compute_client.request(
                            "GET",
                            "projects/{}/aggregated/instances".format(project_id),
                            params=params,
                        )
                        for zone_data in resp.get("items", {}).values():
                            all_vms.extend(zone_data.get("instances", []))
                        page_token_state = resp.get("nextPageToken")
                        if not page_token_state:
                            break

                matched = any(
                    vm.get("status", "").upper() == expected_state
                    for vm in all_vms
                )
                all_results["vm_state"] = matched
                if not matched:
                    all_failure_reasons["vm_state"] = (
                        "No VM found in '{}' state".format(rules["vm_state"])
                    )
                logger.info("vm_state check (%s): %s", expected_state, matched)
            except Exception as e:
                logger.warning("Failed to check vm_state: %s", e)
                all_results["vm_state"] = False
                all_failure_reasons["vm_state"] = "VM state check error: {}".format(e)

        # ================================================================
        # Has Public IP
        # ================================================================
        if "has_public_ip" in rules:
            try:
                expected_has_ip = rules["has_public_ip"]

                def _has_external_ip(inst: Dict[str, Any]) -> bool:
                    for iface in inst.get("networkInterfaces", []):
                        for access_config in iface.get("accessConfigs", []):
                            if access_config.get("natIP"):
                                return True
                    return False

                if expected_has_ip:
                    matched = any(_has_external_ip(inst) for inst in active_instances)
                    all_results["has_public_ip"] = matched
                    if not matched:
                        all_failure_reasons["has_public_ip"] = (
                            "No VM instance found with a public IP"
                        )
                else:
                    matched = all(not _has_external_ip(inst) for inst in active_instances)
                    all_results["has_public_ip"] = matched
                    if not matched:
                        all_failure_reasons["has_public_ip"] = (
                            "VM instance found with a public IP but should not have one"
                        )
                logger.info("has_public_ip check (%s): %s", expected_has_ip, matched)
            except Exception as e:
                logger.warning("Failed to check has_public_ip: %s", e)
                all_results["has_public_ip"] = False
                all_failure_reasons["has_public_ip"] = "Public IP check error: {}".format(e)

        # ================================================================
        # Allowed VM Types
        # Pass exact GCP machine type string, e.g. "n1-standard-1", "n2-standard-2"
        # ================================================================
        if "allowed_vm_types" in rules:
            try:
                allowed_types = rules["allowed_vm_types"]
                if isinstance(allowed_types, str):
                    allowed_types = [allowed_types]
                allowed_types_set = set(allowed_types)

                matched = False
                for inst in active_instances:
                    machine_type_url = inst.get("machineType", "")
                    machine_type = machine_type_url.split("/")[-1] if machine_type_url else ""
                    if machine_type in allowed_types_set:
                        matched = True
                        break

                all_results["allowed_vm_types"] = matched
                if not matched:
                    all_failure_reasons["allowed_vm_types"] = (
                        "No VM found with allowed type(s): {}".format(allowed_types)
                    )
                logger.info("allowed_vm_types check (%s): %s", allowed_types, matched)
            except Exception as e:
                logger.warning("Failed to check allowed_vm_types: %s", e)
                all_results["allowed_vm_types"] = False
                all_failure_reasons["allowed_vm_types"] = "VM type check error: {}".format(e)

        # ================================================================
        # Allowed Image Keywords (e.g. "ubuntu", "windows", "debian")
        # Checks licenses URL and source disk URL for the keyword substring
        # ================================================================
        if "allowed_image" in rules:
            try:
                keywords = rules["allowed_image"]
                if isinstance(keywords, str):
                    keywords = [keywords]

                matched = False
                for inst in active_instances:
                    for disk in inst.get("disks", []):
                        licenses = disk.get("licenses", [])
                        source = disk.get("source", "").lower()
                        license_text = " ".join(licenses).lower()
                        combined = license_text + " " + source

                        if any(kw.lower() in combined for kw in keywords):
                            matched = True
                            break
                    if matched:
                        break

                all_results["allowed_image"] = matched
                if not matched:
                    all_failure_reasons["allowed_image"] = (
                        "No VM found with image matching keyword(s): {}".format(keywords)
                    )
                logger.info("allowed_image check (%s): %s", keywords, matched)
            except Exception as e:
                logger.warning("Failed to check allowed_image: %s", e)
                all_results["allowed_image"] = False
                all_failure_reasons["allowed_image"] = "Image keyword check error: {}".format(e)

        # ================================================================
        # Service Account Attached
        # ================================================================
        if "service_account_attached" in rules:
            try:
                expected_sa = rules["service_account_attached"]
                if expected_sa:
                    matched = any(
                        len(inst.get("serviceAccounts", [])) > 0
                        for inst in active_instances
                    )
                    all_results["service_account_attached"] = matched
                    if not matched:
                        all_failure_reasons["service_account_attached"] = (
                            "No VM found with a service account attached"
                        )
                else:
                    matched = all(
                        len(inst.get("serviceAccounts", [])) == 0
                        for inst in active_instances
                    )
                    all_results["service_account_attached"] = matched
                    if not matched:
                        all_failure_reasons["service_account_attached"] = (
                            "VM found with a service account but should not have one"
                        )
                logger.info("service_account_attached check (%s): %s", expected_sa, matched)
            except Exception as e:
                logger.warning("Failed to check service_account_attached: %s", e)
                all_results["service_account_attached"] = False
                all_failure_reasons["service_account_attached"] = "Service account check error: {}".format(e)

        # ================================================================
        # Disk Existence
        # ================================================================
        if "disk" in rules:
            try:
                if zone:
                    disk_response = compute_client.request(
                        "GET",
                        "projects/{}/zones/{}/disks".format(project_id, zone),
                        params={"maxResults": 500},
                    )
                    disks = disk_response.get("items", [])
                else:
                    disk_response = compute_client.request(
                        "GET",
                        "projects/{}/aggregated/disks".format(project_id),
                        params={"maxResults": 500},
                    )
                    disks = []
                    for zone_data in disk_response.get("items", {}).values():
                        disks.extend(zone_data.get("disks", []))

                passed = (len(disks) > 0) == rules["disk"]
                all_results["disk"] = passed
                if not passed:
                    all_failure_reasons["disk"] = (
                        "No disk found"
                        if rules["disk"]
                        else "Disk exists but should not"
                    )
                logger.info("disk check: %s", passed)
            except Exception as e:
                logger.warning("Failed to check disk: %s", e)
                all_results["disk"] = False
                all_failure_reasons["disk"] = "Disk check error: {}".format(e)

        # ================================================================
        # Disk Type
        # Accepts string or list, e.g. "pd-balanced" or ["pd-balanced"]
        # Exact match against GCP disk type, e.g. "pd-standard", "pd-ssd", "pd-balanced"
        # ================================================================
        if "disk_type" in rules:
            try:
                raw_disk_type = rules["disk_type"]
                if isinstance(raw_disk_type, list):
                    expected_disk_types = set(v.lower() for v in raw_disk_type)
                else:
                    expected_disk_types = {str(raw_disk_type).lower()}

                if zone:
                    disk_response = compute_client.request(
                        "GET",
                        "projects/{}/zones/{}/disks".format(project_id, zone),
                        params={"maxResults": 500},
                    )
                    disks = disk_response.get("items", [])
                else:
                    disk_response = compute_client.request(
                        "GET",
                        "projects/{}/aggregated/disks".format(project_id),
                        params={"maxResults": 500},
                    )
                    disks = []
                    for zone_data in disk_response.get("items", {}).values():
                        disks.extend(zone_data.get("disks", []))

                matched = False
                for disk in disks:
                    disk_type_url = disk.get("type", "")
                    disk_type = disk_type_url.split("/")[-1].lower() if disk_type_url else ""
                    if disk_type in expected_disk_types:
                        matched = True
                        break

                all_results["disk_type"] = matched
                if not matched:
                    all_failure_reasons["disk_type"] = (
                        "No disk found with type '{}'".format(raw_disk_type)
                    )
                logger.info("disk_type check (%s): %s", expected_disk_types, matched)
            except Exception as e:
                logger.warning("Failed to check disk_type: %s", e)
                all_results["disk_type"] = False
                all_failure_reasons["disk_type"] = "Disk type check error: {}".format(e)

        # ================================================================
        # Instance Group Created
        # ================================================================
        if "instance_group_created" in rules:
            try:
                if zone:
                    ig_response = compute_client.request(
                        "GET",
                        "projects/{}/zones/{}/instanceGroups".format(project_id, zone),
                        params={"maxResults": 500},
                    )
                    instance_groups = ig_response.get("items", [])
                else:
                    ig_response = compute_client.request(
                        "GET",
                        "projects/{}/aggregated/instanceGroups".format(project_id),
                        params={"maxResults": 500},
                    )
                    instance_groups = []
                    for zone_data in ig_response.get("items", {}).values():
                        instance_groups.extend(zone_data.get("instanceGroups", []))

                passed = (len(instance_groups) > 0) == rules["instance_group_created"]
                all_results["instance_group_created"] = passed
                if not passed:
                    all_failure_reasons["instance_group_created"] = (
                        "Instance Group not found"
                        if rules["instance_group_created"]
                        else "Instance Group exists but should not"
                    )
                logger.info("instance_group_created check: %s", passed)
            except Exception as e:
                logger.warning("Failed to check instance_group_created: %s", e)
                all_results["instance_group_created"] = False
                all_failure_reasons["instance_group_created"] = "Instance Group check error: {}".format(e)

        # ================================================================
        # Instance Template Created
        # Uses aggregated/instanceTemplates — same pattern as aggregated/instances
        # and aggregated/instanceGroups which work correctly with GCPClientFactory.
        # global/instanceTemplates returns empty items; aggregated covers all regions.
        # ================================================================
        if "instance_template_created" in rules:
            try:
                instance_templates = []
                page_token_it = None

                while True:
                    params = {"maxResults": 500}
                    if page_token_it:
                        params["pageToken"] = page_token_it

                    it_response = compute_client.request(
                        "GET",
                        "projects/{}/aggregated/instanceTemplates".format(project_id),
                        params=params,
                    )
                    logger.info(
                        "instance_template raw response keys: %s",
                        list(it_response.keys()),
                    )

                    for scope_data in it_response.get("items", {}).values():
                        instance_templates.extend(scope_data.get("instanceTemplates", []))

                    page_token_it = it_response.get("nextPageToken")
                    if not page_token_it:
                        break

                logger.info("Total instance templates found: %s", len(instance_templates))
                passed = (len(instance_templates) > 0) == rules["instance_template_created"]
                all_results["instance_template_created"] = passed
                if not passed:
                    all_failure_reasons["instance_template_created"] = (
                        "Instance Template not found"
                        if rules["instance_template_created"]
                        else "Instance Template exists but should not"
                    )
                logger.info("instance_template_created check: %s", passed)
            except Exception as e:
                logger.warning("Failed to check instance_template_created: %s", e)
                all_results["instance_template_created"] = False
                all_failure_reasons["instance_template_created"] = "Instance Template check error: {}".format(e)

        # ================================================================
        # WEB CHECKS (per-instance HTTP probe, scan across all instances)
        # ================================================================

        def _get_public_ip(inst: Dict[str, Any]):
            for iface in inst.get("networkInterfaces", []):
                for ac in iface.get("accessConfigs", []):
                    if ac.get("natIP"):
                        return ac["natIP"]
            return None

        # Apache Web Server Installed
        if "install_apache_webserver" in rules:
            apache_found = False
            for inst in active_instances:
                public_ip = _get_public_ip(inst)
                if not public_ip or inst.get("status", "").upper() != "RUNNING":
                    continue
                for path in ["/", "/index.html"]:
                    url = "http://{}{}".format(public_ip, path)
                    try:
                        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                        conn = urllib.request.urlopen(req, timeout=5)
                        code = conn.getcode()
                        logger.info("Apache check %s → HTTP %s", url, code)
                        if code in [200, 301, 302]:
                            apache_found = True
                            break
                    except urllib.error.HTTPError as e:
                        if e.code == 403 and "Apache" in str(e.headers.get("Server", "")):
                            apache_found = True
                            break
                        logger.info("Apache check %s → HTTP error %s", url, e.code)
                    except Exception as e:
                        logger.info("Apache check %s → %s", url, e)
                if apache_found:
                    break

            passed = apache_found == rules["install_apache_webserver"]
            all_results["install_apache_webserver"] = passed
            if not passed:
                all_failure_reasons["install_apache_webserver"] = (
                    "Apache web server not found running on any instance"
                    if rules["install_apache_webserver"]
                    else "Apache web server is running but should not be"
                )
            logger.info("install_apache_webserver check: %s", passed)

        # VM Web Application Status
        if "check_vm_web_application_status" in rules:
            web_app_found = False
            for inst in active_instances:
                public_ip = _get_public_ip(inst)
                if not public_ip or inst.get("status", "").upper() != "RUNNING":
                    continue
                for path in ["/", "/index.html"]:
                    url = "http://{}{}".format(public_ip, path)
                    try:
                        conn = urllib.request.urlopen(url, timeout=5)
                        if conn.getcode() == 200:
                            web_app_found = True
                            logger.info("Web app check SUCCESS at %s", url)
                            break
                    except Exception as e:
                        logger.info("Web app check %s → %s", url, e)
                if web_app_found:
                    break

            passed = web_app_found == rules["check_vm_web_application_status"]
            all_results["check_vm_web_application_status"] = passed
            if not passed:
                all_failure_reasons["check_vm_web_application_status"] = (
                    "Web application not reachable on any VM instance"
                    if rules["check_vm_web_application_status"]
                    else "Web application is reachable but should not be"
                )
            logger.info("check_vm_web_application_status check: %s", passed)

        # ================================================================
        # FINAL DECISION
        # ================================================================
        logger.info("All validation results: %s", all_results)

        failed_checks = [k for k, v in all_results.items() if not v]

        if not failed_checks:
            return {
                "status": "SUCCESS",
                "action": "GCP_COMPUTE_VALIDATION",
                "project_id": project_id,
                "zone": zone or "all-zones",
                "data": {
                    "instances_checked": len(active_instances),
                    "validation_results": all_results,
                },
            }

        reasons = [all_failure_reasons[k] for k in failed_checks if k in all_failure_reasons]
        return {
            "status": "FAILED",
            "action": "GCP_COMPUTE_VALIDATION",
            "reason": "; ".join(reasons) if reasons else "ValidationFailed",
            "project_id": project_id,
            "zone": zone or "all-zones",
            "data": {
                "instances_checked": len(active_instances),
                "failed_checks": failed_checks,
                "validation_results": all_results,
            },
        }

    except GCPAPIError as exc:
        logger.exception("GCP API Error during Compute Engine validation")
        return {
            "status": "FAILED",
            "action": "GCP_COMPUTE_VALIDATION",
            "reason": "GCPAPIError",
            "project_id": project_id,
            "zone": zone or "all-zones",
            "detail": str(exc.message),
            "http_status": exc.status_code,
        }
    except Exception as exc:
        logger.exception("Unhandled exception during GCP Compute Engine validation")
        return {
            "status": "FAILED",
            "action": "GCP_COMPUTE_VALIDATION",
            "reason": str(exc),
            "project_id": project_id,
            "zone": zone or "all-zones",
        }