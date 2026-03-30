import json
import logging
from typing import Any, Dict

from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-vm-validate")
logger.setLevel(logging.INFO)


def validation_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Real GCP VM validation rule executor.

    Expected spec keys:
    - project_id
    - gcp_access_token / access_token
    - zone (optional)
    - validation_config: { ... rule json ... }
    """
    masked_spec = dict(spec)
    for key in ("gcp_access_token", "access_token", "google_access_token"):
        if masked_spec.get(key):
            masked_spec[key] = "***REDACTED***"

    logger.info("GCP VM Validation triggered")
    logger.info("Incoming spec: %s", json.dumps(masked_spec))

    # -------------------------------------------------------------------
    # Validate target scope and rules
    # -------------------------------------------------------------------
    rules = spec.get("validation_config", {}) or {}
    zone = spec.get("zone") or rules.get("zone")
    region = spec.get("region") or zone
    if not zone and not region:
        return {
            "status": "FAILED",
            "action": "VM_VALIDATION",
            "reason": "ZoneOrRegionRequired",
        }

    try:
        # -------------------------------------------------------------------
        # Build project context and authenticated compute client
        # -------------------------------------------------------------------
        project_id = str(spec.get("project_id") or spec.get("gcp_project_id") or "").strip()
        if not project_id:
            raise ValueError("project_id is required for GCP VM operations")

        compute_client = GCPClientFactory.create(
            "compute",
            creds={
                "access_token": (
                    spec.get("gcp_access_token")
                    or spec.get("access_token")
                    or spec.get("google_access_token")
                ),
                "timeout": int(spec.get("timeout") or 30),
            },
        )

        # -------------------------------------------------------------------
        # Discover VM inventory from GCP Compute Engine
        # -------------------------------------------------------------------
        instances: list[Dict[str, Any]] = []
        page_token = None
        if zone:
            zone_name = str(zone).strip().rstrip("/").split("/")[-1]
            while True:
                params: Dict[str, Any] = {}
                if page_token:
                    params["pageToken"] = page_token
                payload = compute_client.request(
                    "GET",
                    "projects/{}/zones/{}/instances".format(project_id, zone_name),
                    params=params,
                )
                for instance in payload.get("items", []):
                    if isinstance(instance, dict):
                        instances.append(instance)
                page_token = payload.get("nextPageToken")
                if not page_token:
                    break
        else:
            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token
                payload = compute_client.request(
                    "GET",
                    "projects/{}/aggregated/instances".format(project_id),
                    params=params,
                )
                for zone_payload in payload.get("items", {}).values():
                    if not isinstance(zone_payload, dict):
                        continue
                    for instance in zone_payload.get("instances", []):
                        if isinstance(instance, dict):
                            instances.append(instance)
                page_token = payload.get("nextPageToken")
                if not page_token:
                    break
    except ValueError as exc:
        return {
            "status": "FAILED",
            "action": "VM_VALIDATION",
            "reason": "InvalidSpec",
            "detail": str(exc),
        }
    except GCPAPIError as exc:
        logger.error("Failed to list GCP VM instances: %s", str(exc), exc_info=True)
        return {
            "status": "FAILED",
            "action": "VM_VALIDATION",
            "reason": "ListInstancesFailed",
            "detail": str(exc),
            "http_status": exc.status_code,
            "region": region,
        }

    # -------------------------------------------------------------------
    # Normalize raw Compute Engine inventory
    # -------------------------------------------------------------------
    normalized_instances: list[dict[str, Any]] = []
    for instance in instances:
        metadata = instance.get("metadata", {})
        metadata_items = metadata.get("items", []) if isinstance(metadata, dict) else []
        metadata_map = {
            str(item.get("key")): item.get("value")
            for item in metadata_items
            if isinstance(item, dict) and item.get("key")
        }

        public_ips = []
        network_interfaces = instance.get("networkInterfaces", [])
        for interface in network_interfaces if isinstance(network_interfaces, list) else []:
            access_configs = interface.get("accessConfigs", [])
            for config in access_configs if isinstance(access_configs, list) else []:
                nat_ip = config.get("natIP")
                if nat_ip:
                    public_ips.append(str(nat_ip))

        service_account_email = None
        service_account_scopes: list[Any] = []
        service_accounts = instance.get("serviceAccounts", [])
        if isinstance(service_accounts, list) and service_accounts:
            first_sa = service_accounts[0]
            if isinstance(first_sa, dict):
                service_account_email = first_sa.get("email")
                scopes = first_sa.get("scopes", [])
                service_account_scopes = scopes if isinstance(scopes, list) else []

        os_login_enabled_raw = metadata_map.get("enable-oslogin")
        if isinstance(os_login_enabled_raw, bool):
            os_login_enabled = os_login_enabled_raw
        elif isinstance(os_login_enabled_raw, str):
            os_login_enabled = os_login_enabled_raw.strip().lower() in {"1", "true", "yes", "enabled"}
        else:
            os_login_enabled = bool(os_login_enabled_raw)

        machine_type = instance.get("machineType")
        machine_type = str(machine_type).strip().rstrip("/").split("/")[-1] if machine_type else None
        instance_zone = instance.get("zone")
        instance_zone = str(instance_zone).strip().rstrip("/").split("/")[-1] if instance_zone else None

        normalized_instances.append(
            {
                "name": instance.get("name"),
                "status": str(instance.get("status") or "").upper(),
                "machine_type": machine_type,
                "zone": instance_zone,
                "labels": instance.get("labels", {}) if isinstance(instance.get("labels"), dict) else {},
                "public_ips": public_ips,
                "has_public_ip": bool(public_ips),
                "service_account_email": service_account_email,
                "service_account_scopes": service_account_scopes,
                "os_login_enabled": os_login_enabled,
            }
        )

    # -------------------------------------------------------------------
    # Validate presence-only rules
    # -------------------------------------------------------------------
    if "vm_instance" in rules:
        vm_instance_rule = rules["vm_instance"]
        if isinstance(vm_instance_rule, bool):
            expected = vm_instance_rule
        elif isinstance(vm_instance_rule, str):
            expected = vm_instance_rule.strip().lower() in {"1", "true", "yes", "enabled"}
        else:
            expected = bool(vm_instance_rule)
        instance_exists = len(normalized_instances) > 0
        if instance_exists != expected:
            return {
                "status": "FAILED",
                "action": "VM_VALIDATION",
                "reason": "UnexpectedInstanceFound" if instance_exists else "NoActiveInstanceFound",
                "region": region,
            }
        if len(rules) == 1:
            return {
                "status": "SUCCESS",
                "action": "VM_VALIDATION",
                "region": region,
                "data": {
                    "validation_results": {"vm_instance": True},
                    "message": "VM presence validation passed",
                    "instances": [instance.get("name") for instance in normalized_instances],
                },
            }

    if not normalized_instances:
        return {
            "status": "FAILED",
            "action": "VM_VALIDATION",
            "reason": "NoActiveInstanceFound",
            "region": region,
        }

    # -------------------------------------------------------------------
    # Apply validation rules on discovered VM instances
    # -------------------------------------------------------------------
    validation_results: Dict[str, bool] = {}
    matched_instances: list[dict[str, Any]] = []

    for instance in normalized_instances:
        instance_result: Dict[str, bool] = {}
        instance_name = instance.get("name")
        instance_status = instance.get("status")
        machine_type = instance.get("machine_type")
        instance_zone = instance.get("zone")
        has_public_ip = bool(instance.get("has_public_ip"))
        labels = instance.get("labels", {}) if isinstance(instance.get("labels"), dict) else {}
        service_account_email = instance.get("service_account_email")
        os_login_enabled = bool(instance.get("os_login_enabled"))

        logger.info(
            "Validating VM %s | status=%s | machine_type=%s | zone=%s | public_ip=%s",
            instance_name,
            instance_status,
            machine_type,
            instance_zone,
            has_public_ip,
        )

        if "instance_state" in rules:
            instance_result["instance_state"] = (
                instance_status == str(rules["instance_state"]).upper()
            )

        if "instance_name" in rules:
            instance_result["instance_name"] = str(instance_name) == str(rules["instance_name"])

        if "allowed_machine_types" in rules:
            allowed_machine_types = rules["allowed_machine_types"]
            if not isinstance(allowed_machine_types, list):
                allowed_machine_types = [allowed_machine_types]
            instance_result["allowed_machine_types"] = machine_type in allowed_machine_types

        if "zone" in rules:
            instance_result["zone"] = str(instance_zone) == str(rules["zone"])

        if "has_public_ip" in rules:
            has_public_ip_rule = rules["has_public_ip"]
            if isinstance(has_public_ip_rule, bool):
                expected_has_public_ip = has_public_ip_rule
            elif isinstance(has_public_ip_rule, str):
                expected_has_public_ip = has_public_ip_rule.strip().lower() in {"1", "true", "yes", "enabled"}
            else:
                expected_has_public_ip = bool(has_public_ip_rule)
            instance_result["has_public_ip"] = has_public_ip == expected_has_public_ip

        if "os_login_enabled" in rules:
            os_login_rule = rules["os_login_enabled"]
            if isinstance(os_login_rule, bool):
                expected_os_login = os_login_rule
            elif isinstance(os_login_rule, str):
                expected_os_login = os_login_rule.strip().lower() in {"1", "true", "yes", "enabled"}
            else:
                expected_os_login = bool(os_login_rule)
            instance_result["os_login_enabled"] = os_login_enabled == expected_os_login

        if "service_account_attached" in rules:
            service_account_attached_rule = rules["service_account_attached"]
            if isinstance(service_account_attached_rule, bool):
                expected_service_account_attached = service_account_attached_rule
            elif isinstance(service_account_attached_rule, str):
                expected_service_account_attached = (
                    service_account_attached_rule.strip().lower() in {"1", "true", "yes", "enabled"}
                )
            else:
                expected_service_account_attached = bool(service_account_attached_rule)
            instance_result["service_account_attached"] = (
                bool(service_account_email) == expected_service_account_attached
            )

        if "allowed_service_accounts" in rules:
            allowed_service_accounts = rules["allowed_service_accounts"]
            if not isinstance(allowed_service_accounts, list):
                allowed_service_accounts = [allowed_service_accounts]
            instance_result["allowed_service_accounts"] = (
                str(service_account_email or "") in {str(value) for value in allowed_service_accounts}
            )

        if "required_labels" in rules:
            required_labels = rules["required_labels"] if isinstance(rules["required_labels"], dict) else {}
            instance_result["required_labels"] = all(
                str(labels.get(key)) == str(value)
                for key, value in required_labels.items()
            )

        if instance_result:
            matched_instances.append(
                {
                    "name": instance_name,
                    "zone": instance_zone,
                    "machine_type": machine_type,
                    "validation_results": instance_result,
                }
            )
            validation_results.update(instance_result)
            if all(instance_result.values()):
                return {
                    "status": "SUCCESS",
                    "action": "VM_VALIDATION",
                    "region": region,
                    "data": {
                        "instance_name": instance_name,
                        "zone": instance_zone,
                        "machine_type": machine_type,
                        "validation_results": instance_result,
                        "matched_instances": matched_instances,
                    },
                }

    # -------------------------------------------------------------------
    # Return validation failure summary
    # -------------------------------------------------------------------
    return {
        "status": "FAILED",
        "action": "VM_VALIDATION",
        "reason": "ValidationRulesFailed",
        "region": region,
        "data": {
            "validation_results": validation_results,
            "matched_instances": matched_instances,
        },
    }
