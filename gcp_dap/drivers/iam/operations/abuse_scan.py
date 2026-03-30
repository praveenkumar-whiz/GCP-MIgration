import json
import logging
from typing import Any, Dict, List
from execution_plane.platform.gcp.sdk.client import GCPAPIError, GCPClientFactory

# -------------------------------------------------------------------
# Logger configuration
# -------------------------------------------------------------------
logger = logging.getLogger("gcp-iam-abuse")
logger.setLevel(logging.INFO)


def abuse_scan_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    GCP IAM Policy abuse scan.
    
    IAM is a GLOBAL service (not regional).
    
    Expected spec keys:
    - project_id: GCP project ID
    - gcp_access_token / access_token: GCP access token
    - rules: {
          "allowed_user_domains": ["@whizlabs.com", "@example.com"],
          "forbidden_roles": ["roles/owner", "roles/editor"]
      }
    """
    action = "IAM_ABUSE_SCAN"
    
    logger.info("IAM Policy Abuse Scan triggered")
    logger.info("Incoming spec: {}".format(json.dumps(spec)))
    
    abuse_message: List[str] = []
    rules = spec.get("rules", {}) or {}
    
    logger.info("Abuse rules: {}".format(json.dumps(rules)))
    
    project_id = spec.get("project_id")
    
    # -------------------------------------------------------------------
    # VALIDATION
    # -------------------------------------------------------------------
    if not project_id:
        return {
            "status": "FAILED",
            "action": action,
            "reason": "MissingProjectId",
            "detail": "project_id is required",
        }
    
    try:
        # -------------------------------------------------------------------
        # CREDENTIALS
        # -------------------------------------------------------------------
        access_token = (
            spec.get("gcp_access_token")
            or spec.get("access_token")
            or spec.get("google_access_token")
        )
        
        if not access_token:
            return {
                "status": "FAILED",
                "action": action,
                "reason": "MissingAccessToken",
                "detail": "Access token is required",
                "project_id": project_id,
            }
        
        # -------------------------------------------------------------------
        # CLIENT CREATION - IAM uses cloudresourcemanager service
        # -------------------------------------------------------------------
        try:
            crm_client = GCPClientFactory.create(
                service="cloudresourcemanager",
                creds={
                    "access_token": access_token,
                },
            )
        except Exception as e:
            logger.error("Client creation failed", exc_info=True)
            return {
                "status": "FAILED",
                "action": action,
                "reason": "ClientCreationFailed",
                "error": str(e),
                "project_id": project_id,
            }
        
        logger.info("Connected to Cloud Resource Manager | Project={}".format(project_id))
        
        # -------------------------------------------------------------------
        # GET IAM POLICY FOR PROJECT
        # Reference: https://cloud.google.com/resource-manager/reference/rest/v1/projects/getIamPolicy
        # POST https://cloudresourcemanager.googleapis.com/v1/projects/{resource}:getIamPolicy
        # -------------------------------------------------------------------
        try:
            # Get IAM policy for the project
            policy_response = crm_client.request(
                "POST",
                "projects/{}:getIamPolicy".format(project_id),
                json_body={}
            )
            
            bindings = policy_response.get("bindings", [])
            etag = policy_response.get("etag", "")
            version = policy_response.get("version", 1)
            
            number_of_bindings = len(bindings)
            
            logger.info("IAM Policy | Project={} | Bindings={}".format(
                project_id, number_of_bindings
            ))
            
        except Exception as e:
            logger.error("Get IAM policy failed", exc_info=True)
            return {
                "status": "FAILED",
                "action": action,
                "reason": "GetIamPolicyFailed",
                "error": str(e),
                "project_id": project_id,
            }
        
        # -------------------------------------------------------------------
        # APPLY RULES
        # -------------------------------------------------------------------
        
        # Rule: allowed_user_domains
        if "allowed_user_domains" in rules:
            allowed_domains = rules.get("allowed_user_domains")
            if not isinstance(allowed_domains, list) or not allowed_domains:
                return {
                    "status": "FAILED",
                    "action": action,
                    "reason": "InvalidRuleValue",
                    "detail": "allowed_user_domains must be a non-empty list",
                }
            
            allowed_domains_set = set(allowed_domains)
            allowed_domains_str = ", ".join(sorted(allowed_domains_set))
            
            for binding in bindings:
                role = binding.get("role", "")
                members = binding.get("members", [])
                
                for member in members:
                    # Check user accounts (format: user:email@domain.com)
                    if member.startswith("user:"):
                        email = member.replace("user:", "")
                        
                        # Check if email ends with any allowed domain
                        domain_match = False
                        for domain in allowed_domains_set:
                            if email.endswith(domain):
                                domain_match = True
                                break
                        
                        if not domain_match:
                            abuse_message.append(
                                "Project {} | Unauthorized user '{}' assigned to role '{}', allowed domains: {}.".format(
                                    project_id, email, role, allowed_domains_str
                                )
                            )
                            logger.warning(
                                "ABUSE DETECTED - Unauthorized user domain | Project={} | User={} | Role={} | AllowedDomains={}".format(
                                    project_id, email, role, allowed_domains_str
                                )
                            )
        
        # Rule: forbidden_roles
        if "forbidden_roles" in rules:
            forbidden_roles = rules.get("forbidden_roles")
            if not isinstance(forbidden_roles, list) or not forbidden_roles:
                return {
                    "status": "FAILED",
                    "action": action,
                    "reason": "InvalidRuleValue",
                    "detail": "forbidden_roles must be a non-empty list",
                }
            
            forbidden_roles_set = set(forbidden_roles)
            forbidden_roles_str = ", ".join(sorted(forbidden_roles_set))
            
            for binding in bindings:
                role = binding.get("role", "")
                members = binding.get("members", [])
                
                if role in forbidden_roles_set:
                    for member in members:
                        abuse_message.append(
                            "Project {} | Member '{}' assigned to forbidden role '{}', forbidden roles: {}.".format(
                                project_id, member, role, forbidden_roles_str
                            )
                        )
                        logger.warning(
                            "ABUSE DETECTED - Forbidden role assignment | Project={} | Member={} | Role={} | ForbiddenRoles={}".format(
                                project_id, member, role, forbidden_roles_str
                            )
                        )
        
        # -------------------------------------------------------------------
        # RESPONSE DATA
        # -------------------------------------------------------------------
        response_data = {
            "number_of_bindings": number_of_bindings,
            "policy_version": version,
        }
        
        # -------------------------------------------------------------------
        # FINAL RESULT
        # -------------------------------------------------------------------
        if abuse_message:
            logger.warning(
                "IAM abuse scan FAILED | Project={} | Issues={}".format(
                    project_id, len(abuse_message)
                )
            )
            return {
                "status": "FAILED",
                "action": action,
                "reason": "ResourceLimitExceeded",
                "project_id": project_id,
                "abuse_message": abuse_message,
                "data": response_data,
            }
        
        # ------------------------------------------------------------
        # Abuse scan passed
        # ------------------------------------------------------------
        logger.info("IAM abuse scan PASSED | Project={}".format(project_id))
        
        return {
            "status": "SUCCESS",
            "action": action,
            "project_id": project_id,
            "abuse_message": [],
            "data": response_data,
        }
        
    except GCPAPIError as e:
        logger.error("GCP API Error", exc_info=True)
        return {
            "status": "FAILED",
            "action": action,
            "reason": "GCPAPIError",
            "error": str(e),
            "project_id": project_id,
        }
    
    except Exception as e:
        logger.error("Unhandled Exception", exc_info=True)
        return {
            "status": "FAILED",
            "action": action,
            "reason": "UnhandledException",
            "error": str(e),
            "project_id": project_id,
        }
