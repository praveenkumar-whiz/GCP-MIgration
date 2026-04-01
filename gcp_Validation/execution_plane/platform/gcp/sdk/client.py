"""Generic GCP SDK Client wrapper for all Google Cloud API operations"""

import logging
import requests
from typing import Any, Dict, Optional
import json

logger = logging.getLogger(__name__)


class GCPAPIError(Exception):
    """Custom exception for GCP API errors"""
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Optional[Dict] = None):
        self.message = message
        self.status_code = status_code
        self.payload = payload or {}
        super().__init__(self.message)


class GCPSessionFactory:
    """Factory for creating authenticated sessions"""
    
    DEFAULT_TIMEOUT = 30
    
    @staticmethod
    def create_session(access_token: str, timeout: int = DEFAULT_TIMEOUT) -> requests.Session:
        """Create an authenticated requests session"""
        session = requests.Session()
        session.headers.update({
            "Authorization": "Bearer {}".format(access_token),
            "Content-Type": "application/json",
        })
        session.timeout = timeout
        return session


class GCPServiceClient:
    """Generic GCP Service Client for making API requests"""
    
    # Base URLs for different GCP services
    SERVICE_BASE_URLS = {
        # Compute & Networking
        "compute": "https://compute.googleapis.com/compute/v1",
        "dns": "https://dns.googleapis.com/dns/v1",
        "networkmanagement": "https://networkmanagement.googleapis.com/v1",
        "servicenetworking": "https://servicenetworking.googleapis.com/v1",
        
        # Storage
        "storage": "https://storage.googleapis.com/storage/v1",
        "bigtable": "https://bigtableadmin.googleapis.com/v2",
        "firestore": "https://firestore.googleapis.com/v1",
        "datastore": "https://datastore.googleapis.com/v1",
        "spanner": "https://spanner.googleapis.com/v1",
        
        # Databases
        "sqladmin": "https://sqladmin.googleapis.com/v1",
        "redis": "https://redis.googleapis.com/v1",
        "memcache": "https://memcache.googleapis.com/v1",
        "datamigration": "https://datamigration.googleapis.com/v1",
        
        # Container & Kubernetes
        "container": "https://container.googleapis.com/v1",
        "gkehub": "https://gkehub.googleapis.com/v1",
        "batch": "https://batch.googleapis.com/v1",
        
        # Serverless
        "run": "https://run.googleapis.com/v2",
        "cloudfunctions": "https://cloudfunctions.googleapis.com/v2",
        "appengine": "https://appengine.googleapis.com/v1",
        "cloudbuild": "https://cloudbuild.googleapis.com/v1",
        "cloudscheduler": "https://cloudscheduler.googleapis.com/v1",
        "cloudtasks": "https://cloudtasks.googleapis.com/v2",
        
        # Data Analytics
        "bigquery": "https://bigquery.googleapis.com/bigquery/v2",
        "dataflow": "https://dataflow.googleapis.com/v1b3",
        "datapipelines": "https://datapipelines.googleapis.com/v1",
        "dataproc": "https://dataproc.googleapis.com/v1",
        "datafusion": "https://datafusion.googleapis.com/v1",
        "pubsub": "https://pubsub.googleapis.com/v1",
        "composer": "https://composer.googleapis.com/v1",
        "eventarc": "https://eventarc.googleapis.com/v1",
        "looker": "https://looker.googleapis.com/v1",
        
        # AI & ML
        "aiplatform": "https://aiplatform.googleapis.com/v1",
        "ml": "https://ml.googleapis.com/v1",
        "vision": "https://vision.googleapis.com/v1",
        "translate": "https://translation.googleapis.com/v3",
        "speech": "https://speech.googleapis.com/v1",
        "language": "https://language.googleapis.com/v1",
        "notebooks": "https://notebooks.googleapis.com/v2",
        "documentai": "https://documentai.googleapis.com/v1",
        
        # Security & Identity
        "iam": "https://iam.googleapis.com/v1",
        "iamcredentials": "https://iamcredentials.googleapis.com/v1",
        "cloudkms": "https://cloudkms.googleapis.com/v1",
        "secretmanager": "https://secretmanager.googleapis.com/v1",
        "securitycenter": "https://securitycenter.googleapis.com/v1",
        "binaryauthorization": "https://binaryauthorization.googleapis.com/v1",
        
        # Management & Monitoring
        "logging": "https://logging.googleapis.com/v2",
        "monitoring": "https://monitoring.googleapis.com/v3",
        "cloudtrace": "https://cloudtrace.googleapis.com/v2",
        "clouderrorreporting": "https://clouderrorreporting.googleapis.com/v1beta1",
        "cloudprofiler": "https://cloudprofiler.googleapis.com/v2",
        
        # Resource Management
        "cloudresourcemanager": "https://cloudresourcemanager.googleapis.com/v3",
        "serviceusage": "https://serviceusage.googleapis.com/v1",
        "cloudbilling": "https://cloudbilling.googleapis.com/v1",
        
        # Developer Tools
        "sourcerepo": "https://sourcerepo.googleapis.com/v1",
        "artifactregistry": "https://artifactregistry.googleapis.com/v1",
        "containeranalysis": "https://containeranalysis.googleapis.com/v1",
        "deploymentmanager": "https://deploymentmanager.googleapis.com/deploymentmanager/v2",
        
        # Migration & Transfer
        "storagetransfer": "https://storagetransfer.googleapis.com/v1",
        "vmmigration": "https://vmmigration.googleapis.com/v1",
        
        # IoT & Edge
        "cloudiot": "https://cloudiot.googleapis.com/v1",
        
        # API Management
        "apigateway": "https://apigateway.googleapis.com/v1",
        "endpoints": "https://servicemanagement.googleapis.com/v1",
        
        # Healthcare & Life Sciences
        "healthcare": "https://healthcare.googleapis.com/v1",
        
        # Media & Gaming
        "gameservices": "https://gameservices.googleapis.com/v1",
    }
    
    def __init__(self, base_url: str, session: requests.Session, timeout: int):
        self.base_url = str(base_url).rstrip("/")
        self.session = session
        self.timeout = timeout
    
    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make a generic HTTP request to GCP API
        
        Args:
            method: HTTP method (GET, POST, DELETE, PUT, PATCH)
            path: API path (e.g., 'projects/{project}/zones/{zone}/instances')
            params: Query parameters
            json_body: JSON request body
            
        Returns:
            Response payload as dictionary
        """
        # Construct the full URL
        url = "{}/{}".format(self.base_url, path.lstrip('/'))
        
        logger.info("GCP API request | {} {}".format(method, path))
        
        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json_body,
                timeout=self.timeout,
            )
            
            # Log response status
            logger.info("GCP API response | status={}".format(response.status_code))
            
            # Handle error responses
            if response.status_code >= 400:
                try:
                    error_payload = response.json()
                except:
                    error_payload = {"error": {"message": response.text}}
                
                # Safely extract error message
                error_obj = error_payload.get("error", {})
                if isinstance(error_obj, dict):
                    error_message = error_obj.get("message", response.text)
                elif isinstance(error_obj, str):
                    error_message = error_obj
                else:
                    error_message = response.text
                
                logger.error("GCP API error | status={} | message={}".format(response.status_code, error_message))
                raise GCPAPIError(
                    message="GCP API error: {}".format(error_message),
                    status_code=response.status_code,
                    payload=error_payload
                )
            
            # Parse and return successful response
            if response.status_code == 204:  # No Content
                return {}
            
            try:
                payload = response.json()
            except:
                payload = {"raw_response": response.text}
            
            return payload
            
        except requests.exceptions.Timeout:
            raise GCPAPIError(
                message="Request timeout after {}s".format(self.timeout),
                status_code=408
            )
        except requests.exceptions.ConnectionError as e:
            raise GCPAPIError(
                message="Connection error: {}".format(str(e)),
                status_code=503
            )
        except GCPAPIError:
            raise
        except Exception as e:
            logger.exception("Unexpected error in request")
            raise GCPAPIError(
                message="Unexpected error: {}".format(str(e)),
                status_code=500
            )


class GCPClientFactory:
    """
    Centralized GCP client factory.
    Keeps the SDK layer small and generic like AWSClientFactory.
    """
    
    @staticmethod
    def create(
        service: str,
        *,
        creds: Optional[Dict[str, Any]] = None,
    ) -> GCPServiceClient:
        """
        Create a GCP service client
        
        Args:
            service: Service name (e.g., 'compute', 'storage', 'iam')
            creds: Credentials dictionary with 'access_token' and optional 'timeout'
            
        Returns:
            GCPServiceClient instance
        """
        resolved_service = str(service or "").strip().lower()
        resolved_creds = creds or {}
        
        # Extract credentials
        access_token = resolved_creds.get("access_token")
        timeout = int(resolved_creds.get("timeout") or GCPSessionFactory.DEFAULT_TIMEOUT)
        
        logger.info(
            "Creating GCP client | service={} | credentialed={}".format(resolved_service, bool(access_token))
        )
        
        # Validate access token
        if not access_token:
            raise ValueError("gcp access_token is required for GCP operations")
        
        # Get base URL for the service
        base_url = GCPServiceClient.SERVICE_BASE_URLS.get(resolved_service)
        if not base_url:
            # If service not in predefined list, construct a generic URL
            base_url = "https://{}.googleapis.com/v1".format(resolved_service)
            logger.warning(
                "Service '{}' not in predefined list. Using generic URL: {}".format(resolved_service, base_url)
            )
        
        # Create authenticated session
        session = GCPSessionFactory.create_session(access_token, timeout)
        
        # Return the service client
        return GCPServiceClient(
            base_url=base_url,
            session=session,
            timeout=timeout
        )