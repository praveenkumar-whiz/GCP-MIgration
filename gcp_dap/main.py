import sys
import os

# ------------------------
# Ensure project root in sys.path FIRST
# ------------------------
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import importlib
import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Dict
from io import StringIO

# ------------------------
# Logger configuration - CONFIGURE ROOT LOGGER
# ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Suppress SDK client logs (only show WARNING and above)
logging.getLogger("execution_plane.platform.gcp.sdk.client").setLevel(logging.WARNING)

logger = logging.getLogger("dynamic-gcp-api")

# ------------------------
# FastAPI app
# ------------------------
app = FastAPI(title="Dynamic GCP Service API")

# ------------------------
# Request model
# ------------------------
class ServiceRequest(BaseModel):
    service: str
    action: str
    event: Dict[str, Any]

# ------------------------
# API Route
# ------------------------
@app.post("/execute")
async def execute_service(request: ServiceRequest):
    # Store logs in a list
    log_messages = []
    
    # Create a custom handler to capture logs
    class ListHandler(logging.Handler):
        def emit(self, record):
            log_messages.append(self.format(record))
    
    # Add the list handler to root logger
    list_handler = ListHandler()
    list_handler.setFormatter(logging.Formatter("%(levelname)s [%(name)s]: %(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(list_handler)
    
    try:
        service = request.service.strip()
        action = request.action.strip()
        event = request.event

        logger.info(f"Received request - service: {service}, action: {action}")

        # Validate action format
        if not action.endswith("_spec"):
            raise HTTPException(status_code=400, detail="Invalid action format. Must end with '_spec'")

        # Determine module path dynamically
        module_name = action.replace("_spec", "")
        module_path = f"drivers.{service}.operations.{module_name}"

        logger.info(f"Importing module: {module_path}")
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError as e:
            logger.error(f"Module not found: {e}")
            raise HTTPException(status_code=400, detail=f"Module '{module_path}' not found. Error: {str(e)}")

        # Get the function dynamically
        if not hasattr(module, action):
            raise HTTPException(status_code=400, detail=f"Function '{action}' not found in module '{module_path}'")
        func = getattr(module, action)

        # Call the function with the event dictionary
        logger.info(f"Calling function '{action}' with event data")
        result = func(event)

        logger.info("Function call completed successfully")

        return {
            "status": "SUCCESS",
            "result": result,
            "logs": log_messages
        }

    except HTTPException as he:
        logger.error(f"HTTPException: {he.detail}")
        return JSONResponse(status_code=he.status_code, content={
            "status": "FAILED",
            "error": he.detail,
            "logs": log_messages,
        })

    except Exception as e:
        logger.exception("Unexpected error during execution")
        return JSONResponse(status_code=500, content={
            "status": "FAILED",
            "error": str(e),
            "logs": log_messages,
        })
    
    finally:
        # Remove the list handler
        root_logger.removeHandler(list_handler)

# ------------------------
# Health check endpoint
# ------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "dynamic-gcp-api"}