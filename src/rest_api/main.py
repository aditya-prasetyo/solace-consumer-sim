"""
CDC Events REST API Server (FastAPI)
Mock target server that receives CDC events from PySpark and PyFlink consumers
"""

import logging
import os
import random
import re
import time

import yaml
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from .storage import EventStoreManager
from .routes.events import set_store
from .routes import events_router
from .models.events import HealthResponse, MetricsResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load config
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "configs")


def _resolve_env_vars(obj):
    """Resolve ${VAR:default} patterns in config values using environment variables."""
    if isinstance(obj, str):
        def _replace(match):
            var_name = match.group(1)
            default = match.group(2)
            return os.environ.get(var_name, default)
        return re.sub(r'\$\{(\w+):([^}]*)\}', _replace, obj)
    if isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_vars(item) for item in obj]
    return obj


def _load_config() -> dict:
    config_path = os.path.join(CONFIG_DIR, "api.yaml")
    if os.path.exists(config_path):
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}
        return _resolve_env_vars(raw)
    return {}


config = _load_config()
api_config = config.get("api", {})
sim_config = config.get("simulation", {})
storage_config = config.get("storage", {})

# Create app
app = FastAPI(
    title=api_config.get("app", {}).get("title", "CDC Events API"),
    description=api_config.get("app", {}).get("description", "Mock REST API for CDC Events"),
    version=api_config.get("app", {}).get("version", "1.0.0"),
)

# CORS
if api_config.get("cors", {}).get("enabled", True):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=api_config.get("cors", {}).get("allow_origins", ["*"]),
        allow_credentials=True,
        allow_methods=api_config.get("cors", {}).get("allow_methods", ["*"]),
        allow_headers=api_config.get("cors", {}).get("allow_headers", ["*"]),
    )

# Initialize storage
events_cfg = storage_config.get("events", {})
notif_cfg = storage_config.get("notifications", {})
store = EventStoreManager(
    events_max=events_cfg.get("max_size", 10000),
    events_ttl=events_cfg.get("ttl_seconds", 3600),
    notifications_max=notif_cfg.get("max_size", 1000),
    notifications_ttl=notif_cfg.get("ttl_seconds", 3600),
)

# Wire store to routes
set_store(store)

# Failure simulation middleware
failure_config = sim_config.get("failure", {})
latency_config = sim_config.get("latency", {})


@app.middleware("http")
async def simulate_conditions(request: Request, call_next):
    """Simulate failures and latency for testing"""
    # Simulate latency
    if latency_config.get("enabled", False):
        delay = random.uniform(
            latency_config.get("min_ms", 10) / 1000,
            latency_config.get("max_ms", 100) / 1000
        )
        time.sleep(delay)

    # Simulate failures
    if failure_config.get("enabled", False):
        if random.random() < failure_config.get("rate", 0.01):
            error_code = random.choice(failure_config.get("error_codes", [500]))
            store.record_error()
            return Response(
                content=f'{{"error": "Simulated error {error_code}"}}',
                status_code=error_code,
                media_type="application/json"
            )

    response = await call_next(request)
    return response


# Include routes
app.include_router(events_router)


# Health & metrics endpoints
@app.get("/health", response_model=HealthResponse, tags=["system"])
async def health_check():
    """Health check endpoint"""
    metrics = store.get_metrics()
    return HealthResponse(
        status="healthy",
        uptime_seconds=metrics["uptime_seconds"],
        events_received=metrics["total_events"],
        events_stored=store.events.count(),
    )


@app.get("/metrics", response_model=MetricsResponse, tags=["system"])
async def get_metrics():
    """Get server metrics"""
    return MetricsResponse(**store.get_metrics())


def start():
    """Start the API server"""
    import uvicorn

    host = api_config.get("server", {}).get("host", "0.0.0.0")
    port = int(api_config.get("server", {}).get("port", 8000))
    workers = int(api_config.get("server", {}).get("workers", 1))

    logger.info(f"Starting CDC Events API on {host}:{port}")
    uvicorn.run(
        "src.rest_api.main:app",
        host=host,
        port=port,
        workers=workers,
        reload=api_config.get("server", {}).get("reload", False),
    )


if __name__ == "__main__":
    start()
