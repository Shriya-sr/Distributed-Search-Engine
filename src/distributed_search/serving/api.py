
"""FastAPI application for the Node 4 search service."""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from distributed_search.serving.loader import load_serving_data
from distributed_search.serving.service import SearchService


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FRONTEND_DIR = PROJECT_ROOT / "frontend"


def create_app(
    export_dir: str | Path | None = None,
    service: SearchService | None = None,
    frontend_dir: str | Path | None = None,
) -> FastAPI:
    """Create the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Load the local search index when the application starts."""
        if service is not None:
            app.state.search_service = service
        else:
            configured_dir = export_dir or os.getenv(
                "SEARCH_EXPORT_DIR",
                "data/serving/v0",
            )

            serving_data = load_serving_data(configured_dir)
            app.state.search_service = SearchService(serving_data)

        # Cache additional versions only when requested.
        app.state.version_services = {
            app.state.search_service.version: app.state.search_service
        }
        if service is None:
            app.state.serving_root = Path(configured_dir).parent
        else:
            app.state.serving_root = None

        yield

    app = FastAPI(
        title="Distributed Search Engine",
        description="Local search API for Node 4",
        version="1.0.0",
        lifespan=lifespan,
    )

    static_dir = Path(frontend_dir) if frontend_dir else FRONTEND_DIR

    if static_dir.is_dir():
        app.mount(
            "/static",
            StaticFiles(directory=static_dir),
            name="static",
        )

    @app.get("/", include_in_schema=False)
    def home():
        """Serve the frontend homepage."""
        index_file = static_dir / "index.html"

        if not index_file.is_file():
            raise HTTPException(
                status_code=404,
                detail="Frontend index.html not found",
            )

        return FileResponse(index_file)

    @app.get("/health")
    def health() -> dict[str, Any]:
        """Return the health status of the search service."""
        search_service = getattr(
            app.state,
            "search_service",
            None,
        )

        if search_service is None:
            raise HTTPException(
                status_code=503,
                detail="Search service is not ready",
            )

        return {
            "status": "healthy",
            "version": search_service.version,
        }

    def get_version_service(version: int | None):
        default_service = getattr(app.state, "search_service", None)

        if default_service is None:
            raise HTTPException(503, "Search service is not ready")

        if version is None:
            return default_service

        services = app.state.version_services

        if version in services:
            return services[version]

        root = app.state.serving_root
        if root is None:
            raise HTTPException(404, "Requested version is unavailable")

        version_dir = root / f"v{version}"
        if not version_dir.is_dir():
            raise HTTPException(404, "Requested version is unavailable")

        try:
            loaded = load_serving_data(version_dir)
            if loaded["version"] != version:
                raise ValueError("Export version mismatch")
            selected = SearchService(loaded)
        except (ValueError, OSError, KeyError) as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Version {version} could not be loaded",
            ) from exc

        services[version] = selected
        return selected

    @app.get("/versions")
    def versions():
        """List the serving versions currently available."""
        default_service = getattr(app.state, "search_service", None)
        if default_service is None:
            raise HTTPException(503, "Search service is not ready")

        available = set(app.state.version_services)
        root = app.state.serving_root

        if root is not None and root.is_dir():
            for candidate in root.iterdir():
                if (
                    candidate.is_dir()
                    and candidate.name in ("v0", "v1")
                    and (candidate / "manifest.json").is_file()
                ):
                    available.add(int(candidate.name[1:]))

        return {
            "default_version": default_service.version,
            "available_versions": sorted(available),
        }

    @app.get("/search")
    def search(
        q: str = Query(..., min_length=1, description="Search query"),
        limit: int = Query(10, ge=1, le=100),
        version: int | None = Query(None, ge=0, le=1),
    ) -> dict[str, Any]:
        """Search documents and return ranked results."""
        search_service = get_version_service(version)

        try:
            return search_service.search(q, limit)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc

    return app


app = create_app()
