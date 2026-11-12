
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

    @app.get("/search")
    def search(
        q: str = Query(..., min_length=1, description="Search query"),
        limit: int = Query(10, ge=1, le=100),
    ) -> dict[str, Any]:
        """Search documents and return ranked results."""
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

        try:
            return search_service.search(q, limit)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc

    return app


app = create_app()
