
"""Tests for the Node 4 FastAPI application."""

from fastapi.testclient import TestClient

from distributed_search.serving.api import create_app


class FakeSearchService:
    """Provide predictable search results for API tests."""

    version = 1

    def search(self, query: str, limit: int = 10) -> dict:
        """Return sample ranked search results."""
        results = [
            {
                "doc_id": "doc-1",
                "title": "Distributed Systems",
                "url": "https://example.com/doc-1",
                "score": 0.95,
                "version": self.version,
            },
            {
                "doc_id": "doc-2",
                "title": "Search Engines",
                "url": "https://example.com/doc-2",
                "score": 0.75,
                "version": self.version,
            },
        ]

        return {
            "query": query,
            "version": self.version,
            "total_results": len(results),
            "results": results[:limit],
        }


def create_test_client() -> TestClient:
    """Create a test client with an injected fake search service."""
    app = create_app(service=FakeSearchService())
    return TestClient(app)


def test_health_endpoint():
    with create_test_client() as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "version": 1,
    }


def test_search_endpoint():
    with create_test_client() as client:
        response = client.get(
            "/search",
            params={"q": "distributed systems"},
        )

    assert response.status_code == 200

    data = response.json()

    assert data["query"] == "distributed systems"
    assert data["version"] == 1
    assert data["total_results"] == 2
    assert data["results"][0]["doc_id"] == "doc-1"
    assert data["results"][0]["title"] == "Distributed Systems"
    assert data["results"][0]["score"] == 0.95


def test_search_respects_limit():
    with create_test_client() as client:
        response = client.get(
            "/search",
            params={"q": "search", "limit": 1},
        )

    assert response.status_code == 200
    assert len(response.json()["results"]) == 1


def test_search_rejects_empty_query():
    with create_test_client() as client:
        response = client.get("/search", params={"q": ""})

    assert response.status_code == 422


def test_search_rejects_invalid_limit():
    with create_test_client() as client:
        response = client.get(
            "/search",
            params={"q": "search", "limit": 0},
        )

    assert response.status_code == 422


def test_search_service_unavailable():
    app = create_app(service=FakeSearchService())

    with TestClient(app) as client:
        app.state.search_service = None
        response = client.get("/search", params={"q": "search"})

    assert response.status_code == 503


def test_homepage_serves_html():
    with create_test_client() as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Distributed Search Engine" in response.text


def test_stylesheet_is_served():
    with create_test_client() as client:
        response = client.get("/static/style.css")

    assert response.status_code == 200
    assert "text/css" in response.headers["content-type"]


def test_javascript_is_served():
    with create_test_client() as client:
        response = client.get("/static/script.js")

    assert response.status_code == 200
    assert "javascript" in response.headers["content-type"]
