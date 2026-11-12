"""Tests for Phase 0 / Phase 1 API version selection."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from distributed_search.serving.api import create_app


class FakeSearchService:
    def __init__(self, version):
        self.version = version

    def search(self, query, limit):
        return {
            "query": query,
            "version": self.version,
            "results": [],
        }


def test_default_version_remains_available():
    app = create_app(service=FakeSearchService(0))

    with TestClient(app) as client:
        response = client.get("/search?q=spark")

    assert response.status_code == 200
    assert response.json()["version"] == 0


def test_versions_with_injected_service():
    app = create_app(service=FakeSearchService(0))

    with TestClient(app) as client:
        response = client.get("/versions")

    assert response.status_code == 200
    assert response.json() == {
        "default_version": 0,
        "available_versions": [0],
    }


def test_missing_version_returns_404():
    app = create_app(service=FakeSearchService(0))

    with TestClient(app) as client:
        response = client.get("/search?q=spark&version=1")

    assert response.status_code == 404


def test_invalid_version_returns_422():
    app = create_app(service=FakeSearchService(0))

    with TestClient(app) as client:
        response = client.get("/search?q=spark&version=2")

    assert response.status_code == 422


def test_phase_one_export_can_be_selected(tmp_path):
    serving_root = tmp_path / "serving"
    v0 = serving_root / "v0"
    v1 = serving_root / "v1"

    v0.mkdir(parents=True)
    v1.mkdir(parents=True)

    (v0 / "manifest.json").write_text("{}")
    (v1 / "manifest.json").write_text("{}")

    def fake_loader(path):
        version = int(path.name[1:])
        return {"version": version}

    with (
        patch(
            "distributed_search.serving.api.load_serving_data",
            side_effect=fake_loader,
        ),
        patch(
            "distributed_search.serving.api.SearchService",
            side_effect=lambda data: FakeSearchService(data["version"]),
        ),
    ):
        app = create_app(export_dir=v0)

        with TestClient(app) as client:
            versions = client.get("/versions")
            result = client.get("/search?q=spark&version=1")
            default = client.get("/search?q=spark")

    assert versions.status_code == 200
    assert versions.json()["available_versions"] == [0, 1]
    assert result.status_code == 200
    assert result.json()["version"] == 1
    assert default.status_code == 200
    assert default.json()["version"] == 0
