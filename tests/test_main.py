"""Tests for the main application module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from src.main import AuthMiddleware, create_application, lifespan
from src.service.kb_auth import AdminPermission, KBaseUser


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


class TestAuthMiddleware:
    """Tests for the AuthMiddleware."""

    def _make_app_with_auth(self):
        app = FastAPI()
        app.add_middleware(AuthMiddleware)

        @app.get("/test")
        async def test_route():
            return {"ok": True}

        return app

    def test_no_auth_header_sets_none_user(self):
        app = self._make_app_with_auth()
        with patch("src.main.app_state") as mock_state:
            client = TestClient(app, raise_server_exceptions=False)
            response = client.get("/test")
        assert response.status_code == 200
        mock_state.set_request_user.assert_called_once()
        args = mock_state.set_request_user.call_args
        assert args[0][1] is None

    def test_valid_bearer_token(self):
        app = self._make_app_with_auth()
        user = KBaseUser("testuser", AdminPermission.NONE)

        with patch("src.main.app_state") as mock_state:
            mock_app_state_obj = MagicMock()
            mock_app_state_obj.auth.get_user = AsyncMock(return_value=user)
            mock_state.get_app_state.return_value = mock_app_state_obj

            client = TestClient(app, raise_server_exceptions=False)
            response = client.get("/test", headers={"Authorization": "Bearer valid_token"})

        assert response.status_code == 200

    def test_missing_scheme_raises(self):
        app = self._make_app_with_auth()
        app.add_exception_handler(
            Exception,
            lambda req, exc: __import__("fastapi.responses", fromlist=["JSONResponse"]).JSONResponse(
                status_code=401, content={"error": str(exc)}
            ),
        )
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/test", headers={"Authorization": "just-a-token"})
        assert response.status_code == 401

    def test_wrong_scheme_raises(self):
        app = self._make_app_with_auth()
        app.add_exception_handler(
            Exception,
            lambda req, exc: __import__("fastapi.responses", fromlist=["JSONResponse"]).JSONResponse(
                status_code=401, content={"error": str(exc)}
            ),
        )
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/test", headers={"Authorization": "Basic user:pass"})
        assert response.status_code == 401


class TestLifespan:
    """Tests for the lifespan context manager."""

    @pytest.mark.asyncio
    async def test_lifespan_calls_build_and_destroy(self):
        mock_app = MagicMock(spec=FastAPI)

        with (
            patch("src.main.app_state.build_app", new_callable=AsyncMock) as mock_build,
            patch("src.main.app_state.destroy_app_state", new_callable=AsyncMock) as mock_destroy,
        ):
            async with lifespan(mock_app):
                mock_build.assert_called_once_with(mock_app)
                mock_destroy.assert_not_called()

            mock_destroy.assert_called_once_with(mock_app)


class TestCreateApplication:
    """Tests for the create_application factory."""

    def test_creates_fastapi_app(self):
        app = create_application()
        assert isinstance(app, FastAPI)

    def test_includes_cluster_routes(self):
        app = create_application()
        paths = [route.path for route in app.routes]
        assert "/clusters" in paths

    def test_includes_health_route(self):
        app = create_application()
        paths = [route.path for route in app.routes]
        assert "/health" in paths
