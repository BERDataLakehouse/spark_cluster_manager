"""Tests for the app state module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.service.app_state import (
    AppState,
    RequestState,
    _get_app_state_from_app,
    build_app,
    destroy_app_state,
    get_app_state,
    get_request_user,
    set_request_user,
)
from src.service.kb_auth import AdminPermission, KBaseUser


class TestBuildApp:
    """Tests for build_app function."""

    @pytest.mark.asyncio
    async def test_build_app_initializes_state(self):
        mock_app = MagicMock()
        mock_app.state = MagicMock()

        with patch(
            "src.service.app_state.KBaseAuth.create",
            new_callable=AsyncMock,
        ) as mock_create:
            mock_auth = MagicMock()
            mock_create.return_value = mock_auth
            await build_app(mock_app)

        assert mock_app.state._auth == mock_auth
        assert isinstance(mock_app.state._spark_state, AppState)

    @pytest.mark.asyncio
    async def test_build_app_uses_env_vars(self):
        mock_app = MagicMock()
        mock_app.state = MagicMock()

        with (
            patch.dict(
                "os.environ",
                {
                    "KBASE_AUTH_URL": "http://custom/auth/",
                    "KBASE_ADMIN_ROLES": "ADMIN1,ADMIN2",
                },
            ),
            patch(
                "src.service.app_state.KBaseAuth.create",
                new_callable=AsyncMock,
            ) as mock_create,
        ):
            mock_create.return_value = MagicMock()
            await build_app(mock_app)

        mock_create.assert_called_once_with(
            "http://custom/auth/", full_admin_roles=["ADMIN1", "ADMIN2"]
        )


class TestDestroyAppState:
    """Tests for destroy_app_state function."""

    @pytest.mark.asyncio
    async def test_destroy_app_state_completes(self):
        mock_app = MagicMock()
        await destroy_app_state(mock_app)


class TestGetAppState:
    """Tests for get_app_state and _get_app_state_from_app."""

    def test_get_app_state_success(self):
        mock_request = MagicMock()
        mock_auth = MagicMock()
        expected_state = AppState(auth=mock_auth)
        mock_request.app.state._spark_state = expected_state
        result = get_app_state(mock_request)
        assert result == expected_state

    def test_get_app_state_not_initialized(self):
        mock_app = MagicMock(spec=[])
        mock_app.state = MagicMock(spec=[])
        with pytest.raises(ValueError, match="App state has not been initialized"):
            _get_app_state_from_app(mock_app)

    def test_get_app_state_none_value(self):
        mock_app = MagicMock()
        mock_app.state._spark_state = None
        with pytest.raises(ValueError, match="App state has not been initialized"):
            _get_app_state_from_app(mock_app)


class TestRequestUser:
    """Tests for set_request_user and get_request_user."""

    def test_set_and_get_request_user(self):
        mock_request = MagicMock()
        mock_request.state = MagicMock()
        user = KBaseUser("testuser", AdminPermission.NONE)
        set_request_user(mock_request, user)
        assert mock_request.state._request_state == RequestState(user=user)

    def test_set_request_user_none(self):
        mock_request = MagicMock()
        mock_request.state = MagicMock()
        set_request_user(mock_request, None)
        assert mock_request.state._request_state == RequestState(user=None)

    def test_get_request_user_not_set(self):
        mock_request = MagicMock(spec=[])
        mock_request.state = MagicMock(spec=[])
        result = get_request_user(mock_request)
        assert result is None

    def test_get_request_user_none_state(self):
        mock_request = MagicMock()
        mock_request.state._request_state = None
        result = get_request_user(mock_request)
        assert result is None
