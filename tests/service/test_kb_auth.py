"""Tests for the KBase authentication module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.service.exceptions import InvalidTokenError, MissingRoleError
from src.service.kb_auth import (
    AdminPermission,
    KBaseAuth,
    KBaseUser,
    _check_error,
    _get,
)


class TestCheckError:
    """Tests for the _check_error helper function."""

    @pytest.mark.asyncio
    async def test_success_status_does_nothing(self):
        response = MagicMock()
        response.status = 200
        await _check_error(response)

    @pytest.mark.asyncio
    async def test_non_json_response_raises_ioerror(self):
        response = MagicMock()
        response.status = 500
        response.json = AsyncMock(side_effect=ValueError("not json"))
        response.text = "Internal Server Error"
        with pytest.raises(IOError, match="Non-JSON response"):
            await _check_error(response)

    @pytest.mark.asyncio
    async def test_invalid_token_error(self):
        response = MagicMock()
        response.status = 401
        response.json = AsyncMock(
            return_value={"error": {"appcode": 10020, "message": "Invalid token"}}
        )
        with pytest.raises(InvalidTokenError, match="token is invalid"):
            await _check_error(response)

    @pytest.mark.asyncio
    async def test_other_json_error_raises_ioerror(self):
        response = MagicMock()
        response.status = 403
        response.json = AsyncMock(
            return_value={"error": {"appcode": 99999, "message": "Forbidden access"}}
        )
        with pytest.raises(IOError, match="Forbidden access"):
            await _check_error(response)


class TestGet:
    """Tests for the _get helper function."""

    @pytest.mark.asyncio
    async def test_successful_get(self):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"servicename": "Auth"})

        # aiohttp uses nested async context managers: ClientSession() and session.get()
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get.return_value = mock_get_cm

        mock_session_cm = AsyncMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await _get("http://test/", {"Accept": "application/json"})

        assert result == {"servicename": "Auth"}


class TestKBaseAuthInit:
    """Tests for KBaseAuth constructor."""

    def test_valid_init(self):
        auth = KBaseAuth(
            "http://auth/",
            required_roles=["ROLE1"],
            full_admin_roles=["ADMIN"],
            cache_max_size=100,
            cache_expiration=60,
            service_name="Authentication Service",
        )
        assert auth._url == "http://auth/"
        assert auth._req_roles == {"ROLE1"}
        assert auth._full_roles == {"ADMIN"}

    def test_no_required_roles(self):
        auth = KBaseAuth(
            "http://auth/",
            required_roles=None,
            full_admin_roles=None,
            cache_max_size=100,
            cache_expiration=60,
            service_name="Authentication Service",
        )
        assert auth._req_roles is None
        assert auth._full_roles == set()

    def test_wrong_service_name_raises(self):
        with pytest.raises(IOError, match="does not appear to be"):
            KBaseAuth(
                "http://auth/",
                required_roles=None,
                full_admin_roles=None,
                cache_max_size=100,
                cache_expiration=60,
                service_name="Wrong Service",
            )


class TestKBaseAuthCreate:
    """Tests for KBaseAuth.create classmethod."""

    @pytest.mark.asyncio
    async def test_create_appends_slash(self):
        with patch(
            "src.service.kb_auth._get",
            new_callable=AsyncMock,
            return_value={"servicename": "Authentication Service"},
        ):
            auth = await KBaseAuth.create("http://auth")
            assert auth._url == "http://auth/"

    @pytest.mark.asyncio
    async def test_create_with_trailing_slash(self):
        with patch(
            "src.service.kb_auth._get",
            new_callable=AsyncMock,
            return_value={"servicename": "Authentication Service"},
        ):
            auth = await KBaseAuth.create("http://auth/")
            assert auth._url == "http://auth/"

    @pytest.mark.asyncio
    async def test_create_with_falsy_url_raises(self):
        with pytest.raises(ValueError, match="auth_url is required"):
            await KBaseAuth.create("")


class TestKBaseAuthGetUser:
    """Tests for KBaseAuth.get_user method."""

    def _make_auth(self, required_roles=None, full_admin_roles=None):
        return KBaseAuth(
            "http://auth/",
            required_roles=required_roles,
            full_admin_roles=full_admin_roles,
            cache_max_size=100,
            cache_expiration=60,
            service_name="Authentication Service",
        )

    @pytest.mark.asyncio
    async def test_get_user_cache_miss(self):
        auth = self._make_auth()
        with patch(
            "src.service.kb_auth._get",
            new_callable=AsyncMock,
            return_value={"user": "testuser", "customroles": []},
        ):
            user = await auth.get_user("valid_token")
        assert user == KBaseUser("testuser", AdminPermission.NONE)

    @pytest.mark.asyncio
    async def test_get_user_cache_hit(self):
        auth = self._make_auth()
        auth._cache.set("cached_token", ("cacheduser", AdminPermission.FULL))
        user = await auth.get_user("cached_token")
        assert user == KBaseUser("cacheduser", AdminPermission.FULL)

    @pytest.mark.asyncio
    async def test_get_user_with_admin_role(self):
        auth = self._make_auth(full_admin_roles=["KBASE_ADMIN"])
        with patch(
            "src.service.kb_auth._get",
            new_callable=AsyncMock,
            return_value={"user": "admin", "customroles": ["KBASE_ADMIN"]},
        ):
            user = await auth.get_user("admin_token")
        assert user.admin_perm == AdminPermission.FULL

    @pytest.mark.asyncio
    async def test_get_user_missing_required_role(self):
        auth = self._make_auth(required_roles=["REQUIRED_ROLE"])
        with patch(
            "src.service.kb_auth._get",
            new_callable=AsyncMock,
            return_value={"user": "user", "customroles": ["OTHER_ROLE"]},
        ):
            with pytest.raises(MissingRoleError, match="missing a required"):
                await auth.get_user("token")

    @pytest.mark.asyncio
    async def test_get_user_falsy_token_raises(self):
        auth = self._make_auth()
        with pytest.raises(ValueError, match="token is required"):
            await auth.get_user("")


class TestGetAdminRole:
    """Tests for KBaseAuth._get_admin_role method."""

    def test_full_admin(self):
        auth = KBaseAuth(
            "http://auth/",
            required_roles=None,
            full_admin_roles=["ADMIN"],
            cache_max_size=100,
            cache_expiration=60,
            service_name="Authentication Service",
        )
        assert auth._get_admin_role({"ADMIN", "OTHER"}) == AdminPermission.FULL

    def test_no_admin(self):
        auth = KBaseAuth(
            "http://auth/",
            required_roles=None,
            full_admin_roles=["ADMIN"],
            cache_max_size=100,
            cache_expiration=60,
            service_name="Authentication Service",
        )
        assert auth._get_admin_role({"OTHER"}) == AdminPermission.NONE
