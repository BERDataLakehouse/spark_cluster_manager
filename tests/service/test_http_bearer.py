"""Tests for the HTTP Bearer authentication module."""

from unittest.mock import MagicMock

import pytest

from src.service.exceptions import MissingTokenError
from src.service.http_bearer import KBaseHTTPBearer
from src.service.kb_auth import AdminPermission, KBaseUser


class TestKBaseHTTPBearer:
    """Tests for KBaseHTTPBearer dependency."""

    def test_init_defaults(self):
        bearer = KBaseHTTPBearer()
        assert bearer.optional is False
        assert bearer.scheme_name == "KBaseHTTPBearer"

    def test_init_optional(self):
        bearer = KBaseHTTPBearer(optional=True)
        assert bearer.optional is True

    def test_init_custom_scheme_name(self):
        bearer = KBaseHTTPBearer(scheme_name="CustomAuth")
        assert bearer.scheme_name == "CustomAuth"

    @pytest.mark.asyncio
    async def test_call_returns_user(self):
        bearer = KBaseHTTPBearer()
        mock_request = MagicMock()
        user = KBaseUser("testuser", AdminPermission.NONE)
        mock_request.state._request_state = MagicMock()
        mock_request.state._request_state.user = user

        with pytest.MonkeyPatch.context() as m:
            m.setattr(
                "src.service.http_bearer.app_state.get_request_user",
                lambda r: user,
            )
            result = await bearer(mock_request)
        assert result == user

    @pytest.mark.asyncio
    async def test_call_missing_user_not_optional_raises(self):
        bearer = KBaseHTTPBearer()
        mock_request = MagicMock()

        with pytest.MonkeyPatch.context() as m:
            m.setattr(
                "src.service.http_bearer.app_state.get_request_user",
                lambda r: None,
            )
            with pytest.raises(
                MissingTokenError, match="Authorization header required"
            ):
                await bearer(mock_request)

    @pytest.mark.asyncio
    async def test_call_missing_user_optional_returns_none(self):
        bearer = KBaseHTTPBearer(optional=True)
        mock_request = MagicMock()

        with pytest.MonkeyPatch.context() as m:
            m.setattr(
                "src.service.http_bearer.app_state.get_request_user",
                lambda r: None,
            )
            result = await bearer(mock_request)
        assert result is None
