"""Tests for the exception handlers module."""

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

from src.service.exception_handlers import _format_error, universal_error_handler
from src.service.exceptions import (
    ClusterDeletionError,
    ConfigurationLimitExceededError,
    InvalidTokenError,
    MissingTokenError,
    SparkManagerError,
)


class TestFormatError:
    """Tests for the _format_error helper."""

    def test_format_error_with_all_params(self):
        result = _format_error(400, 10050, "Configuration limit exceeded", "Too many workers")
        assert result.status_code == 400
        body = result.body
        assert b"Too many workers" in body
        assert b"10050" in body

    def test_format_error_with_none_message(self):
        result = _format_error(500, None, "some_type", None)
        assert result.status_code == 500
        body = result.body
        assert b"some_type" in body

    def test_format_error_with_no_type_or_message(self):
        result = _format_error(500, None, None, None)
        body = result.body
        assert b"Unknown error" in body


class TestUniversalErrorHandler:
    """Tests for the universal_error_handler."""

    @pytest.fixture
    def mock_request(self):
        return MagicMock()

    @pytest.mark.asyncio
    async def test_spark_manager_error(self, mock_request):
        exc = MissingTokenError("No token provided")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 401
        assert b"No token provided" in result.body

    @pytest.mark.asyncio
    async def test_invalid_token_error(self, mock_request):
        exc = InvalidTokenError("Token expired")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 401
        assert b"Token expired" in result.body

    @pytest.mark.asyncio
    async def test_configuration_limit_exceeded_error(self, mock_request):
        exc = ConfigurationLimitExceededError("Too many cores")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 400

    @pytest.mark.asyncio
    async def test_cluster_deletion_error(self, mock_request):
        exc = ClusterDeletionError("Failed to delete")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 400

    @pytest.mark.asyncio
    async def test_base_spark_manager_error(self, mock_request):
        exc = SparkManagerError("Generic error")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 500

    @pytest.mark.asyncio
    async def test_request_validation_error(self, mock_request):
        exc = RequestValidationError(
            errors=[{"loc": ("body", "name"), "msg": "field required", "type": "value_error.missing"}]
        )
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 400
        assert b"request_validation_failed" in result.body.lower() or b"30010" in result.body

    @pytest.mark.asyncio
    async def test_http_exception(self, mock_request):
        exc = HTTPException(status_code=404, detail="Resource not found")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 404
        assert b"Resource not found" in result.body

    @pytest.mark.asyncio
    async def test_http_exception_403(self, mock_request):
        exc = HTTPException(status_code=403, detail="Forbidden")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 403

    @pytest.mark.asyncio
    async def test_generic_exception(self, mock_request):
        exc = RuntimeError("Something broke")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 500
        assert b"unexpected error" in result.body.lower()

    @pytest.mark.asyncio
    async def test_spark_manager_error_empty_message(self, mock_request):
        exc = MissingTokenError("")
        result = await universal_error_handler(mock_request, exc)
        assert result.status_code == 401
