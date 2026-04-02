"""Tests for the error mapping module."""

from fastapi import status

from src.service.error_mapping import ErrorMapping, map_error
from src.service.errors import ErrorType
from src.service.exceptions import (
    AuthenticationError,
    ClusterDeletionError,
    ConfigurationLimitExceededError,
    InvalidAuthHeaderError,
    InvalidTokenError,
    MissingRoleError,
    MissingTokenError,
    SparkManagerError,
)


class TestMapError:
    """Tests for the map_error function."""

    def test_missing_token_error(self):
        result = map_error(MissingTokenError("no token"))
        assert result.err_type == ErrorType.NO_TOKEN
        assert result.http_code == status.HTTP_401_UNAUTHORIZED

    def test_invalid_auth_header_error(self):
        result = map_error(InvalidAuthHeaderError("bad header"))
        assert result.err_type == ErrorType.INVALID_AUTH_HEADER
        assert result.http_code == status.HTTP_401_UNAUTHORIZED

    def test_invalid_token_error(self):
        result = map_error(InvalidTokenError("expired"))
        assert result.err_type == ErrorType.INVALID_TOKEN
        assert result.http_code == status.HTTP_401_UNAUTHORIZED

    def test_missing_role_error(self):
        result = map_error(MissingRoleError("no role"))
        assert result.err_type == ErrorType.MISSING_ROLE
        assert result.http_code == status.HTTP_403_FORBIDDEN

    def test_authentication_error(self):
        result = map_error(AuthenticationError("auth failed"))
        assert result.err_type == ErrorType.AUTHENTICATION_FAILED
        assert result.http_code == status.HTTP_401_UNAUTHORIZED

    def test_configuration_limit_exceeded_error(self):
        result = map_error(ConfigurationLimitExceededError("too many"))
        assert result.err_type == ErrorType.CONFIGURATION_LIMIT_EXCEEDED
        assert result.http_code == status.HTTP_400_BAD_REQUEST

    def test_cluster_deletion_error(self):
        result = map_error(ClusterDeletionError("failed"))
        assert result.err_type == ErrorType.CLUSTER_DELETION_FAILED
        assert result.http_code == status.HTTP_400_BAD_REQUEST

    def test_base_spark_manager_error(self):
        result = map_error(SparkManagerError("generic"))
        assert result.err_type is None
        assert result.http_code == status.HTTP_500_INTERNAL_SERVER_ERROR

    def test_unmapped_error_subclass(self):
        class CustomSparkError(SparkManagerError):
            pass

        result = map_error(CustomSparkError("custom"))
        assert result.err_type is None
        assert result.http_code == status.HTTP_500_INTERNAL_SERVER_ERROR

    def test_error_mapping_named_tuple(self):
        mapping = ErrorMapping(err_type=ErrorType.NO_TOKEN, http_code=401)
        assert mapping.err_type == ErrorType.NO_TOKEN
        assert mapping.http_code == 401
