import os
from unittest.mock import MagicMock, Mock
import pytest
from fastapi.testclient import TestClient
from kubernetes.client.rest import ApiException
from src.main import create_application


@pytest.fixture
def client():
    app = create_application()
    return TestClient(app)


@pytest.fixture
def sample_env_vars():
    """Complete set of environment variables for testing."""
    return {
        "KUBE_NAMESPACE": "test-namespace",
        "SPARK_IMAGE": "spark:test-image",
        "BERDL_POSTGRES_USER": "test_user",
        "BERDL_POSTGRES_PASSWORD": "test_password",
        "BERDL_POSTGRES_DB": "test_db",
        "BERDL_POSTGRES_URL": "postgresql://test_user:test_password@localhost:5432/test_db",
        "BERDL_REDIS_HOST": "localhost",
        "BERDL_REDIS_PORT": "6379",
        "BERDL_HIVE_METASTORE_URI": "thrift://localhost:9083",
        "BERDL_DELTALAKE_WAREHOUSE_DIRECTORY_PATH": "s3://test-bucket/warehouse",
        "SPARK_MASTER_PORT": "7077",
        "SPARK_MASTER_WEBUI_PORT": "8080",
        "DEFAULT_SPARK_WORKER_CORES": "2",
        "DEFAULT_SPARK_WORKER_MEMORY": "2GiB",
        "SPARK_WORKER_PORT": "7078",
        "SPARK_WORKER_WEBUI_PORT": "8081",
        "BERDL_TOLERATIONS": "dev",
    }


@pytest.fixture
def test_username():
    """Standard test username with underscores for testing sanitization."""
    return "test_user"


@pytest.fixture
def mock_k8s_apis(mocker):
    """Mock Kubernetes API clients and return both core and apps APIs."""
    # Patch the k8s module at import time
    mock_core_api = MagicMock()
    mock_apps_api = MagicMock()

    mocker.patch("kubernetes.client.CoreV1Api", return_value=mock_core_api)
    mocker.patch("kubernetes.client.AppsV1Api", return_value=mock_apps_api)

    return {
        "core_api": mock_core_api,
        "apps_api": mock_apps_api
    }


@pytest.fixture
def mock_deployment_status():
    """Create a mock deployment status object."""
    def _create_status(replicas=3, ready_replicas=3, available_replicas=3, unavailable_replicas=0):
        status = Mock()
        status.replicas = replicas
        status.ready_replicas = ready_replicas
        status.available_replicas = available_replicas
        status.unavailable_replicas = unavailable_replicas
        return status

    return _create_status


@pytest.fixture
def mock_kbase_auth(mocker):
    """Mock KBase authentication."""
    mock_auth = mocker.patch("src.service.kb_auth.KBaseAuth")
    mock_instance = mock_auth.return_value

    # Mock get_user method
    mock_user = Mock()
    mock_user.username = "test_user"
    mock_user.email = "test@example.com"
    mock_user.display_name = "Test User"
    mock_user.admin_permission = 1  # NONE

    mock_instance.get_user.return_value = mock_user

    return mock_instance


@pytest.fixture
def api_exception_404():
    """Create a 404 ApiException."""
    return ApiException(status=404, reason="Not Found")


@pytest.fixture
def api_exception_409():
    """Create a 409 Conflict ApiException."""
    return ApiException(status=409, reason="Conflict")
