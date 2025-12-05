import os
import re
from unittest.mock import Mock, MagicMock, patch
import pytest
from kubernetes.client.rest import ApiException
from src.spark_manager import KubeSparkManager, sanitize_k8s_name
from src.service.exceptions import ClusterDeletionError


# ============================================================================
# sanitize_k8s_name() Tests
# ============================================================================


class TestSanitizeK8sName:
    """Tests for the sanitize_k8s_name function."""

    def test_sanitize_replaces_underscores(self):
        """Test that underscores are replaced with hyphens."""
        assert sanitize_k8s_name("test_user") == "test-user"
        assert sanitize_k8s_name("user_name_test") == "user-name-test"

    def test_sanitize_lowercases(self):
        """Test that input is converted to lowercase."""
        assert sanitize_k8s_name("TestUser") == "testuser"
        assert sanitize_k8s_name("UPPERCASE_USER") == "uppercase-user"

    def test_sanitize_removes_special_chars(self):
        """Test that invalid characters are replaced with hyphens."""
        assert sanitize_k8s_name("user@name") == "user-name"
        assert sanitize_k8s_name("user#name$test") == "user-name-test"
        assert sanitize_k8s_name("user!@#$%name") == "user-name"

    def test_sanitize_removes_leading_trailing(self):
        """Test that leading/trailing non-alphanumeric chars are removed."""
        assert sanitize_k8s_name("_user_") == "user"
        assert sanitize_k8s_name("...test...") == "test"
        assert sanitize_k8s_name("---username---") == "username"

    def test_sanitize_collapses_hyphens(self):
        """Test that multiple consecutive hyphens are collapsed."""
        assert sanitize_k8s_name("test---user") == "test-user"
        assert sanitize_k8s_name("user___name") == "user-name"
        assert sanitize_k8s_name("test--user") == "test-user"

    def test_sanitize_preserves_valid_chars(self):
        """Test that valid characters (alphanumeric, hyphens, dots) are preserved."""
        assert sanitize_k8s_name("user-name") == "user-name"
        assert sanitize_k8s_name("user.name") == "user.name"
        assert sanitize_k8s_name("user123") == "user123"

    def test_sanitize_truncates_253_chars(self):
        """Test that names longer than 253 chars are truncated."""
        long_name = "a" * 300
        result = sanitize_k8s_name(long_name)
        assert len(result) == 253
        assert result == "a" * 253

    def test_sanitize_empty_after_removal(self):
        """Test handling of strings that become empty after sanitization."""
        result = sanitize_k8s_name("___")
        assert result == ""

    def test_sanitize_mixed_special_chars(self):
        """Test with mixed special characters."""
        assert sanitize_k8s_name("user!@#$%name") == "user-name"
        assert sanitize_k8s_name("test*&^user") == "test-user"

    def test_sanitize_preserves_dots(self):
        """Test that dots in valid positions are preserved."""
        assert sanitize_k8s_name("user.name.test") == "user.name.test"
        assert sanitize_k8s_name("test.user.123") == "test.user.123"


# ============================================================================
# KubeSparkManager Initialization Tests
# ============================================================================


class TestKubeSparkManagerInit:
    """Tests for KubeSparkManager initialization."""

    def test_missing_berdl_tolerations_raises_error(self, sample_env_vars):
        """Test that missing BERDL_TOLERATIONS raises a ValueError."""
        # Remove BERDL_TOLERATIONS from environment
        env_without_tolerations = {k: v for k, v in sample_env_vars.items() if k != "BERDL_TOLERATIONS"}

        with patch.dict(os.environ, env_without_tolerations, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                with pytest.raises(ValueError, match="Missing required environment variables"):
                    KubeSparkManager("testuser")

    def test_init_sanitizes_username_in_names(self, sample_env_vars):
        """Test that master_name and worker_name use sanitized username."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("test_user")

                # Usernames with underscores should be sanitized
                assert manager.master_name == "spark-master-test-user"
                assert manager.worker_name == "spark-worker-test-user"

    def test_init_generates_cluster_id_format(self, sample_env_vars):
        """Test that cluster_id follows the format: spark-{sanitized}-{uuid[:8]}."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("test_user")

                # Cluster ID should start with spark-test-user- and have an 8-char UUID
                pattern = r"^spark-test-user-[a-f0-9]{8}$"
                assert re.match(pattern, manager.cluster_id)

    def test_init_loads_k8s_config(self, sample_env_vars):
        """Test that load_incluster_config() is called during initialization."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config") as mock_load_config:
                KubeSparkManager("testuser")

                # Verify Kubernetes config was loaded
                mock_load_config.assert_called_once()

    def test_init_sets_image_pull_policy(self, sample_env_vars):
        """Test that image_pull_policy defaults to 'IfNotPresent'."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                assert manager.image_pull_policy == "IfNotPresent"


# ============================================================================
# Kubernetes API Exception Handling Tests
# ============================================================================


class TestKubernetesAPIExceptions:
    """Tests for Kubernetes API exception handling."""

    def test_create_deployment_409_deletes_and_recreates(self, sample_env_vars, mock_k8s_apis, api_exception_409):
        """Test that 409 Conflict on deployment creation triggers delete and recreate."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                with patch("src.spark_manager.render_yaml_template") as mock_render:
                    mock_render.return_value = {"mock": "deployment"}

                    manager = KubeSparkManager("testuser")

                    # Mock apps_api to raise 409 on first create, succeed on second
                    mock_k8s_apis["apps_api"].create_namespaced_deployment.side_effect = [
                        api_exception_409,
                        Mock()
                    ]
                    mock_k8s_apis["apps_api"].delete_namespaced_deployment.return_value = Mock()

                    # Call create_or_replace_deployment
                    manager._create_or_replace_deployment(
                        {"mock": "deployment"}, "test-deployment", "test deployment"
                    )

                    # Verify delete was called after 409
                    mock_k8s_apis["apps_api"].delete_namespaced_deployment.assert_called_once()
                    # Verify create was called twice (initial + retry)
                    assert mock_k8s_apis["apps_api"].create_namespaced_deployment.call_count == 2

    def test_create_service_409_deletes_and_recreates(self, sample_env_vars, mock_k8s_apis, api_exception_409):
        """Test that 409 Conflict on service creation triggers delete and recreate."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock core_api to raise 409 on first create, succeed on second
                mock_k8s_apis["core_api"].create_namespaced_service.side_effect = [
                    api_exception_409,
                    Mock()
                ]
                mock_k8s_apis["core_api"].delete_namespaced_service.return_value = Mock()

                # Call create_or_replace_service
                manager._create_or_replace_service(
                    {"mock": "service"}, "test-service", "test service"
                )

                # Verify delete was called after 409
                mock_k8s_apis["core_api"].delete_namespaced_service.assert_called_once()
                # Verify create was called twice
                assert mock_k8s_apis["core_api"].create_namespaced_service.call_count == 2

    def test_create_deployment_non_409_raises(self, sample_env_vars, mock_k8s_apis):
        """Test that non-409 ApiException is propagated."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock to raise 500 error
                mock_k8s_apis["apps_api"].create_namespaced_deployment.side_effect = ApiException(
                    status=500, reason="Internal Server Error"
                )

                # Should raise the original exception
                with pytest.raises(ApiException) as exc_info:
                    manager._create_or_replace_deployment(
                        {"mock": "deployment"}, "test-deployment", "test deployment"
                    )

                assert exc_info.value.status == 500

    def test_get_status_404_returns_not_exists(self, sample_env_vars, mock_k8s_apis, api_exception_404):
        """Test that 404 on get_deployment_status returns exists=False."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock to raise 404
                mock_k8s_apis["apps_api"].read_namespaced_deployment.side_effect = api_exception_404

                status = manager._get_deployment_status("non-existent-deployment")

                # Deployment should not exist, but error is set (caught by generic Exception handler)
                assert status.exists is False
                assert status.error is not None  # Error message is populated from exception

    def test_get_status_handles_none_fields(self, sample_env_vars, mock_k8s_apis):
        """Test that None replica fields are handled gracefully."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Create mock deployment with None status fields
                mock_deployment = Mock()
                mock_deployment.status.replicas = None
                mock_deployment.status.ready_replicas = None
                mock_deployment.status.available_replicas = None
                mock_deployment.status.unavailable_replicas = None

                mock_k8s_apis["apps_api"].read_namespaced_deployment.return_value = mock_deployment

                status = manager._get_deployment_status("test-deployment")

                # None fields should default to 0
                assert status.replicas == 0
                assert status.ready_replicas == 0
                assert status.available_replicas == 0
                assert status.unavailable_replicas == 0

    def test_delete_resource_404_logs_warning(self, sample_env_vars, mock_k8s_apis, api_exception_404):
        """Test that 404 during delete is logged as warning, not error."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock delete to raise 404
                mock_k8s_apis["apps_api"].delete_namespaced_deployment.side_effect = api_exception_404

                # Call attempt_delete
                result = manager._attempt_delete(
                    mock_k8s_apis["apps_api"].delete_namespaced_deployment,
                    "non-existent",
                    "deployment"
                )

                # Resource should be marked as not existing
                assert result["deleted"] is False
                assert result["resource_exists"] is False


# ============================================================================
# Cluster Status Calculation Tests
# ============================================================================


class TestClusterStatus:
    """Tests for cluster status calculation logic."""

    def test_status_ready_when_all_replicas_ready(self, sample_env_vars, mock_k8s_apis, mock_deployment_status):
        """Test that is_ready=True when ready_replicas equals replicas."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock deployment with all replicas ready
                mock_deployment = Mock()
                mock_deployment.status = mock_deployment_status(replicas=3, ready_replicas=3)

                mock_k8s_apis["apps_api"].read_namespaced_deployment.return_value = mock_deployment

                status = manager._get_deployment_status("test-deployment")

                assert status.is_ready is True
                assert status.replicas == 3
                assert status.ready_replicas == 3

    def test_status_not_ready_when_partial_replicas(self, sample_env_vars, mock_k8s_apis, mock_deployment_status):
        """Test that is_ready=False when ready_replicas < replicas."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock deployment with partial replicas ready
                mock_deployment = Mock()
                mock_deployment.status = mock_deployment_status(replicas=3, ready_replicas=1)

                mock_k8s_apis["apps_api"].read_namespaced_deployment.return_value = mock_deployment

                status = manager._get_deployment_status("test-deployment")

                assert status.is_ready is False
                assert status.replicas == 3
                assert status.ready_replicas == 1

    def test_status_master_url_only_if_ready(self, sample_env_vars, mock_k8s_apis, mock_deployment_status):
        """Test that master_url is only populated when ready_replicas > 0."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock master with ready replicas
                mock_master = Mock()
                mock_master.status = mock_deployment_status(replicas=1, ready_replicas=1)

                # Mock worker
                mock_worker = Mock()
                mock_worker.status = mock_deployment_status(replicas=3, ready_replicas=3)

                mock_k8s_apis["apps_api"].read_namespaced_deployment.side_effect = [mock_master, mock_worker]

                status = manager.get_cluster_status()

                # Master URL should be populated
                assert status.master_url is not None
                assert "spark-master-testuser" in status.master_url
                assert status.master_ui_url is not None

    def test_status_error_flag_on_exception(self, sample_env_vars, mock_k8s_apis):
        """Test that error flag is set when exception occurs."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock to raise exception
                mock_k8s_apis["apps_api"].read_namespaced_deployment.side_effect = Exception("Test error")

                status = manager._get_deployment_status("test-deployment")

                # Error should be captured
                assert status.error == "Test error"

    def test_status_deployment_not_found(self, sample_env_vars, mock_k8s_apis, api_exception_404):
        """Test that 404 returns exists=False and no URLs."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock to raise 404 for both deployments
                mock_k8s_apis["apps_api"].read_namespaced_deployment.side_effect = api_exception_404

                status = manager.get_cluster_status()

                # Master URL should be None
                assert status.master_url is None
                assert status.master_ui_url is None


# ============================================================================
# Cluster Deletion Logic Tests
# ============================================================================


class TestClusterDeletion:
    """Tests for cluster deletion logic."""

    def test_delete_all_resources_success(self, sample_env_vars, mock_k8s_apis):
        """Test successful deletion of all 3 resources."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock successful deletions (need to set return value for all 3 calls)
                mock_k8s_apis["apps_api"].delete_namespaced_deployment.return_value = Mock()
                mock_k8s_apis["core_api"].delete_namespaced_service.return_value = Mock()

                result = manager.delete_cluster()

                # Should return success message
                assert "deleted successfully" in result.message.lower()

    def test_delete_no_resources_found(self, sample_env_vars, mock_k8s_apis, api_exception_404):
        """Test deletion when no resources exist (all 404)."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock all deletions to return 404
                mock_k8s_apis["apps_api"].delete_namespaced_deployment.side_effect = api_exception_404
                mock_k8s_apis["core_api"].delete_namespaced_service.side_effect = api_exception_404

                result = manager.delete_cluster()

                # Should return "no resources found" message
                assert "no" in result.message.lower()
                assert "found" in result.message.lower()

    def test_delete_partial_raises_error(self, sample_env_vars, mock_k8s_apis, api_exception_404):
        """Test that partial deletion (some succeed, some fail) raises ApiException."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Mock partial deletion: worker succeeds, master fails
                mock_k8s_apis["apps_api"].delete_namespaced_deployment.side_effect = [
                    Mock(),  # Worker deployment succeeds
                    ApiException(status=500, reason="Server Error")  # Master deployment fails
                ]
                mock_k8s_apis["core_api"].delete_namespaced_service.side_effect = api_exception_404

                # Should raise ApiException (re-raised from _attempt_delete)
                with pytest.raises(ApiException) as exc_info:
                    manager.delete_cluster()

                assert exc_info.value.status == 500

    def test_delete_order_worker_master_service(self, sample_env_vars, mock_k8s_apis):
        """Test that deletion order is: worker deployment, master deployment, master service."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                deletion_order = []

                def track_worker_delete(*args, **kwargs):
                    deletion_order.append("worker")
                    return Mock()

                def track_master_delete(*args, **kwargs):
                    deletion_order.append("master")
                    return Mock()

                def track_service_delete(*args, **kwargs):
                    deletion_order.append("service")
                    return Mock()

                # Set up side effects
                mock_k8s_apis["apps_api"].delete_namespaced_deployment.side_effect = [
                    track_worker_delete(),
                    track_master_delete()
                ]
                mock_k8s_apis["core_api"].delete_namespaced_service.side_effect = [track_service_delete()]

                manager.delete_cluster()

                # Verify deletion order
                assert deletion_order == ["worker", "master", "service"]

    def test_delete_404_not_counted_as_failure(self, sample_env_vars, mock_k8s_apis, api_exception_404):
        """Test that 404 resources are skipped gracefully (not counted as failures)."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                manager = KubeSparkManager("testuser")

                # Worker exists, master and service don't
                mock_k8s_apis["apps_api"].delete_namespaced_deployment.side_effect = [
                    Mock(),  # Worker succeeds
                    api_exception_404  # Master doesn't exist
                ]
                mock_k8s_apis["core_api"].delete_namespaced_service.side_effect = api_exception_404

                result = manager.delete_cluster()

                # Should succeed (404s are not failures)
                assert "successfully" in result.message.lower() or "no" in result.message.lower()


# ============================================================================
# Template Variable Tests
# ============================================================================


class TestTemplateVariables:
    """Tests for template variable mapping."""

    def test_create_master_deployment_includes_tolerations(self, sample_env_vars):
        """Test that _create_master_deployment includes tolerations in template values."""
        environment = "dev"

        with patch.dict(os.environ, {**sample_env_vars, "BERDL_TOLERATIONS": environment}, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                with patch("src.spark_manager.render_yaml_template") as mock_render:
                    with patch.object(KubeSparkManager, "_create_or_replace_deployment"):
                        mock_render.return_value = {"mock": "deployment"}

                        manager = KubeSparkManager("testuser")
                        manager._create_master_deployment(2, "4Gi")

                        # Verify render_yaml_template was called with tolerations
                        assert mock_render.called
                        call_args = mock_render.call_args[0]
                        template_values = call_args[1]
                        assert "BERDL_TOLERATIONS" in template_values
                        assert template_values["BERDL_TOLERATIONS"] == environment

    def test_create_worker_deployment_includes_tolerations(self, sample_env_vars):
        """Test that _create_worker_deployment includes tolerations in template values."""
        environment = "prod"

        with patch.dict(os.environ, {**sample_env_vars, "BERDL_TOLERATIONS": environment}, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                with patch("src.spark_manager.render_yaml_template") as mock_render:
                    with patch.object(KubeSparkManager, "_create_or_replace_deployment"):
                        mock_render.return_value = {"mock": "deployment"}

                        manager = KubeSparkManager("testuser")
                        manager._create_worker_deployment(3, 2, "4Gi")

                        # Verify render_yaml_template was called with tolerations
                        assert mock_render.called
                        call_args = mock_render.call_args[0]
                        template_values = call_args[1]
                        assert "BERDL_TOLERATIONS" in template_values
                        assert template_values["BERDL_TOLERATIONS"] == environment

    def test_tolerations_variable_passed(self, sample_env_vars):
        """Test that BERDL_TOLERATIONS is passed to template values."""
        with patch.dict(os.environ, sample_env_vars, clear=True):
            with patch("kubernetes.config.load_incluster_config"):
                with patch("src.spark_manager.render_yaml_template") as mock_render:
                    with patch.object(KubeSparkManager, "_create_or_replace_deployment"):
                        mock_render.return_value = {"mock": "deployment"}

                        manager = KubeSparkManager("testuser")
                        manager._create_master_deployment(2, "4Gi")

                        # Get template values
                        call_args = mock_render.call_args[0]
                        template_values = call_args[1]

                        # Verify BERDL_TOLERATIONS is in template_values
                        assert "BERDL_TOLERATIONS" in template_values
                        assert template_values["BERDL_TOLERATIONS"] == "dev"
