"""Tests for the clusters routes module."""

from unittest.mock import MagicMock, patch

import pytest

from src.routes.clusters import create_cluster, delete_cluster, get_cluster_status
from src.service.exceptions import ConfigurationLimitExceededError
from src.service.kb_auth import AdminPermission, KBaseUser
from src.service.models import (
    DEFAULT_MASTER_MEMORY,
    DEFAULT_WORKER_MEMORY,
    ClusterDeleteResponse,
    SparkClusterConfig,
    SparkClusterCreateResponse,
    SparkClusterStatus,
)


class TestCreateCluster:
    """Tests for the create_cluster endpoint."""

    @pytest.mark.asyncio
    async def test_create_cluster_admin(self):
        admin_user = KBaseUser("admin", AdminPermission.FULL)
        config = SparkClusterConfig(
            worker_memory=DEFAULT_WORKER_MEMORY,
            master_memory=DEFAULT_MASTER_MEMORY,
        )
        mock_result = MagicMock(spec=SparkClusterCreateResponse)

        with patch("src.routes.clusters.KubeSparkManager") as mock_manager_cls:
            mock_manager = MagicMock()
            mock_manager.create_cluster.return_value = mock_result
            mock_manager_cls.return_value = mock_manager

            result = await create_cluster(config, admin_user)

        assert result == mock_result
        mock_manager_cls.assert_called_once_with(username="admin")

    @pytest.mark.asyncio
    async def test_create_cluster_non_admin_within_limits(self):
        user = KBaseUser("regular", AdminPermission.NONE)
        config = SparkClusterConfig(
            worker_memory=DEFAULT_WORKER_MEMORY,
            master_memory=DEFAULT_MASTER_MEMORY,
        )
        mock_result = MagicMock(spec=SparkClusterCreateResponse)

        with patch("src.routes.clusters.KubeSparkManager") as mock_manager_cls:
            mock_manager = MagicMock()
            mock_manager.create_cluster.return_value = mock_result
            mock_manager_cls.return_value = mock_manager

            result = await create_cluster(config, user)

        assert result == mock_result

    @pytest.mark.asyncio
    async def test_create_cluster_non_admin_exceeds_worker_count(self):
        user = KBaseUser("regular", AdminPermission.NONE)
        default = SparkClusterConfig(
            worker_memory=DEFAULT_WORKER_MEMORY,
            master_memory=DEFAULT_MASTER_MEMORY,
        )
        config = SparkClusterConfig(
            worker_count=default.worker_count + 1,
            worker_memory=DEFAULT_WORKER_MEMORY,
            master_memory=DEFAULT_MASTER_MEMORY,
        )

        with patch("src.routes.clusters.KubeSparkManager"):
            with pytest.raises(
                ConfigurationLimitExceededError, match="exceeds default limits"
            ):
                await create_cluster(config, user)

    @pytest.mark.asyncio
    async def test_create_cluster_non_admin_exceeds_worker_cores(self):
        user = KBaseUser("regular", AdminPermission.NONE)
        default = SparkClusterConfig(
            worker_memory=DEFAULT_WORKER_MEMORY,
            master_memory=DEFAULT_MASTER_MEMORY,
        )
        config = SparkClusterConfig(
            worker_cores=default.worker_cores + 1,
            worker_memory=DEFAULT_WORKER_MEMORY,
            master_memory=DEFAULT_MASTER_MEMORY,
        )

        with patch("src.routes.clusters.KubeSparkManager"):
            with pytest.raises(ConfigurationLimitExceededError):
                await create_cluster(config, user)

    @pytest.mark.asyncio
    async def test_create_cluster_admin_bypasses_limits(self):
        admin = KBaseUser("admin", AdminPermission.FULL)
        config = SparkClusterConfig(
            worker_count=25,
            worker_cores=64,
            worker_memory=DEFAULT_WORKER_MEMORY,
            master_memory=DEFAULT_MASTER_MEMORY,
        )
        mock_result = MagicMock(spec=SparkClusterCreateResponse)

        with patch("src.routes.clusters.KubeSparkManager") as mock_manager_cls:
            mock_manager = MagicMock()
            mock_manager.create_cluster.return_value = mock_result
            mock_manager_cls.return_value = mock_manager

            result = await create_cluster(config, admin)

        assert result == mock_result


class TestGetClusterStatus:
    """Tests for the get_cluster_status endpoint."""

    @pytest.mark.asyncio
    async def test_get_cluster_status(self):
        user = KBaseUser("testuser", AdminPermission.NONE)
        mock_status = MagicMock(spec=SparkClusterStatus)

        with patch("src.routes.clusters.KubeSparkManager") as mock_manager_cls:
            mock_manager = MagicMock()
            mock_manager.get_cluster_status.return_value = mock_status
            mock_manager_cls.return_value = mock_manager

            result = await get_cluster_status(user)

        assert result == mock_status
        mock_manager_cls.assert_called_once_with(username="testuser")


class TestDeleteCluster:
    """Tests for the delete_cluster endpoint."""

    @pytest.mark.asyncio
    async def test_delete_cluster(self):
        user = KBaseUser("testuser", AdminPermission.NONE)
        mock_response = MagicMock(spec=ClusterDeleteResponse)

        with patch("src.routes.clusters.KubeSparkManager") as mock_manager_cls:
            mock_manager = MagicMock()
            mock_manager.delete_cluster.return_value = mock_response
            mock_manager_cls.return_value = mock_manager

            result = await delete_cluster(user)

        assert result == mock_response
        mock_manager_cls.assert_called_once_with(username="testuser")
