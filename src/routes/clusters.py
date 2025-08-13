"""
Routes for Spark cluster management.
"""

import logging
import math

from fastapi import APIRouter, Depends, status

from src.service import kb_auth
from src.service.dependencies import auth
from src.service.exceptions import ConfigurationLimitExceededError
from src.service.kb_auth import AdminPermission
from src.service.models import (
    DEFAULT_MASTER_MEMORY,
    DEFAULT_WORKER_MEMORY,
    ClusterDeleteResponse,
    SparkClusterConfig,
    SparkClusterCreateResponse,
    SparkClusterStatus,
)
from src.spark_manager import KubeSparkManager

logger = logging.getLogger(__name__)

# Create a router for cluster endpoints
router = APIRouter(prefix="/clusters", tags=["clusters"])


@router.post(
    "",
    response_model=SparkClusterCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new Spark cluster",
    description="Creates a new Spark cluster for the authenticated user with the specified configuration.",
)
async def create_cluster(
    config: SparkClusterConfig, user: kb_auth.KBaseUser = Depends(auth)
) -> SparkClusterCreateResponse:
    """Create a new Spark cluster for the authenticated user."""

    manager = KubeSparkManager(
        username=str(user.user),
    )

    # Validate config against defaults for non-admin users
    if user.admin_perm != AdminPermission.FULL:
        default_config = SparkClusterConfig(
            worker_memory=DEFAULT_WORKER_MEMORY,  # type: ignore[assignment]
            master_memory=DEFAULT_MASTER_MEMORY,  # type: ignore[assignment]
        )

        exceeds_limits = (
            config.worker_count > default_config.worker_count
            or config.worker_cores > default_config.worker_cores
            or config.worker_memory > default_config.worker_memory
            or config.master_cores > default_config.master_cores
            or config.master_memory > default_config.master_memory
        )

        if exceeds_limits:
            raise ConfigurationLimitExceededError(
                "Configuration exceeds default limits for non-admin users. "
                f"Max Workers: {default_config.worker_count}, "
                f"Max Worker Cores: {default_config.worker_cores}, "
                f"Max Worker Memory: {default_config.worker_memory.human_readable()}, "
                f"Max Master Cores: {default_config.master_cores}, "
                f"Max Master Memory: {default_config.master_memory.human_readable()}."
            )

    # Convert ByteSize memory configuration into the specific string format required by Kubernetes
    # Kubernetes requires memory values to match the pattern: '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'
    # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
    # Using 'G' suffix for now.
    # TODO: Make ByteSize.human_readable() output compatible with Kubernetes.
    worker_mem_str = f"{math.ceil(config.worker_memory.to('GB'))}G"
    master_mem_str = f"{math.ceil(config.master_memory.to('GB'))}G"

    # Create the cluster
    result = manager.create_cluster(
        worker_count=config.worker_count,
        worker_cores=config.worker_cores,
        worker_memory=worker_mem_str,
        master_cores=config.master_cores,
        master_memory=master_mem_str,
    )

    return result


@router.get(
    "",
    response_model=SparkClusterStatus,
    summary="Get cluster status",
    description="Retrieves the status of the Spark cluster for the authenticated user. Important: Even with a successful API call (HTTP 200), check the 'error' field in the response to determine if there are issues with the cluster deployments.",
)
async def get_cluster_status(
    user: kb_auth.KBaseUser = Depends(auth),
) -> SparkClusterStatus:
    """Get the status of the Spark cluster belonging to the authenticated user.

    Note: A successful API call (HTTP 200) does not necessarily mean the cluster is healthy.
    Always check the 'error' field in the response to determine if there are issues with
    the cluster deployments.
    """

    manager = KubeSparkManager(
        username=str(user.user),
    )

    status = manager.get_cluster_status()

    return status


@router.delete(
    "",
    response_model=ClusterDeleteResponse,
    summary="Delete a Spark cluster",
    description="Deletes a specific Spark cluster for the authenticated user.",
)
async def delete_cluster(
    user: kb_auth.KBaseUser = Depends(auth),
) -> ClusterDeleteResponse:
    """Delete a specific Spark cluster for the authenticated user."""

    return KubeSparkManager(username=user.user).delete_cluster()
