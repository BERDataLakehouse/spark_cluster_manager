import logging
import os
import pathlib
import uuid
from typing import Any, Callable, Dict

from kubernetes.client.rest import ApiException

import kubernetes as k8s
from src.service.exceptions import ClusterDeletionError
from src.service.models import (
    ClusterDeleteResponse,
    DeploymentStatus,
    SparkClusterCreateResponse,
    SparkClusterStatus,
)
from src.template_utils import render_yaml_template

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the path to the templates directory
TEMPLATES_DIR = pathlib.Path(__file__).parent / "templates"

#TODO: We should use a pydantic settings model for env vars
# One model for SPARK_MASTER (prefix) env vars
# Another model for SPARK_WORKER (prefix) env vars
# One model for the SPARK MANAGER env vars or BERDL specific vars?

class KubeSparkManager:
    """
    Manager for user-specific Spark clusters in Kubernetes.

    This class provides methods to create, manage, and destroy Spark clusters
    for individual JupyterHub users.
    """

    # Required environment variables
    REQUIRED_ENV_VARS = {
        "KUBE_NAMESPACE": "Kubernetes namespace for Spark clusters",
        "SPARK_IMAGE": "Docker image for Spark master and workers",
        "BERDL_POSTGRES_USER": "PostgreSQL username",
        "BERDL_POSTGRES_PASSWORD": "PostgreSQL password",
        "BERDL_POSTGRES_DB": "PostgreSQL database name",
        "BERDL_POSTGRES_URL": "PostgreSQL connection URL",
        "BERDL_REDIS_HOST": "Redis host",
        "BERDL_REDIS_PORT": "Redis port",
        "BERDL_HIVE_METASTORE_URI": "Hive metastore Thrift URI",
        "BERDL_DELTALAKE_WAREHOUSE_DIRECTORY_PATH": "Delta Lake warehouse directory (S3 bucket)",
        "SPARK_MASTER_PORT" : "Port for Spark master (used in Spark configs)",
        "SPARK_MASTER_WEBUI_PORT" : "Web UI port for Spark master (used in Spark configs)",
        "DEFAULT_SPARK_WORKER_CORES": "Number of CPU cores for each Spark worker",
        "DEFAULT_SPARK_WORKER_MEMORY": "Memory allocation for each Spark worker",
        "SPARK_WORKER_PORT": "Port for Spark workers local daemon",
        "SPARK_WORKER_WEBUI_PORT": "Web UI port for Spark workers",

    }

    # SPARK_MASTER_HOST SPARK_MASTER_URL and SPARK_MODE are needed, but not taken from the env, they are in the templates

    # Template files
    MASTER_DEPLOYMENT_TEMPLATE_FILE = os.environ.get(
        "MASTER_DEPLOYMENT_TEMPLATE_FILE", "spark_master_deployment.yaml"
    )
    WORKER_DEPLOYMENT_TEMPLATE_FILE = os.environ.get(
        "WORKER_DEPLOYMENT_TEMPLATE_FILE", "spark_worker_deployment.yaml"
    )
    MASTER_SERVICE_TEMPLATE_FILE = os.environ.get(
        "MASTER_SERVICE_TEMPLATE_FILE", "spark_master_service.yaml"
    )

    # Full paths to template files
    MASTER_DEPLOYMENT_TEMPLATE = str(TEMPLATES_DIR / MASTER_DEPLOYMENT_TEMPLATE_FILE)
    WORKER_DEPLOYMENT_TEMPLATE = str(TEMPLATES_DIR / WORKER_DEPLOYMENT_TEMPLATE_FILE)
    MASTER_SERVICE_TEMPLATE = str(TEMPLATES_DIR / MASTER_SERVICE_TEMPLATE_FILE)

    # Default configuration values for cluster settings
    # TODO MAKE THESE REQUIRED ENV VARS: FIX THIS
    DEFAULT_WORKER_COUNT = int(os.environ.get("DEFAULT_WORKER_COUNT", "4"))
    DEFAULT_WORKER_CORES = int(os.environ["DEFAULT_SPARK_WORKER_CORES"])
    DEFAULT_WORKER_MEMORY = os.environ["DEFAULT_SPARK_WORKER_MEMORY"]
    DEFAULT_MASTER_CORES = int(os.environ.get("DEFAULT_MASTER_CORES", "1"))
    DEFAULT_MASTER_MEMORY = os.environ.get("DEFAULT_MASTER_MEMORY", "50GiB")

    DEFAULT_IMAGE_PULL_POLICY = os.environ.get(
        "SPARK_IMAGE_PULL_POLICY", "IfNotPresent"
    )

    DEFAULT_EXECUTOR_CORES = 2
    DEFAULT_MAX_CORES_PER_APPLICATION = 10
    DEFAULT_MAX_EXECUTORS = 5

    DEFAULT_MASTER_PORT = 7077
    DEFAULT_MASTER_WEBUI_PORT = 8090
    DEFAULT_WORKER_WEBUI_PORT = 8081

    @classmethod
    def validate_environment(cls) -> Dict[str, str]:
        """
        Validate that all required environment variables are set.

        Returns:
            Dict[str, str]: Dictionary of validated environment variables
        """
        missing_vars = []
        env_values = {}

        for var, description in cls.REQUIRED_ENV_VARS.items():
            value = os.environ.get(var)
            if not value or not value.strip():
                missing_vars.append(f"{var} ({description})")
            env_values[var] = value

        if missing_vars:
            raise ValueError(
                "Missing required environment variables:\n"
                + "\n".join(f"- {var}" for var in missing_vars)
            )

        return env_values

    def __init__(self, username: str):
        """
        Initialize the KubeSparkManager with user-specific configuration.
        This should only be run inside a kubernetes container.

        Args:
            username: Username of the JupyterHub user

        Raises:
            ValueError: If required environment variables are not set
        """
        # Validate environment variables
        env_vars = self.validate_environment()

        self.username = username
        self.namespace = env_vars["KUBE_NAMESPACE"]
        self.image = env_vars["SPARK_IMAGE"]
        self.image_pull_policy = self.DEFAULT_IMAGE_PULL_POLICY

        # Generate a unique identifier for this user's Spark cluster
        self.cluster_id = f"spark-{username.lower()}-{str(uuid.uuid4())[:8]}"

        # Service names
        self.master_name = f"spark-master-{username.lower()}"
        self.worker_name = f"spark-worker-{username.lower()}"

        # Initialize Kubernetes client
        k8s.config.load_incluster_config()
        self.core_api = k8s.client.CoreV1Api()
        self.apps_api = k8s.client.AppsV1Api()

        logger.info(
            f"Initialized KubeSparkManager for user {username} in namespace {self.namespace}"
        )

    def create_cluster(
        self,
        worker_count: int = DEFAULT_WORKER_COUNT,
        worker_cores: int = DEFAULT_WORKER_CORES,
        worker_memory: str = DEFAULT_WORKER_MEMORY,
        master_cores: int = DEFAULT_MASTER_CORES,
        master_memory: str = DEFAULT_MASTER_MEMORY,
    ) -> SparkClusterCreateResponse:
        """
        Create a new Spark cluster for the user.

        Args:
            worker_count: Number of Spark worker replicas
            worker_cores: Number of CPU cores for each worker
            worker_memory: Memory allocation for each worker
            master_cores: Number of CPU cores for the master
            master_memory: Memory allocation for the master

        Returns:
            The Spark master URL for connecting to the cluster
        """
        # Create the Spark master deployment and service
        self._create_master_deployment(master_cores, master_memory)
        self._create_master_service()

        # Create the Spark worker deployment
        self._create_worker_deployment(worker_count, worker_cores, worker_memory)

        # Return cluster information
        master_url = (
            f"spark://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_PORT}"
        )
        master_ui_url = f"http://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_WEBUI_PORT}"

        return SparkClusterCreateResponse(
            cluster_id=self.cluster_id,
            master_url=master_url,
            master_ui_url=master_ui_url,
        )

    def _create_master_deployment(self, cores: int, memory: str):
        """
        Create the Spark master deployment.

        Args:
            cores: Number of CPU cores for the master
            memory: Memory allocation for the master
        """
        template_values = {
            "MASTER_NAME": self.master_name,
            "NAMESPACE": self.namespace,
            "USERNAME": self.username,
            "USERNAME_LOWER": self.username.lower(),
            "CLUSTER_ID": self.cluster_id,
            "IMAGE": self.image,
            "IMAGE_PULL_POLICY": self.image_pull_policy,
            "SPARK_MASTER_PORT": self.DEFAULT_MASTER_PORT,
            "SPARK_MASTER_WEBUI_PORT": self.DEFAULT_MASTER_WEBUI_PORT,
            "MAX_EXECUTORS": os.environ.get(
                "MAX_EXECUTORS", self.DEFAULT_MAX_EXECUTORS
            ),
            "MAX_CORES_PER_APPLICATION": os.environ.get(
                "MAX_CORES_PER_APPLICATION", self.DEFAULT_MAX_CORES_PER_APPLICATION
            ),
            "EXECUTOR_CORES": os.environ.get(
                "EXECUTOR_CORES", self.DEFAULT_EXECUTOR_CORES
            ),
            "SPARK_MASTER_MEMORY": memory,
            "SPARK_MASTER_CORES": cores,
            "MASTER_NODE_SELECTOR_VALUES": os.environ.get("MASTER_NODE_SELECTOR_VALUES", ""),
            "BERDL_POSTGRES_USER": os.environ["BERDL_POSTGRES_USER"],
            "BERDL_POSTGRES_PASSWORD": os.environ["BERDL_POSTGRES_PASSWORD"],
            "BERDL_POSTGRES_DB": os.environ["BERDL_POSTGRES_DB"],
            "BERDL_POSTGRES_URL": os.environ["BERDL_POSTGRES_URL"],
            "BERDL_REDIS_HOST": os.environ["BERDL_REDIS_HOST"],
            "BERDL_REDIS_PORT": os.environ["BERDL_REDIS_PORT"],
            "BERDL_DELTALAKE_WAREHOUSE_DIRECTORY_PATH": os.environ["BERDL_DELTALAKE_WAREHOUSE_DIRECTORY_PATH"],
        }

        deployment = render_yaml_template(
            self.MASTER_DEPLOYMENT_TEMPLATE, template_values
        )

        self._create_or_replace_deployment(
            deployment, self.master_name, "Spark master deployment"
        )
        return deployment



    def _create_master_service(self):
        """Create a Kubernetes service for the Spark master."""
        template_values = {
            "MASTER_NAME": self.master_name,
            "NAMESPACE": self.namespace,
            "USERNAME": self.username,
            "CLUSTER_ID": self.cluster_id,
            "SPARK_MASTER_PORT": self.DEFAULT_MASTER_PORT,
            "SPARK_MASTER_WEBUI_PORT": self.DEFAULT_MASTER_WEBUI_PORT,
        }

        service = render_yaml_template(self.MASTER_SERVICE_TEMPLATE, template_values)

        self._create_or_replace_service(
            service, self.master_name, "Spark master service"
        )

        return service

    def _create_worker_deployment(
        self,
        worker_count: int,
        worker_cores: int,
        worker_memory: str,
    ):
        """
        Create the Spark worker deployment.

        Args:
            worker_count: Number of worker replicas
            worker_cores: CPU cores per worker
            worker_memory: Memory allocation per worker in GiB
        """

        spark_memory_mb = float(worker_memory.replace('GiB', '').replace('Gi', '').replace('G', '')) * 1024 * 0.9
        spark_memory_mb = f"{int(spark_memory_mb)}m"

        template_values = {
            "WORKER_NAME": self.worker_name,
            "NAMESPACE": self.namespace,
            "USERNAME": self.username,
            "CLUSTER_ID": self.cluster_id,
            "IMAGE": self.image,
            "IMAGE_PULL_POLICY": self.image_pull_policy,
            "MASTER_NAME": self.master_name,
            "MASTER_PORT": self.DEFAULT_MASTER_PORT,
            "WORKER_COUNT": worker_count,
            "SPARK_WORKER_CONTAINER_CORES": worker_cores,
            "SPARK_WORKER_CONTAINER_MEMORY": worker_memory,
            "SPARK_WORKER_WEBUI_PORT": self.DEFAULT_WORKER_WEBUI_PORT,
            "SPARK_WORKER_PORT": os.environ.get("SPARK_WORKER_PORT"),
            "SPARK_WORKER_MEMORY": spark_memory_mb,
            "SPARK_WORKER_CORES": worker_cores,
            "WORKER_NODE_SELECTOR_VALUES": os.environ.get("WORKER_NODE_SELECTOR_VALUES", ""),
            "BERDL_POSTGRES_USER": os.environ["BERDL_POSTGRES_USER"],
            "BERDL_POSTGRES_PASSWORD": os.environ["BERDL_POSTGRES_PASSWORD"],
            "BERDL_POSTGRES_DB": os.environ["BERDL_POSTGRES_DB"],
            "BERDL_POSTGRES_URL": os.environ["BERDL_POSTGRES_URL"],
            "BERDL_REDIS_HOST": os.environ["BERDL_REDIS_HOST"],
            "BERDL_REDIS_PORT": os.environ["BERDL_REDIS_PORT"],
            "BERDL_DELTALAKE_WAREHOUSE_DIRECTORY_PATH": os.environ["BERDL_DELTALAKE_WAREHOUSE_DIRECTORY_PATH"],
            "BERDL_HIVE_METASTORE_URI": os.environ["BERDL_HIVE_METASTORE_URI"]
        }

        deployment = render_yaml_template(
            self.WORKER_DEPLOYMENT_TEMPLATE, template_values
        )

        self._create_or_replace_deployment(
            deployment,
            self.worker_name,
            f"Spark worker deployment with {worker_count} replicas",
        )

        return deployment

    def _create_or_replace_service(
        self, service: dict[str, Any], name: str, resource_description: str
    ) -> None:
        """
        Create a Kubernetes service, replacing it if it already exists.

        Args:
            service: The service definition
            name: Name of the service
            resource_description: Description of the resource for logging
        """
        try:
            # Try to create the service first
            self.core_api.create_namespaced_service(
                namespace=self.namespace, body=service
            )
            logger.info(f"Created {resource_description}: {name}")
        except ApiException as e:
            if e.status == 409:  # Conflict - already exists
                try:
                    # Delete the existing service
                    self.core_api.delete_namespaced_service(
                        name=name, namespace=self.namespace
                    )
                    logger.info(f"Deleted existing {resource_description}: {name}")

                    # Create new service
                    self.core_api.create_namespaced_service(
                        namespace=self.namespace, body=service
                    )
                    logger.info(f"Recreated {resource_description}: {name}")
                except ApiException as delete_error:
                    logger.error(
                        f"Error replacing {resource_description}: {delete_error}"
                    )
                    raise
            else:
                logger.error(f"Error creating {resource_description}: {e}")
                raise

    def _create_or_replace_deployment(
        self, deployment: dict[str, Any], name: str, resource_description: str
    ) -> None:
        """
        Create a Kubernetes deployment, replacing it if it already exists.

        Args:
            deployment: The deployment definition
            name: Name of the deployment
            resource_description: Description of the resource for logging
        """
        try:
            # Try to create the deployment first
            self.apps_api.create_namespaced_deployment(
                namespace=self.namespace, body=deployment
            )
            logger.info(f"Created {resource_description}: {name}")
        except ApiException as e:
            if e.status == 409:  # Conflict - already exists
                try:
                    # Delete the existing deployment
                    self.apps_api.delete_namespaced_deployment(
                        name=name, namespace=self.namespace
                    )
                    logger.info(f"Deleted existing {resource_description}: {name}")

                    # Create new deployment
                    self.apps_api.create_namespaced_deployment(
                        namespace=self.namespace, body=deployment
                    )
                    logger.info(f"Recreated {resource_description}: {name}")
                except ApiException as delete_error:
                    logger.error(
                        f"Error replacing {resource_description}: {delete_error}"
                    )
                    raise
            else:
                logger.error(f"Error creating {resource_description}: {e}")
                raise

    def get_cluster_status(self) -> SparkClusterStatus:
        """
        Get the status of the Spark cluster for the authenticated user.

        Returns:
            SparkClusterStatus: The current status of the Spark cluster
        """
        master_status = self._get_deployment_status(self.master_name)
        worker_status = self._get_deployment_status(self.worker_name)

        master_url = None
        master_ui_url = None

        # Add the master URL if the master has ready replicas
        if master_status.ready_replicas > 0:
            master_url = f"spark://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_PORT}"
            master_ui_url = f"http://{self.master_name}.{self.namespace}:{self.DEFAULT_MASTER_WEBUI_PORT}"

        status = SparkClusterStatus(
            master=master_status,
            workers=worker_status,
            master_url=master_url,
            master_ui_url=master_ui_url,
            error=bool(master_status.error or worker_status.error),
        )

        return status

    def _get_deployment_status(self, deployment_name: str) -> DeploymentStatus:
        """
        Get the current status of a deployment.
        """

        status = DeploymentStatus(
            exists=False,
        )

        try:
            deployment = self.apps_api.read_namespaced_deployment(
                name=deployment_name, namespace=self.namespace
            )

            # Set exists flag since we found the deployment
            status.exists = True

            status_obj = getattr(deployment, "status", None)

            if status_obj is None:
                status.error = "Deployment status is None"
                return status

            # Update status with actual values
            # Exercising precaution with the Kubernetes API, so opting to use getattr
            status.available_replicas = (
                getattr(status_obj, "available_replicas", 0) or 0
            )
            status.ready_replicas = getattr(status_obj, "ready_replicas", 0) or 0
            status.replicas = getattr(status_obj, "replicas", 0) or 0
            status.unavailable_replicas = (
                getattr(status_obj, "unavailable_replicas", 0) or 0
            )
            status.is_ready = (
                status.ready_replicas == status.replicas and status.replicas > 0
            )

        except Exception as e:
            logger.exception(
                "Error while fetching deployment status for '%s' in namespace '%s': %s",
                deployment_name,
                self.namespace,
                str(e),
            )
            status.error = str(e)

        return status

    def _attempt_delete(
        self, delete_fn: Callable, resource_name: str, resource_label: str
    ) -> dict[str, bool]:
        """
        Helper method to delete a Kubernetes resource.
        """
        result = {"deleted": False, "resource_exists": True}
        try:
            delete_fn(name=resource_name, namespace=self.namespace)
            logger.info(f"Deleted Spark {resource_label}: {resource_name}")
            result["deleted"] = True
        except ApiException as e:
            if e.status == 404:  # Resource not found
                logger.warning(
                    f"Spark {resource_label} {resource_name} not found, skipping deletion"
                )
                result["resource_exists"] = False
            else:
                logger.error(f"Error deleting Spark {resource_label}: {e}")
                raise
        return result

    def delete_cluster(self) -> ClusterDeleteResponse:
        """Delete the entire Spark cluster for the authenticated user."""
        try:
            deletion_results = {
                "worker_deployment": self._attempt_delete(
                    self.apps_api.delete_namespaced_deployment,
                    self.worker_name,
                    "worker deployment",
                ),
                "master_deployment": self._attempt_delete(
                    self.apps_api.delete_namespaced_deployment,
                    self.master_name,
                    "master deployment",
                ),
                "master_service": self._attempt_delete(
                    self.core_api.delete_namespaced_service,
                    self.master_name,
                    "master service",
                ),
            }

            resources_found = sum(
                1 for res in deletion_results.values() if res["resource_exists"]
            )
            resources_deleted = sum(
                1 for res in deletion_results.values() if res["deleted"]
            )

            if resources_found == 0:
                status_message = (
                    f"No Spark cluster resources found for user {self.username}"
                )
            elif resources_deleted == resources_found:
                status_message = (
                    f"Spark cluster for user {self.username} deleted successfully"
                )
            else:
                raise ClusterDeletionError(
                    f"Spark cluster deletion partially completed for user {self.username}"
                )

            return ClusterDeleteResponse(message=status_message)

        except Exception as e:
            logger.error(f"Error deleting Spark cluster: {e}")
            raise
