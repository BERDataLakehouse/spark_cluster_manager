import json
import os
from src.spark_manager import KubeSparkManager
from unittest.mock import patch


def assert_no_none_values(nested_dict):
    """Ensure no None values in a nested dictionary."""
    for key, value in nested_dict.items():
        if isinstance(value, dict):
            assert_no_none_values(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    assert_no_none_values(item)
                else:
                    assert item is not None
        else:
            assert value is not None


def test_spark_worker_create():
    """Test the creation of a Spark worker template."""
    """ This is both a "Real" unit test, and a helper to checks to see if you made a mistake in mapping environ variables to the jinja 
    template."""
    """ It tells you if there is a None value somewhere, and then you can dump the template to a file to see what is missing. """

    # Patch out kubernetes client to avoid actual API calls

    with patch("src.spark_manager.k8s") as mock_kubernetes:
        mock_kubernetes.client.CoreV1Api.return_value.create_namespaced_pod = (
            lambda *args, **kwargs: None
        )

        spark_manager = KubeSparkManager(username="Bob")

        # validate_environment
        assert spark_manager is not None
        assert spark_manager.validate_environment()

        master_template = spark_manager._create_master_deployment(
            cores=1, memory="1GiB"
        )
        assert_no_none_values(master_template)

        masster_service = spark_manager._create_master_service()
        assert_no_none_values(masster_service)

        worker_template = spark_manager._create_worker_deployment(
            worker_count=2, worker_cores=1, worker_memory="2GiB"
        )

        assert_no_none_values(worker_template)

        #
        # master_service = spark_manager._create_master_service()
        # assert master_service is not None
        # with open('master_service.json', 'w') as f:
        #     json.dump(master_service, f, indent=4)
        #
