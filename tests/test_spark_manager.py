import os
from unittest.mock import patch, MagicMock
import pytest
from src.spark_manager import KubeSparkManager


def test_noop():
    assert 1 == 1



def test_missing_berdl_tolerations_raises_error():
    """Test that missing BERDL_TOLERATIONS raises a ValueError."""
    # Create an environment without BERDL_TOLERATIONS
    env_without_tolerations = {k: v for k, v in os.environ.items() if k != 'BERDL_TOLERATIONS'}
    
    with patch.dict(os.environ, env_without_tolerations, clear=True):
        with patch('kubernetes.config.load_incluster_config'):
            with pytest.raises(ValueError, match="Missing required environment variables"):
                KubeSparkManager('testuser')


def test_create_master_deployment_includes_tolerations():
    """Test that _create_master_deployment includes tolerations in template values."""
    environment = "dev"
    
    with patch.dict(os.environ, {'BERDL_TOLERATIONS': environment}):
        with patch('kubernetes.config.load_incluster_config'):
            with patch('src.spark_manager.render_yaml_template') as mock_render:
                with patch.object(KubeSparkManager, '_create_or_replace_deployment'):
                    mock_render.return_value = {'mock': 'deployment'}
                    
                    manager = KubeSparkManager('testuser')
                    manager._create_master_deployment(2, '4Gi')
                    
                    # Verify render_yaml_template was called with tolerations
                    assert mock_render.called
                    call_args = mock_render.call_args[0]
                    template_values = call_args[1]
                    assert 'BERDL_TOLERATIONS' in template_values
                    assert template_values['BERDL_TOLERATIONS'] == environment


def test_create_worker_deployment_includes_tolerations():
    """Test that _create_worker_deployment includes tolerations in template values."""
    environment = "prod"
    
    with patch.dict(os.environ, {'BERDL_TOLERATIONS': environment}):
        with patch('kubernetes.config.load_incluster_config'):
            with patch('src.spark_manager.render_yaml_template') as mock_render:
                with patch.object(KubeSparkManager, '_create_or_replace_deployment'):
                    mock_render.return_value = {'mock': 'deployment'}
                    
                    manager = KubeSparkManager('testuser')
                    manager._create_worker_deployment(3, 2, '4Gi')
                    
                    # Verify render_yaml_template was called with tolerations
                    assert mock_render.called
                    call_args = mock_render.call_args[0]
                    template_values = call_args[1]
                    assert 'BERDL_TOLERATIONS' in template_values
                    assert template_values['BERDL_TOLERATIONS'] == environment
