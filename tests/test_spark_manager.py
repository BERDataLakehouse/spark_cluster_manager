import json
import os
from unittest.mock import patch, MagicMock
from src.spark_manager import KubeSparkManager


def test_noop():
    assert 1 == 1


def test_parse_tolerations_empty():
    """Test that empty tolerations return empty string."""
    with patch.dict(os.environ, {}, clear=False):
        # Remove BERDL_TOLERATIONS if it exists
        if 'BERDL_TOLERATIONS' in os.environ:
            del os.environ['BERDL_TOLERATIONS']
        
        # Mock the kubernetes config loading to avoid actual cluster connection
        with patch('kubernetes.config.load_incluster_config'):
            manager = KubeSparkManager('testuser')
            result = manager._parse_tolerations()
            assert result == ""


def test_parse_tolerations_invalid_json():
    """Test that invalid JSON tolerations return empty string."""
    invalid_json = '{"key": "environments", "operator": "Equal", "value": "dev"'  # Missing closing brace
    
    with patch.dict(os.environ, {'BERDL_TOLERATIONS': invalid_json}):
        with patch('kubernetes.config.load_incluster_config'):
            manager = KubeSparkManager('testuser')
            result = manager._parse_tolerations()
            assert result == ""


def test_parse_tolerations_not_list():
    """Test that non-list JSON tolerations return empty string."""
    not_list_json = '{"key": "environments", "operator": "Equal", "value": "dev"}'
    
    with patch.dict(os.environ, {'BERDL_TOLERATIONS': not_list_json}):
        with patch('kubernetes.config.load_incluster_config'):
            manager = KubeSparkManager('testuser')
            result = manager._parse_tolerations()
            assert result == ""


def test_parse_tolerations_valid():
    """Test that valid tolerations JSON is returned correctly."""
    tolerations = [
        {
            "key": "environments",
            "operator": "Equal",
            "value": "dev",
            "effect": "NoSchedule"
        },
        {
            "key": "environments",
            "operator": "Equal", 
            "value": "prod",
            "effect": "NoSchedule"
        }
    ]
    tolerations_json = json.dumps(tolerations)
    
    with patch.dict(os.environ, {'BERDL_TOLERATIONS': tolerations_json}):
        with patch('kubernetes.config.load_incluster_config'):
            manager = KubeSparkManager('testuser')
            result = manager._parse_tolerations()
            assert result == tolerations_json


def test_create_master_deployment_includes_tolerations():
    """Test that _create_master_deployment includes tolerations in template values."""
    tolerations = [{"key": "environments", "operator": "Equal", "value": "dev", "effect": "NoSchedule"}]
    tolerations_json = json.dumps(tolerations)
    
    with patch.dict(os.environ, {'BERDL_TOLERATIONS': tolerations_json}):
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
                    assert template_values['BERDL_TOLERATIONS'] == tolerations_json


def test_create_worker_deployment_includes_tolerations():
    """Test that _create_worker_deployment includes tolerations in template values."""
    tolerations = [{"key": "environments", "operator": "Equal", "value": "prod", "effect": "NoSchedule"}]
    tolerations_json = json.dumps(tolerations)
    
    with patch.dict(os.environ, {'BERDL_TOLERATIONS': tolerations_json}):
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
                    assert template_values['BERDL_TOLERATIONS'] == tolerations_json
