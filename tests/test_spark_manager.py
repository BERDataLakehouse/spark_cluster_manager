import json
import os
from unittest.mock import patch
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
