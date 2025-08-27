import json
import os
from src.template_utils import render_yaml_template


def test_noop():
    assert 1 == 1


def test_render_yaml_template_with_tolerations():
    """Test that tolerations are correctly included in rendered YAML."""
    # Create a simple template for testing
    template_content = """
spec:
  {% if BERDL_TOLERATIONS and BERDL_TOLERATIONS.strip() %}
  tolerations: {{ BERDL_TOLERATIONS }}
  {% endif %}
  containers:
    - name: test
"""
    
    # Write template to a temporary file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(template_content)
        template_path = f.name
    
    try:
        # Test with tolerations
        tolerations = [
            {
                "key": "environments",
                "operator": "Equal",
                "value": "dev", 
                "effect": "NoSchedule"
            }
        ]
        tolerations_json = json.dumps(tolerations)
        
        values = {
            "BERDL_TOLERATIONS": tolerations_json
        }
        
        result = render_yaml_template(template_path, values)
        
        # Check that tolerations are included in the rendered YAML
        assert 'tolerations' in result['spec']
        assert result['spec']['tolerations'] == tolerations
        
        # Test without tolerations
        values_empty = {
            "BERDL_TOLERATIONS": ""
        }
        
        result_empty = render_yaml_template(template_path, values_empty)
        assert 'tolerations' not in result_empty['spec']
        
    finally:
        # Clean up temp file
        os.unlink(template_path)
