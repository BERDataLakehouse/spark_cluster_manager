import os
from src.template_utils import render_yaml_template


def test_noop():
    assert 1 == 1


def test_render_yaml_template_with_tolerations():
    """Test that tolerations are correctly included in rendered YAML."""
    # Create a simple template for testing
    template_content = """
spec:
  tolerations:
    - key: "noschedule"
      operator: "Equal"
      value: "True"
      effect: "NoSchedule"
    {% if BERDL_TOLERATIONS and BERDL_TOLERATIONS.strip() %}
    - key: "environments"
      operator: "Equal"
      value: "{{ BERDL_TOLERATIONS }}"
      effect: "NoSchedule"
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
        # Test with environment-specific toleration
        environment = "dev"
        
        values = {
            "BERDL_TOLERATIONS": environment
        }
        
        result = render_yaml_template(template_path, values)
        
        # Check that tolerations are included in the rendered YAML
        assert 'tolerations' in result['spec']
        tolerations = result['spec']['tolerations']
        
        # Should have both default and environment tolerations
        assert len(tolerations) == 2
        
        # Check default toleration
        default_toleration = tolerations[0]
        assert default_toleration['key'] == 'noschedule'
        assert default_toleration['operator'] == 'Equal'
        assert default_toleration['value'] == 'True'
        assert default_toleration['effect'] == 'NoSchedule'
        
        # Check environment toleration
        env_toleration = tolerations[1]
        assert env_toleration['key'] == 'environments'
        assert env_toleration['operator'] == 'Equal'
        assert env_toleration['value'] == environment
        assert env_toleration['effect'] == 'NoSchedule'
        
        # Test without environment toleration
        values_empty = {
            "BERDL_TOLERATIONS": ""
        }
        
        result_empty = render_yaml_template(template_path, values_empty)
        assert 'tolerations' in result_empty['spec']
        
        # Should only have the default toleration
        tolerations_empty = result_empty['spec']['tolerations']
        assert len(tolerations_empty) == 1
        assert tolerations_empty[0]['key'] == 'noschedule'
        
    finally:
        # Clean up temp file
        os.unlink(template_path)
