"""Tests for the clusters routes module."""

from src.routes import clusters


def test_clusters_imports():
    """Test that clusters routes module can be imported."""
    assert clusters is not None


def test_noop():
    """Simple placeholder test."""
    assert 1 == 1 