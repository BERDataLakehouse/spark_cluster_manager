"""Tests for the exceptions module."""

from src.service.exceptions import SparkManagerError


def test_exceptions_imports():
    """Test that exceptions module can be imported."""
    assert SparkManagerError is not None


def test_noop():
    """Simple placeholder test."""
    assert 1 == 1 
