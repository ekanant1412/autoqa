# Root-level conftest — registers custom CLI options so pytest can parse them
# before loading tests/conftest.py (which has the full implementation)
import pytest

def pytest_addoption(parser):
    try:
        parser.addoption("--ssoId", action="store", default=None)
        parser.addoption("--max-cursors", action="store", default="5")
        parser.addoption("--save-responses", action="store_true", default=False)
        parser.addoption("--dag", action="store", default="all")
    except ValueError:
        pass  # already registered by tests/conftest.py
