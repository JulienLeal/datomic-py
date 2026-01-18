"""Shared pytest fixtures for tests."""

import os
import subprocess
from unittest.mock import patch

import httpx
import pytest


def get_docker_host() -> str | None:
    """
    Get the Docker host from docker context or environment.

    Supports:
    - Standard Docker (Linux): uses default /var/run/docker.sock
    - Docker Desktop (Mac/Windows): uses default socket or context
    - Colima (Mac): detects socket from docker context

    Returns:
        The Docker host URL, or None to use the default socket.

    """
    # First check if DOCKER_HOST is already set
    if os.environ.get("DOCKER_HOST"):
        return os.environ["DOCKER_HOST"]

    # Try to get the Docker endpoint from the current context
    # This works for Colima, Docker Desktop, and other non-default setups
    try:
        result = subprocess.run(
            ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            host = result.stdout.strip()
            # Only set if it's not the default socket (let Docker use its default)
            if host != "unix:///var/run/docker.sock":
                return host
    except Exception:
        pass

    # Return None to let Docker library use its default socket
    return None


def configure_docker_environment():
    """
    Configure environment variables for Docker/testcontainers.

    This function detects the Docker setup and configures the environment
    accordingly. It works with:
    - Standard Docker on Linux (default socket)
    - Docker Desktop on Mac/Windows
    - Colima on Mac
    """
    docker_host = get_docker_host()
    if docker_host:
        os.environ["DOCKER_HOST"] = docker_host

    # Disable Ryuk which has issues with some Docker setups (e.g., Colima)
    # This is safe for standard Docker as well
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"


# Configure Docker environment when conftest is loaded
configure_docker_environment()


@pytest.fixture
def mock_httpx():
    """Fixture to mock httpx module."""
    with patch("pydatomic.datomic.httpx") as mock:
        yield mock


@pytest.fixture
def mock_httpx_with_exceptions():
    """Fixture to mock httpx module with real exceptions."""
    with patch("pydatomic.datomic.httpx") as mock:
        # Preserve the real exception classes
        mock.ConnectError = httpx.ConnectError
        mock.TimeoutException = httpx.TimeoutException
        mock.HTTPError = httpx.HTTPError
        yield mock


@pytest.fixture(scope="module")
def datomic_container():
    """
    Provide a Datomic container for the test module.

    Using module scope to avoid starting a new container for each test,
    which would be slow.
    """
    from pydatomic.testcontainer import DatomicContainer

    with DatomicContainer() as container:
        yield container


@pytest.fixture(scope="module")
def conn(datomic_container):
    """Provide a Datomic connection."""
    return datomic_container.get_connection()


@pytest.fixture(scope="module")
def db(conn):
    """
    Provide a database for testing.

    Uses a single database for all tests to avoid resource issues.
    """
    return conn.create_database("test-db")
