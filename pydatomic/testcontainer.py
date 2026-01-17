"""Datomic testcontainer for integration testing.

This module provides a Docker-based Datomic container for integration testing
using testcontainers-python. It sets up a Datomic environment with a REST API
that can be used with the pydatomic client library.

The container runs:
- Datomic Pro transactor (in-memory dev mode)
- Datomic REST server (bin/rest)
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

if TYPE_CHECKING:
    from pydatomic import Datomic

# Datomic ports
TRANSACTOR_PORT = 4334
REST_PORT = 3000

# Datomic Pro version
DATOMIC_VERSION = "1.0.7482"


class DatomicContainer:
    """A testcontainer that runs Datomic Pro with a REST API server.

    This container runs a Datomic Pro transactor with an embedded REST server,
    providing HTTP access to the database compatible with pydatomic.

    The container is built from scratch using an ARM64/AMD64 compatible base
    image to support both Intel and Apple Silicon Macs.

    Example usage:
        with DatomicContainer() as datomic:
            conn = datomic.get_connection()
            db = conn.create_database("test-db")
            db.transact('[{:db/id #db/id[:db.part/user] :test/name "value"}]')
            result = db.query("[:find ?e :where [?e :test/name]]")
    """

    def __init__(
        self,
        storage_alias: str = "dev",
        datomic_version: str = DATOMIC_VERSION,
    ):
        """Initialize the Datomic testcontainer.

        Args:
            storage_alias: The storage alias to use for the REST API.
            datomic_version: The version of Datomic Pro to use.
        """
        self.storage_alias = storage_alias
        self.datomic_version = datomic_version

        self._container: DockerContainer | None = None
        self._image: DockerImage | None = None
        self._started = False

    def start(self) -> DatomicContainer:
        """Start the Datomic container.

        Returns:
            Self for method chaining.
        """
        if self._started:
            return self

        # Build a custom image that includes Datomic and the REST server
        self._build_image()

        self._container = (
            DockerContainer("pydatomic-datomic-test:latest")
            .with_exposed_ports(TRANSACTOR_PORT, REST_PORT)
            .waiting_for(LogMessageWaitStrategy("System started").with_startup_timeout(180))
        )

        self._container.start()

        # Wait additional time for REST server
        time.sleep(5)

        # Wait for the REST server to be ready
        self._wait_for_rest_server()

        self._started = True
        return self

    def _build_image(self) -> None:
        """Build the Datomic Docker image."""
        import tempfile

        dockerfile_content = self._get_dockerfile()

        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile_path = Path(tmpdir) / "Dockerfile"
            dockerfile_path.write_text(dockerfile_content)

            # Create the start script
            start_script_path = Path(tmpdir) / "start.sh"
            start_script_path.write_text(self._get_start_script())

            # Build the image
            self._image = DockerImage(
                path=tmpdir,
                tag="pydatomic-datomic-test:latest",
            )
            self._image.build()

    def _get_dockerfile(self) -> str:
        """Read the Dockerfile template for the Datomic image."""
        dockerfile_path = Path(__file__).with_name("dockerfile.Datomic")
        dockerfile_template = dockerfile_path.read_text()
        return dockerfile_template.format(
            datomic_version=self.datomic_version,
            transactor_port=TRANSACTOR_PORT,
            rest_port=REST_PORT,
        )

    def _get_start_script(self) -> str:
        """Generate the startup script."""
        return f"""#!/bin/bash
set -e

echo "Starting Datomic transactor..."
cd /opt/datomic

# Start the transactor in the background
bin/transactor transactor-dev.properties &
TRANSACTOR_PID=$!

# Wait for transactor to be ready
echo "Waiting for transactor to start on port {TRANSACTOR_PORT}..."
while ! nc -z localhost {TRANSACTOR_PORT} 2>/dev/null; do
    sleep 1
done
echo "Transactor is ready"

# Give transactor a moment to fully initialize
sleep 5

# Start the REST server
echo "Starting REST server on port {REST_PORT}..."
bin/rest -p {REST_PORT} {self.storage_alias} datomic:dev://localhost:{TRANSACTOR_PORT}/ &
REST_PID=$!

# Wait for REST server to be ready
echo "Waiting for REST server to start..."
while ! nc -z localhost {REST_PORT} 2>/dev/null; do
    sleep 1
done
echo "REST server is ready on port {REST_PORT}"

# Keep the script running and wait for processes
wait -n $TRANSACTOR_PID $REST_PID
"""

    def _wait_for_rest_server(self, timeout: int = 120) -> None:
        """Wait for the REST server to be ready."""
        import requests

        url = self.get_rest_url()
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{url}data/", timeout=5)
                if response.status_code == 200:
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)

        raise TimeoutError(f"REST server did not start within {timeout} seconds")

    def stop(self) -> None:
        """Stop and remove the container."""
        if self._container:
            try:
                self._container.stop()
            except Exception:
                pass
            self._container = None

        self._started = False

    def get_rest_url(self) -> str:
        """Get the URL for the Datomic REST API.

        Returns:
            The HTTP URL to connect to the REST server.
        """
        if not self._container:
            raise RuntimeError("Container not started")

        host = self._container.get_container_host_ip()
        port = self._container.get_exposed_port(REST_PORT)
        return f"http://{host}:{port}/"

    def get_storage_alias(self) -> str:
        """Get the storage alias for connecting to Datomic.

        Returns:
            The storage alias to use with the Datomic client.
        """
        return self.storage_alias

    def get_connection(self) -> Datomic:
        """Get a Datomic connection object.

        Returns:
            A configured Datomic client ready to use.
        """
        from pydatomic import Datomic

        return Datomic(self.get_rest_url(), self.get_storage_alias())

    def __enter__(self) -> DatomicContainer:
        """Context manager entry."""
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()
