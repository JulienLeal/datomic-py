"""Datomic REST API client."""

from typing import Any
from urllib.parse import urljoin

import httpx

from datomic_py.edn import loads
from datomic_py.exceptions import DatomicClientError, DatomicConnectionError


class Database:
    """Wrapper around a Datomic database that delegates to the connection."""

    def __init__(self, name: str, conn: "Datomic"):
        self.name = name
        self.conn = conn

    def __getattr__(self, name: str):
        def f(*args, **kwargs):
            return getattr(self.conn, name)(self.name, *args, **kwargs)

        return f


class Datomic:
    """Datomic REST API client."""

    def __init__(self, location: str, storage: str, timeout: float = 30.0):
        self.location = location
        self.storage = storage
        self.timeout = timeout

    def db_url(self, dbname: str) -> str:
        """Construct the database URL."""
        base = urljoin(self.location, "data/")
        return f"{base}{self.storage}/{dbname}"

    def _request(
        self,
        method: str,
        url: str,
        *,
        expected_status: tuple[int, ...] = (200,),
        **kwargs,
    ) -> httpx.Response:
        """Make an HTTP request with error handling."""
        kwargs.setdefault("timeout", self.timeout)
        try:
            with httpx.Client() as client:
                r = client.request(method.upper(), url, **kwargs)
        except httpx.ConnectError as e:
            raise DatomicConnectionError(f"Failed to connect to {url}: {e}") from e
        except httpx.TimeoutException as e:
            raise DatomicConnectionError(f"Request to {url} timed out: {e}") from e
        except httpx.HTTPError as e:
            raise DatomicClientError(f"Request to {url} failed: {e}") from e

        if r.status_code not in expected_status:
            raise DatomicClientError(
                f"Request failed with status {r.status_code}: {r.text}"
            )
        return r

    def create_database(self, dbname: str) -> Database:
        """Create a new database."""
        self._request(
            "post",
            self.db_url(""),
            data={"db-name": dbname},
            expected_status=(200, 201),
        )
        return Database(dbname, self)

    def transact(self, dbname: str, data: list[str]) -> dict[str, Any]:
        """
        Execute a transaction against the database.

        Args:
            dbname: The name of the database.
            data: A list of EDN strings representing transaction data.
                  Each string should be a valid Datomic transaction map.

        Returns:
            A dict containing the transaction result with keys like
            ':db-before', ':db-after', ':tx-data', and ':tempids'.

        """
        data_str = f"[{'\n'.join(data)}\n]"
        r = self._request(
            "post",
            self.db_url(dbname) + "/",
            data={"tx-data": data_str},
            headers={"Accept": "application/edn"},
            expected_status=(200, 201),
        )
        return loads(r.content)

    def query(
        self, dbname: str, query: str, extra_args: list[Any] | None = None, history: bool = False
    ) -> tuple[tuple[Any, ...], ...]:
        """
        Execute a query against the database.

        Args:
            dbname: The name of the database.
            query: A Datomic query in EDN format.
            extra_args: Optional list of additional query arguments.
            history: If True, query against the full history of the database.

        Returns:
            A tuple of tuples containing the query results.

        """
        if extra_args is None:
            extra_args = []
        args = "[{:db/alias " + self.storage + "/" + dbname
        if history:
            args += " :history true"
        args += "} " + " ".join(str(a) for a in extra_args) + "]"
        r = self._request(
            "get",
            urljoin(self.location, "api/query"),
            params={"args": args, "q": query},
            headers={"Accept": "application/edn"},
            expected_status=(200,),
        )
        return loads(r.content)

    def entity(self, dbname: str, eid: int) -> dict[str, Any]:
        """Retrieve an entity by ID."""
        r = self._request(
            "get",
            self.db_url(dbname) + "/-/entity",
            params={"e": eid},
            headers={"Accept": "application/edn"},
            expected_status=(200,),
        )
        return loads(r.content)
