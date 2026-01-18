"""Async Datomic REST API client."""

from urllib.parse import urljoin

import httpx

from pydatomic.edn import loads
from pydatomic.exceptions import DatomicClientError, DatomicConnectionError


class AsyncDatabase:
    """Async wrapper around a Datomic database that delegates to the connection."""

    def __init__(self, name: str, conn: "AsyncDatomic"):
        self.name = name
        self.conn = conn

    def __getattr__(self, name: str):
        async def f(*args, **kwargs):
            return await getattr(self.conn, name)(self.name, *args, **kwargs)

        return f


class AsyncDatomic:
    """Async Datomic REST API client."""

    def __init__(self, location: str, storage: str, timeout: float = 30.0):
        self.location = location
        self.storage = storage
        self.timeout = timeout

    def db_url(self, dbname: str) -> str:
        """Construct the database URL."""
        return urljoin(self.location, "data/") + self.storage + "/" + dbname

    async def _request(
        self,
        method: str,
        url: str,
        *,
        expected_status: tuple[int, ...] = (200,),
        **kwargs,
    ) -> httpx.Response:
        """Make an async HTTP request with error handling."""
        kwargs.setdefault("timeout", self.timeout)
        try:
            async with httpx.AsyncClient() as client:
                r = await client.request(method.upper(), url, **kwargs)
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

    async def create_database(self, dbname: str) -> AsyncDatabase:
        """Create a new database."""
        await self._request(
            "post",
            self.db_url(""),
            data={"db-name": dbname},
            expected_status=(200, 201),
        )
        return AsyncDatabase(dbname, self)

    async def transact(self, dbname: str, data: list[str]) -> dict:
        """Execute a transaction."""
        data_str = f"[{'\n'.join(data)}\n]"
        r = await self._request(
            "post",
            self.db_url(dbname) + "/",
            data={"tx-data": data_str},
            headers={"Accept": "application/edn"},
            expected_status=(200, 201),
        )
        return loads(r.content)

    async def query(
        self, dbname: str, query: str, extra_args: list | None = None, history: bool = False
    ) -> tuple:
        """Execute a query against the database."""
        if extra_args is None:
            extra_args = []
        args = "[{:db/alias " + self.storage + "/" + dbname
        if history:
            args += " :history true"
        args += "} " + " ".join(str(a) for a in extra_args) + "]"
        r = await self._request(
            "get",
            urljoin(self.location, "api/query"),
            params={"args": args, "q": query},
            headers={"Accept": "application/edn"},
            expected_status=(200,),
        )
        return loads(r.content)

    async def entity(self, dbname: str, eid: int) -> dict:
        """Retrieve an entity by ID."""
        r = await self._request(
            "get",
            self.db_url(dbname) + "/-/entity",
            params={"e": eid},
            headers={"Accept": "application/edn"},
            expected_status=(200,),
        )
        return loads(r.content)
