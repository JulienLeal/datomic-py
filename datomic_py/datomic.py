"""Datomic REST API client."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar, overload

import httpx

from datomic_py.edn import loads
from datomic_py.exceptions import DatomicClientError, DatomicConnectionError

if TYPE_CHECKING:
    from datomic_py.serialization.factories import EntityFactory, RowFactory

T = TypeVar("T")


class Database:
    """Wrapper around a Datomic database that delegates to the connection."""

    def __init__(self, name: str, conn: Datomic):
        self.name = name
        self.conn = conn

    def __getattr__(self, name: str) -> Callable[..., Any]:
        def f(*args: Any, **kwargs: Any) -> Any:
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
        from urllib.parse import urljoin

        base = urljoin(self.location, "data/")
        return f"{base}{self.storage}/{dbname}"

    def _request(
        self,
        method: str,
        url: str,
        *,
        expected_status: tuple[int, ...] = (200,),
        **kwargs: Any,
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

    def db(self, dbname: str) -> Database:
        """
        Get a Database wrapper for the given database name.

        Args:
            dbname: The name of the database.

        Returns:
            A Database instance that delegates calls to this connection.

        """
        return Database(dbname, self)

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

    @overload
    def query(
        self,
        dbname: str,
        query: str,
        extra_args: list[Any] | None = None,
        history: bool = False,
        *,
        row_factory: None = None,
        columns: Sequence[str] | None = None,
    ) -> tuple[tuple[Any, ...], ...]: ...

    @overload
    def query(
        self,
        dbname: str,
        query: str,
        extra_args: list[Any] | None = None,
        history: bool = False,
        *,
        row_factory: RowFactory[T],
        columns: Sequence[str] | None = None,
    ) -> tuple[T, ...]: ...

    def query(
        self,
        dbname: str,
        query: str,
        extra_args: list[Any] | None = None,
        history: bool = False,
        *,
        row_factory: RowFactory[T] | None = None,
        columns: Sequence[str] | None = None,
    ) -> tuple[tuple[Any, ...], ...] | tuple[T, ...]:
        """
        Execute a query against the database.

        Args:
            dbname: The name of the database.
            query: A Datomic query in EDN format.
            extra_args: Optional list of additional query arguments.
            history: If True, query against the full history of the database.
            row_factory: Optional factory to transform result rows.
                        If provided, each row tuple is passed through this factory.
            columns: Column names for row factory. If not provided, they are
                    extracted from the :find clause of the query.

        Returns:
            A tuple of tuples (default) or transformed objects if row_factory provided.

        Example:
            # Default: returns tuples
            results = conn.query(db, "[:find ?name :where [?e :person/name ?name]]")
            # -> (("Alice",), ("Bob",))

            # With dict_row factory
            from datomic_py.serialization import dict_row
            results = conn.query(db, q, row_factory=dict_row)
            # -> ({"name": "Alice"}, {"name": "Bob"})

        """
        from urllib.parse import urljoin

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
        raw_result: tuple[tuple[Any, ...], ...] = loads(r.content)

        if row_factory is None:
            return raw_result

        # Extract column names from query if not provided
        if columns is None:
            columns = self._extract_find_vars(query)

        return tuple(row_factory(row, columns) for row in raw_result)

    def _extract_find_vars(self, query: str) -> tuple[str, ...]:
        """
        Extract variable names from :find clause.

        Args:
            query: The Datomic query string.

        Returns:
            A tuple of variable names from the :find clause.

        """
        # Find the :find clause and extract ?var patterns
        # Handle various :find patterns including pull expressions
        find_match = re.search(r":find\s+(.*?)(?:\s*:(?:in|where|with|keys|strs|syms)\s|$)", query, re.DOTALL | re.IGNORECASE)
        if find_match:
            find_clause = find_match.group(1)
            # Extract ?var patterns (ignore variables inside pull expressions)
            # Simple approach: find all ?word patterns
            vars_found = re.findall(r"\?(\w+)", find_clause)
            return tuple(vars_found)
        return ()

    @overload
    def entity(
        self,
        dbname: str,
        eid: int,
        *,
        entity_factory: None = None,
    ) -> dict[str, Any]: ...

    @overload
    def entity(
        self,
        dbname: str,
        eid: int,
        *,
        entity_factory: EntityFactory[T],
    ) -> T: ...

    def entity(
        self,
        dbname: str,
        eid: int,
        *,
        entity_factory: EntityFactory[T] | None = None,
    ) -> dict[str, Any] | T:
        """
        Retrieve an entity by ID.

        Args:
            dbname: Database name.
            eid: Entity ID.
            entity_factory: Optional factory to transform the entity.
                           If provided, the raw entity dict is passed through this factory.

        Returns:
            Entity dict (default) or transformed object if factory provided.

        Example:
            # Default: returns dict
            entity = conn.entity(db, 123)
            # -> {":db/id": 123, ":person/name": "Alice", ...}

            # With clean_dict_entity factory
            from datomic_py.serialization import clean_dict_entity
            entity = conn.entity(db, 123, entity_factory=clean_dict_entity())
            # -> {"db_id": 123, "name": "Alice", ...}

        """
        r = self._request(
            "get",
            self.db_url(dbname) + "/-/entity",
            params={"e": eid},
            headers={"Accept": "application/edn"},
            expected_status=(200,),
        )
        raw_entity: dict[str, Any] = loads(r.content)

        if entity_factory is None:
            return raw_entity

        return entity_factory(raw_entity)
