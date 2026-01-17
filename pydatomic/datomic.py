"""Datomic REST API client."""

from urllib.parse import urljoin

import requests

from pydatomic.edn import loads
from pydatomic.exceptions import DatomicClientError, DatomicConnectionError


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
        return urljoin(self.location, "data/") + self.storage + "/" + dbname

    def _request(
        self,
        method: str,
        url: str,
        *,
        expected_status: tuple[int, ...] = (200,),
        **kwargs,
    ) -> requests.Response:
        """Make an HTTP request with error handling."""
        kwargs.setdefault("timeout", self.timeout)
        try:
            r = getattr(requests, method)(url, **kwargs)
        except requests.exceptions.ConnectionError as e:
            raise DatomicConnectionError(f"Failed to connect to {url}: {e}") from e
        except requests.exceptions.Timeout as e:
            raise DatomicConnectionError(f"Request to {url} timed out: {e}") from e
        except requests.exceptions.RequestException as e:
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

    def transact(self, dbname: str, data: list[str]) -> dict:
        """Execute a transaction."""
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
        self, dbname: str, query: str, extra_args: list | None = None, history: bool = False
    ) -> tuple:
        """Execute a query against the database."""
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

    def entity(self, dbname: str, eid: int) -> dict:
        """Retrieve an entity by ID."""
        r = self._request(
            "get",
            self.db_url(dbname) + "/-/entity",
            params={"e": eid},
            headers={"Accept": "application/edn"},
            expected_status=(200,),
        )
        return loads(r.content)


if __name__ == "__main__":
    q = """[{
  :db/id #db/id[:db.part/db]
  :db/ident :person/name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/doc "A person's name"
  :db.install/_attribute :db.part/db}]"""

    conn = Datomic("http://localhost:3000/", "tdb")
    db = conn.create_database("cms")
    db.transact(q)
    db.transact('[{:db/id #db/id[:db.part/user] :person/name "Peter"}]')
    r = db.query("[:find ?e ?n :where [?e :person/name ?n]]")
    print(r)
    eid = r[0][0]
    print(db.query("[:find ?n :in $ ?e :where [?e :person/name ?n]]", [eid], history=True))
    print(db.entity(eid))
