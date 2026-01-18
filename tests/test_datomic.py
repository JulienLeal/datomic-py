"""Tests for the Datomic REST client."""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import httpx
import pytest

from pydatomic.datomic import Database, Datomic
from pydatomic.exceptions import DatomicClientError, DatomicConnectionError


class TestDatomic:
    """Tests for Datomic client."""

    def test_create_db(self):
        """Verify create_database()."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_response = Mock(status_code=201)
        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            db = conn.create_database("cms")

            mock_client.request.assert_called_once_with(
                "POST",
                "http://localhost:3000/data/tdb/",
                data={"db-name": "cms"},
                timeout=30.0,
            )
            assert isinstance(db, Database)
            assert db.name == "cms"

    def test_transact(self):
        """Verify transact()."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_response = Mock(
            status_code=201,
            content=(
                b'{:db-before {:basis-t 63, :db/alias "dev/scratch"}, '
                b':db-after {:basis-t 1000, :db/alias "dev/scratch"}, '
                b':tx-data [{:e 13194139534312, :a 50, :v #inst "2014-12-01T15:27:26.632-00:00", '
                b':tx 13194139534312, :added true} {:e 17592186045417, :a 62, '
                b':v "hello REST world", :tx 13194139534312, :added true}], '
                b':tempids {-9223350046623220292 17592186045417}}'
            ),
        )

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            result = db.transact('[{:db/id #db/id[:db.part/user] :person/name "Peter"}]')

        assert result[":db-after"] == {":db/alias": "dev/scratch", ":basis-t": 1000}
        assert result[":db-before"] == {":db/alias": "dev/scratch", ":basis-t": 63}
        assert result[":tempids"] == {-9223350046623220292: 17592186045417}
        assert len(result[":tx-data"]) == 2

        # Check the transaction data
        tx_data = result[":tx-data"]
        assert tx_data[0][":e"] == 13194139534312
        assert tx_data[0][":added"] is True
        assert isinstance(tx_data[0][":v"], datetime)
        assert tx_data[1][":v"] == "hello REST world"

    def test_query(self):
        """Verify query()."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_response = Mock(status_code=200, content=b"[[17592186048482]]")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            result = db.query("[:find ?e ?n :where [?e :person/name ?n]]")

            assert result == ((17592186048482,),)
            mock_client.request.assert_called_once_with(
                "GET",
                "http://localhost:3000/api/query",
                headers={"Accept": "application/edn"},
                params={
                    "q": "[:find ?e ?n :where [?e :person/name ?n]]",
                    "args": "[{:db/alias tdb/db} ]",
                },
                timeout=30.0,
            )

    def test_query_with_history(self):
        """Verify query() with history flag."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_response = Mock(status_code=200, content=b'[["value"]]')

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            result = db.query("[:find ?n :where [?e :person/name ?n]]", history=True)

            assert result == (("value",),)
            call_args = mock_client.request.call_args
            assert ":history true" in call_args[1]["params"]["args"]

    def test_query_with_extra_args(self):
        """Verify query() with extra arguments."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_response = Mock(status_code=200, content=b'[["result"]]')

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            result = db.query("[:find ?n :in $ ?e :where [?e :person/name ?n]]", extra_args=[123])

            assert result == (("result",),)
            call_args = mock_client.request.call_args
            assert "123" in call_args[1]["params"]["args"]

    def test_entity(self):
        """Verify entity()."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_response = Mock(status_code=200, content=b'{:person/name "John" :db/id 123}')

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            result = db.entity(123)

            assert result == {":person/name": "John", ":db/id": 123}
            mock_client.request.assert_called_once_with(
                "GET",
                "http://localhost:3000/data/tdb/db/-/entity",
                headers={"Accept": "application/edn"},
                params={"e": 123},
                timeout=30.0,
            )

    def test_db_url(self):
        """Verify db_url construction."""
        conn = Datomic("http://localhost:3000/", "tdb")
        assert conn.db_url("mydb") == "http://localhost:3000/data/tdb/mydb"

    def test_database_delegation(self):
        """Verify Database delegates to Datomic connection."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("testdb", conn)

        mock_response = Mock(status_code=200, content=b"[[1]]")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            # When calling query on Database, it should delegate to conn.query with dbname
            db.query("[:find ?e :where [?e :test/attr]]")

            call_args = mock_client.request.call_args
            assert "testdb" in call_args[1]["params"]["args"]


class TestDatomicErrors:
    """Tests for error handling in Datomic client."""

    def test_create_database_failure(self):
        """Test create_database with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_response = Mock(status_code=500, text="Server error")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            with pytest.raises(DatomicClientError, match="Request failed with status 500"):
                conn.create_database("cms")

    def test_query_failure(self):
        """Test query with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_response = Mock(status_code=400, text="Bad request")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            with pytest.raises(DatomicClientError, match="Request failed with status 400"):
                conn.query("mydb", "invalid query")

    def test_transact_failure(self):
        """Test transact with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_response = Mock(status_code=500, text="Internal error")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            with pytest.raises(DatomicClientError, match="Request failed with status 500"):
                conn.transact("mydb", ["invalid"])

    def test_entity_failure(self):
        """Test entity with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_response = Mock(status_code=404, text="Not found")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            with pytest.raises(DatomicClientError, match="Request failed with status 404"):
                conn.entity("mydb", 123)


class TestDatomicTimeout:
    """Tests for timeout handling in Datomic client."""

    def test_default_timeout(self):
        """Test that default timeout is used."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_response = Mock(status_code=201)

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            conn.create_database("test")

            call_args = mock_client.request.call_args
            assert call_args[1]["timeout"] == 30.0

    def test_custom_timeout(self):
        """Test that custom timeout is used."""
        conn = Datomic("http://localhost:3000/", "tdb", timeout=60.0)

        mock_response = Mock(status_code=201)

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.return_value = mock_response
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            conn.create_database("test")

            call_args = mock_client.request.call_args
            assert call_args[1]["timeout"] == 60.0

    def test_timeout_error(self):
        """Test timeout error handling."""
        conn = Datomic("http://localhost:3000/", "tdb")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.side_effect = httpx.TimeoutException("Connection timed out")
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            with pytest.raises(DatomicConnectionError, match="timed out"):
                conn.create_database("test")


class TestDatomicConnectionErrors:
    """Tests for connection error handling in Datomic client."""

    def test_connection_error(self):
        """Test connection error handling."""
        conn = Datomic("http://localhost:3000/", "tdb")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.side_effect = httpx.ConnectError("Connection refused")
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            with pytest.raises(DatomicConnectionError, match="Failed to connect"):
                conn.create_database("test")

    def test_request_exception(self):
        """Test generic request exception handling."""
        conn = Datomic("http://localhost:3000/", "tdb")

        with patch("pydatomic.datomic.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.request.side_effect = httpx.HTTPError("Unknown error")
            mock_client.__enter__.return_value = mock_client
            mock_client.__exit__.return_value = None
            mock_client_class.return_value = mock_client

            with pytest.raises(DatomicClientError, match="Request to.*failed"):
                conn.create_database("test")
