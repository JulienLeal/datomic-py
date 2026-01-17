"""Tests for the Datomic REST client."""

from datetime import datetime
from unittest.mock import Mock, call

import pytest
import requests

from pydatomic.datomic import Database, Datomic
from pydatomic.exceptions import DatomicClientError, DatomicConnectionError


class TestDatomic:
    """Tests for Datomic client."""

    def test_create_db(self, mock_requests):
        """Verify create_database()."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_requests.post.return_value = Mock(status_code=201)
        db = conn.create_database("cms")

        assert mock_requests.post.mock_calls == [
            call("http://localhost:3000/data/tdb/", data={"db-name": "cms"}, timeout=30.0)
        ]
        assert isinstance(db, Database)
        assert db.name == "cms"

    def test_transact(self, mock_requests):
        """Verify transact()."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_requests.post.return_value = Mock(
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

    def test_query(self, mock_requests):
        """Verify query()."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_requests.get.return_value = Mock(status_code=200, content=b"[[17592186048482]]")

        result = db.query("[:find ?e ?n :where [?e :person/name ?n]]")

        assert result == ((17592186048482,),)
        assert mock_requests.get.mock_calls == [
            call(
                "http://localhost:3000/api/query",
                headers={"Accept": "application/edn"},
                params={
                    "q": "[:find ?e ?n :where [?e :person/name ?n]]",
                    "args": "[{:db/alias tdb/db} ]",
                },
                timeout=30.0,
            )
        ]

    def test_query_with_history(self, mock_requests):
        """Verify query() with history flag."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_requests.get.return_value = Mock(status_code=200, content=b'[["value"]]')

        result = db.query("[:find ?n :where [?e :person/name ?n]]", history=True)

        assert result == (("value",),)
        call_args = mock_requests.get.call_args
        assert ":history true" in call_args[1]["params"]["args"]

    def test_query_with_extra_args(self, mock_requests):
        """Verify query() with extra arguments."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_requests.get.return_value = Mock(status_code=200, content=b'[["result"]]')

        result = db.query("[:find ?n :in $ ?e :where [?e :person/name ?n]]", extra_args=[123])

        assert result == (("result",),)
        call_args = mock_requests.get.call_args
        assert "123" in call_args[1]["params"]["args"]

    def test_entity(self, mock_requests):
        """Verify entity()."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("db", conn)

        mock_requests.get.return_value = Mock(
            status_code=200, content=b'{:person/name "John" :db/id 123}'
        )

        result = db.entity(123)

        assert result == {":person/name": "John", ":db/id": 123}
        assert mock_requests.get.mock_calls == [
            call(
                "http://localhost:3000/data/tdb/db/-/entity",
                headers={"Accept": "application/edn"},
                params={"e": 123},
                timeout=30.0,
            )
        ]

    def test_db_url(self):
        """Verify db_url construction."""
        conn = Datomic("http://localhost:3000/", "tdb")
        assert conn.db_url("mydb") == "http://localhost:3000/data/tdb/mydb"

    def test_database_delegation(self, mock_requests):
        """Verify Database delegates to Datomic connection."""
        conn = Datomic("http://localhost:3000/", "tdb")
        db = Database("testdb", conn)

        mock_requests.get.return_value = Mock(status_code=200, content=b"[[1]]")

        # When calling query on Database, it should delegate to conn.query with dbname
        db.query("[:find ?e :where [?e :test/attr]]")

        call_args = mock_requests.get.call_args
        assert "testdb" in call_args[1]["params"]["args"]


class TestDatomicErrors:
    """Tests for error handling in Datomic client."""

    def test_create_database_failure(self, mock_requests):
        """Test create_database with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_requests.post.return_value = Mock(status_code=500, text="Server error")

        with pytest.raises(DatomicClientError, match="Request failed with status 500"):
            conn.create_database("cms")

    def test_query_failure(self, mock_requests):
        """Test query with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_requests.get.return_value = Mock(status_code=400, text="Bad request")

        with pytest.raises(DatomicClientError, match="Request failed with status 400"):
            conn.query("mydb", "invalid query")

    def test_transact_failure(self, mock_requests):
        """Test transact with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_requests.post.return_value = Mock(status_code=500, text="Internal error")

        with pytest.raises(DatomicClientError, match="Request failed with status 500"):
            conn.transact("mydb", ["invalid"])

    def test_entity_failure(self, mock_requests):
        """Test entity with error response."""
        conn = Datomic("http://localhost:3000/", "tdb")

        mock_requests.get.return_value = Mock(status_code=404, text="Not found")

        with pytest.raises(DatomicClientError, match="Request failed with status 404"):
            conn.entity("mydb", 123)


class TestDatomicTimeout:
    """Tests for timeout handling in Datomic client."""

    def test_default_timeout(self, mock_requests_with_exceptions):
        """Test that default timeout is used."""
        conn = Datomic("http://localhost:3000/", "tdb")
        mock_requests_with_exceptions.post.return_value = Mock(status_code=201)

        conn.create_database("test")

        call_args = mock_requests_with_exceptions.post.call_args
        assert call_args[1]["timeout"] == 30.0

    def test_custom_timeout(self, mock_requests_with_exceptions):
        """Test that custom timeout is used."""
        conn = Datomic("http://localhost:3000/", "tdb", timeout=60.0)
        mock_requests_with_exceptions.post.return_value = Mock(status_code=201)

        conn.create_database("test")

        call_args = mock_requests_with_exceptions.post.call_args
        assert call_args[1]["timeout"] == 60.0

    def test_timeout_error(self, mock_requests_with_exceptions):
        """Test timeout error handling."""
        conn = Datomic("http://localhost:3000/", "tdb")
        mock_requests_with_exceptions.post.side_effect = requests.exceptions.Timeout(
            "Connection timed out"
        )

        with pytest.raises(DatomicConnectionError, match="timed out"):
            conn.create_database("test")


class TestDatomicConnectionErrors:
    """Tests for connection error handling in Datomic client."""

    def test_connection_error(self, mock_requests_with_exceptions):
        """Test connection error handling."""
        conn = Datomic("http://localhost:3000/", "tdb")
        mock_requests_with_exceptions.post.side_effect = requests.exceptions.ConnectionError(
            "Connection refused"
        )

        with pytest.raises(DatomicConnectionError, match="Failed to connect"):
            conn.create_database("test")

    def test_request_exception(self, mock_requests_with_exceptions):
        """Test generic request exception handling."""
        conn = Datomic("http://localhost:3000/", "tdb")
        mock_requests_with_exceptions.post.side_effect = requests.exceptions.RequestException(
            "Unknown error"
        )

        with pytest.raises(DatomicClientError, match="Request to.*failed"):
            conn.create_database("test")
