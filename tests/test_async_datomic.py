"""Tests for the Async Datomic REST client."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from datomic_py.async_datomic import AsyncDatabase, AsyncDatomic
from datomic_py.exceptions import DatomicClientError, DatomicConnectionError


@pytest.fixture
def mock_async_client():
    """Fixture to create a mock async client context manager."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    return mock_client


class TestAsyncDatomic:
    """Tests for AsyncDatomic client."""

    @pytest.mark.asyncio
    async def test_create_db(self, mock_async_client):
        """Verify create_database()."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_response = MagicMock(status_code=201)
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            db = await conn.create_database("cms")

            mock_async_client.request.assert_called_once_with(
                "POST",
                "http://localhost:3000/data/tdb/",
                data={"db-name": "cms"},
                timeout=30.0,
            )
            assert isinstance(db, AsyncDatabase)
            assert db.name == "cms"

    @pytest.mark.asyncio
    async def test_transact(self, mock_async_client):
        """Verify transact()."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")
        db = AsyncDatabase("db", conn)

        mock_response = MagicMock(
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
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            result = await db.transact('[{:db/id #db/id[:db.part/user] :person/name "Peter"}]')

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

    @pytest.mark.asyncio
    async def test_query(self, mock_async_client):
        """Verify query()."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")
        db = AsyncDatabase("db", conn)

        mock_response = MagicMock(status_code=200, content=b"[[17592186048482]]")
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            result = await db.query("[:find ?e ?n :where [?e :person/name ?n]]")

            assert result == ((17592186048482,),)
            mock_async_client.request.assert_called_once_with(
                "GET",
                "http://localhost:3000/api/query",
                headers={"Accept": "application/edn"},
                params={
                    "q": "[:find ?e ?n :where [?e :person/name ?n]]",
                    "args": "[{:db/alias tdb/db} ]",
                },
                timeout=30.0,
            )

    @pytest.mark.asyncio
    async def test_query_with_history(self, mock_async_client):
        """Verify query() with history flag."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")
        db = AsyncDatabase("db", conn)

        mock_response = MagicMock(status_code=200, content=b'[["value"]]')
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            result = await db.query("[:find ?n :where [?e :person/name ?n]]", history=True)

            assert result == (("value",),)
            call_args = mock_async_client.request.call_args
            assert ":history true" in call_args[1]["params"]["args"]

    @pytest.mark.asyncio
    async def test_query_with_extra_args(self, mock_async_client):
        """Verify query() with extra arguments."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")
        db = AsyncDatabase("db", conn)

        mock_response = MagicMock(status_code=200, content=b'[["result"]]')
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            result = await db.query(
                "[:find ?n :in $ ?e :where [?e :person/name ?n]]", extra_args=[123]
            )

            assert result == (("result",),)
            call_args = mock_async_client.request.call_args
            assert "123" in call_args[1]["params"]["args"]

    @pytest.mark.asyncio
    async def test_entity(self, mock_async_client):
        """Verify entity()."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")
        db = AsyncDatabase("db", conn)

        mock_response = MagicMock(status_code=200, content=b'{:person/name "John" :db/id 123}')
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            result = await db.entity(123)

            assert result == {":person/name": "John", ":db/id": 123}
            mock_async_client.request.assert_called_once_with(
                "GET",
                "http://localhost:3000/data/tdb/db/-/entity",
                headers={"Accept": "application/edn"},
                params={"e": 123},
                timeout=30.0,
            )

    def test_db_url(self):
        """Verify db_url construction."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")
        assert conn.db_url("mydb") == "http://localhost:3000/data/tdb/mydb"

    @pytest.mark.asyncio
    async def test_database_delegation(self, mock_async_client):
        """Verify AsyncDatabase delegates to AsyncDatomic connection."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")
        db = AsyncDatabase("testdb", conn)

        mock_response = MagicMock(status_code=200, content=b"[[1]]")
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            # When calling query on AsyncDatabase, it should delegate to conn.query with dbname
            await db.query("[:find ?e :where [?e :test/attr]]")

            call_args = mock_async_client.request.call_args
            assert "testdb" in call_args[1]["params"]["args"]


class TestAsyncDatomicErrors:
    """Tests for error handling in AsyncDatomic client."""

    @pytest.mark.asyncio
    async def test_create_database_failure(self, mock_async_client):
        """Test create_database with error response."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_response = MagicMock(status_code=500, text="Server error")
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            with pytest.raises(DatomicClientError, match="Request failed with status 500"):
                await conn.create_database("cms")

    @pytest.mark.asyncio
    async def test_query_failure(self, mock_async_client):
        """Test query with error response."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_response = MagicMock(status_code=400, text="Bad request")
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            with pytest.raises(DatomicClientError, match="Request failed with status 400"):
                await conn.query("mydb", "invalid query")

    @pytest.mark.asyncio
    async def test_transact_failure(self, mock_async_client):
        """Test transact with error response."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_response = MagicMock(status_code=500, text="Internal error")
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            with pytest.raises(DatomicClientError, match="Request failed with status 500"):
                await conn.transact("mydb", ["invalid"])

    @pytest.mark.asyncio
    async def test_entity_failure(self, mock_async_client):
        """Test entity with error response."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_response = MagicMock(status_code=404, text="Not found")
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            with pytest.raises(DatomicClientError, match="Request failed with status 404"):
                await conn.entity("mydb", 123)


class TestAsyncDatomicTimeout:
    """Tests for timeout handling in AsyncDatomic client."""

    @pytest.mark.asyncio
    async def test_default_timeout(self, mock_async_client):
        """Test that default timeout is used."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_response = MagicMock(status_code=201)
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            await conn.create_database("test")

            call_args = mock_async_client.request.call_args
            assert call_args[1]["timeout"] == 30.0

    @pytest.mark.asyncio
    async def test_custom_timeout(self, mock_async_client):
        """Test that custom timeout is used."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb", timeout=60.0)

        mock_response = MagicMock(status_code=201)
        mock_async_client.request.return_value = mock_response

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_async_client):
            await conn.create_database("test")

            call_args = mock_async_client.request.call_args
            assert call_args[1]["timeout"] == 60.0

    @pytest.mark.asyncio
    async def test_timeout_error(self):
        """Test timeout error handling."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_client = MagicMock()
        mock_client.request = AsyncMock(side_effect=httpx.TimeoutException("Connection timed out"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(DatomicConnectionError, match="timed out"):
                await conn.create_database("test")


class TestAsyncDatomicConnectionErrors:
    """Tests for connection error handling in AsyncDatomic client."""

    @pytest.mark.asyncio
    async def test_connection_error(self):
        """Test connection error handling."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_client = MagicMock()
        mock_client.request = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(DatomicConnectionError, match="Failed to connect"):
                await conn.create_database("test")

    @pytest.mark.asyncio
    async def test_request_exception(self):
        """Test generic request exception handling."""
        conn = AsyncDatomic("http://localhost:3000/", "tdb")

        mock_client = MagicMock()
        mock_client.request = AsyncMock(side_effect=httpx.HTTPError("Unknown error"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch("datomic_py.async_datomic.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(DatomicClientError, match="Request to.*failed"):
                await conn.create_database("test")
