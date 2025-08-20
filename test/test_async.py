import asyncio
import decimal
import json
import logging
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import aurora_data_api.async_ as async_  # noqa
import aurora_data_api.exceptions as exceptions  # noqa
from aurora_data_api.error_codes_mysql import MySQLErrorCodes  # noqa
from aurora_data_api.error_codes_postgresql import PostgreSQLErrorCodes  # noqa
from base import BaseAuroraDataAPITest, CoreAuroraDataAPITest, PEP249ConformanceTestMixin  # noqa

logging.basicConfig(level=logging.INFO)
logging.getLogger("aurora_data_api").setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)


class AsyncTestCase(unittest.TestCase):
    """Base class for async test cases."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def async_test(coro):
        """Decorator to run async test methods."""

        def wrapper(self):
            return self.loop.run_until_complete(coro(self))

        return wrapper


class TestAuroraDataAPI(BaseAuroraDataAPITest, AsyncTestCase):
    """Asynchronous Aurora Data API tests"""

    def get_connect_kwargs(self, skip_begin_transaction=None):
        """Get connection kwargs with optional skip_begin_transaction parameter"""
        kwargs = {
            'database': self.db_name,
            'aurora_cluster_arn': self.cluster_arn,
            'secret_arn': self.secret_arn
        }
        if skip_begin_transaction is not None:
            kwargs['skip_begin_transaction'] = skip_begin_transaction
        return kwargs

    async def get_connection(self, skip_begin_transaction=None):
        """Get an async connection with optional skip_begin_transaction parameter"""
        return await async_.connect(**self.get_connect_kwargs(skip_begin_transaction))

    @classmethod
    def _setup_test_data(cls):
        """Set up test data for async tests"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(cls._async_setup_test_data())
        finally:
            loop.close()

    @classmethod
    async def _async_setup_test_data(cls):
        """Async helper for setting up test data"""
        conn = await async_.connect(
            database=cls.db_name, aurora_cluster_arn=cls.cluster_arn, secret_arn=cls.secret_arn
        )
        async with conn:
            cur = await conn.cursor()
            try:
                await cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                await cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")
                await cur.execute(cls().get_postgresql_create_table_sql())
                await cur.executemany(
                    cls().get_postgresql_insert_sql(),
                    cls().get_test_data(),
                )
            except exceptions.MySQLError.ER_PARSE_ERROR:
                cls.using_mysql = True
                await cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")
                await cur.execute(cls().get_mysql_create_table_sql())
                await cur.executemany(
                    cls().get_mysql_insert_sql(),
                    cls().get_test_data(),
                )

    @classmethod
    def _teardown_test_data(cls):
        """Tear down test data for async tests"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(cls._async_teardown_test_data())
        finally:
            loop.close()

    @classmethod
    async def _async_teardown_test_data(cls):
        """Async helper for tearing down test data"""
        conn = await async_.connect(
            database=cls.db_name, aurora_cluster_arn=cls.cluster_arn, secret_arn=cls.secret_arn
        )
        async with conn:
            cur = await conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS aurora_data_api_test")

    @AsyncTestCase.async_test
    async def test_invalid_statements(self):
        """Test invalid statements with both skip_begin_transaction values"""
        for skip_begin_transaction in [True, False]:
            with self.subTest(skip_begin_transaction=skip_begin_transaction):
                conn = await self.get_connection(skip_begin_transaction)
                async with conn:
                    cur = await conn.cursor()
                    with self.assertRaises((exceptions.PostgreSQLError.ER_SYNTAX_ERR, exceptions.MySQLError.ER_PARSE_ERROR)):
                        await cur.execute("selec * from table")

    @AsyncTestCase.async_test
    async def test_iterators(self):
        """Test cursor iteration functionality with both skip_begin_transaction values"""
        for skip_begin_transaction in [True, False]:
            with self.subTest(skip_begin_transaction=skip_begin_transaction):
                conn = await self.get_connection(skip_begin_transaction)
                async with conn:
                    cur = await conn.cursor()
                    if not self.using_mysql:
                        cur = await conn.cursor()
                        await cur.execute(
                            "select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**6)
                        )
                        result = await cur.fetchone()
                        self.assertEqual(result[0], 0)

                        cur = await conn.cursor()
                        await cur.execute(
                            "select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**7)
                        )
                        result = await cur.fetchone()
                        self.assertEqual(result[0], 1977)

                        cur = await conn.cursor()
                        await cur.execute(
                            "select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**8)
                        )
                        result = await cur.fetchone()
                        self.assertEqual(result[0], 2048)

                        cur = await conn.cursor()
                        await cur.execute(
                            "select count(*) from aurora_data_api_test where pg_column_size(doc) < :s", dict(s=2**10)
                        )
                        result = await cur.fetchone()
                        self.assertEqual(result[0], 2048)

                    cur = await conn.cursor()
                    expect_row0 = self.get_expected_row0()
                    i = 0
                    await cur.execute("select * from aurora_data_api_test")
                    async for f in cur:
                        if i == 0:
                            self.assertEqual(f, expect_row0)
                        i += 1
                    self.assertEqual(i, 2048)

                    cur = await conn.cursor()
                    await cur.execute("select * from aurora_data_api_test")
                    data = await cur.fetchall()
                    self.assertEqual(data[0], expect_row0)
                    self.assertEqual(data[-1][0], 2048)
                    self.assertEqual(data[-1][1], "row2047")
                    if not self.using_mysql:
                        self.assertEqual(json.loads(data[-1][2]), {"x": 2047, "y": str(2047), "z": [2047, 2047 * 2047, 0]})
                    self.assertEqual(data[-1][-2], decimal.Decimal("2047.2047"))
                    self.assertEqual(len(data), 2048)
                    self.assertEqual(len(await cur.fetchall()), 0)

                    cur = await conn.cursor()
                    await cur.execute("select * from aurora_data_api_test")
                    i = 0
                    while True:
                        result = await cur.fetchone()
                        if not result:
                            break
                        i += 1
                    self.assertEqual(i, 2048)

                    cur = await conn.cursor()
                    await cur.execute("select * from aurora_data_api_test")
                    while True:
                        fm = await cur.fetchmany(1001)
                        if not fm:
                            break
                        self.assertIn(len(fm), [1001, 46])

    @unittest.skip(
        "This test now fails because the API was changed to terminate and delete the transaction when the "
        "data returned by the statement exceeds the limit, making automated recovery impossible."
    )
    @AsyncTestCase.async_test
    async def test_pagination_backoff(self):
        if self.using_mysql:
            return
        async with async_.connect(
            database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn
        ) as conn:
            cur = await conn.cursor()
            sql_template = "select concat({}) from aurora_data_api_test"
            sql = sql_template.format(", ".join(["cast(doc as text)"] * 64))
            await cur.execute(sql)
            result = await cur.fetchall()
            self.assertEqual(len(result), 2048)

            concat_args = ", ".join(["cast(doc as text)"] * 100)
            sql = sql_template.format(", ".join("concat({})".format(concat_args) for i in range(32)))
            await cur.execute(sql)
            with self.assertRaisesRegex(Exception, "Database response exceeded size limit"):
                await cur.fetchall()

    @AsyncTestCase.async_test
    async def test_postgres_exceptions(self):
        """Test PostgreSQL-specific exceptions with both skip_begin_transaction values"""
        for skip_begin_transaction in [True, False]:
            with self.subTest(skip_begin_transaction=skip_begin_transaction):
                if self.using_mysql:
                    return
                conn = await self.get_connection(skip_begin_transaction)
                async with conn:
                    cur = await conn.cursor()
                    table = "aurora_data_api_nonexistent_test_table"
                    with self.assertRaises(exceptions.PostgreSQLError.ER_UNDEF_TABLE) as e:
                        sql = f"select * from {table}"
                        await cur.execute(sql)
                    self.assertTrue(f'relation "{table}" does not exist' in str(e.exception))
                    self.assertTrue(isinstance(e.exception.response, dict))

    @AsyncTestCase.async_test
    async def test_rowcount(self):
        """Test rowcount functionality with both skip_begin_transaction values"""
        for skip_begin_transaction in [True, False]:
            with self.subTest(skip_begin_transaction=skip_begin_transaction):
                conn = await self.get_connection(skip_begin_transaction)
                async with conn:
                    cur = await conn.cursor()
                    await cur.execute("select * from aurora_data_api_test limit 8")
                    self.assertEqual(cur.rowcount, 8)

                conn = await self.get_connection(skip_begin_transaction)
                async with conn:
                    cur = await conn.cursor()
                    await cur.execute("select * from aurora_data_api_test limit 9000")
                    self.assertEqual(cur.rowcount, 2048)

                if self.using_mysql:
                    return

                conn = await self.get_connection(skip_begin_transaction)
                async with conn:
                    cur = await conn.cursor()
                    await cur.executemany(
                        "INSERT INTO aurora_data_api_test(name, doc) VALUES (:name, CAST(:doc AS JSONB))",
                        [
                            {
                                "name": "rowcount{}".format(i),
                                "doc": json.dumps({"x": i, "y": str(i), "z": [i, i * i, i**i if i < 512 else 0]}),
                            }
                            for i in range(8)
                        ],
                    )

                    await cur.execute("UPDATE aurora_data_api_test SET doc = '{}' WHERE name like 'rowcount%'")
                    self.assertEqual(cur.rowcount, 8)

                    await cur.execute("DELETE FROM aurora_data_api_test WHERE name like 'rowcount%'")
                    self.assertEqual(cur.rowcount, 8)

    @AsyncTestCase.async_test
    async def test_continue_after_timeout(self):
        """Test continue after timeout functionality with both skip_begin_transaction values"""
        if os.environ.get("TEST_CONTINUE_AFTER_TIMEOUT", "False") != "True":
            self.skipTest("TEST_CONTINUE_AFTER_TIMEOUT env var is not 'True'")

        if self.using_mysql:
            self.skipTest("Not implemented for MySQL")

        for skip_begin_transaction in [True, False]:
            with self.subTest(skip_begin_transaction=skip_begin_transaction):
                try:
                    conn = await self.get_connection(skip_begin_transaction)
                    async with conn:
                        cur = await conn.cursor()
                        with self.assertRaisesRegex(Exception, "StatementTimeoutException"):
                            await cur.execute(
                                (
                                    "INSERT INTO aurora_data_api_test(name) SELECT 'continue_after_timeout'"
                                    "FROM (SELECT pg_sleep(50)) q"
                                )
                            )
                        with self.assertRaisesRegex(async_.DatabaseError, "current transaction is aborted"):
                            await cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")

                    conn = await self.get_connection(skip_begin_transaction)
                    async with conn:
                        cur = await conn.cursor()
                        await cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")
                        result = await cur.fetchone()
                        self.assertEqual(result, (0,))

                    continue_kwargs = self.get_connect_kwargs(skip_begin_transaction).copy()
                    continue_kwargs['continue_after_timeout'] = True
                    conn = await async_.connect(**continue_kwargs)
                    async with conn:
                        cur = await conn.cursor()
                        with self.assertRaisesRegex(Exception, "StatementTimeoutException"):
                            await cur.execute(
                                (
                                    "INSERT INTO aurora_data_api_test(name) SELECT 'continue_after_timeout' "
                                    "FROM (SELECT pg_sleep(50)) q"
                                )
                            )
                        await cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")
                        result = await cur.fetchone()
                        self.assertEqual(result, (1,))
                finally:
                    conn = await self.get_connection(skip_begin_transaction)
                    async with conn:
                        cur = await conn.cursor()
                        await cur.execute("DELETE FROM aurora_data_api_test WHERE name = 'continue_after_timeout'")

    @AsyncTestCase.async_test
    async def test_skip_begin_transaction_behavior(self):
        """Test that skip_begin_transaction actually controls transaction behavior"""
        if self.using_mysql:
            self.skipTest("PostgreSQL-specific transaction behavior test")
        
        # Test with skip_begin_transaction=False (default behavior - should use transactions)
        with self.subTest(skip_begin_transaction=False):
            conn = await self.get_connection(skip_begin_transaction=False)
            async with conn:
                cur = await conn.cursor()
                
                # Insert a test record
                await cur.execute(
                    "INSERT INTO aurora_data_api_test(name, doc) VALUES (:name, CAST(:doc AS JSONB))",
                    {"name": "transaction_test_false", "doc": '{"test": "skip_false"}'}
                )
                
                # Check if we can see the record in the same transaction
                await cur.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'transaction_test_false'")
                count = await cur.fetchone()
                self.assertEqual(count[0], 1, "Record should be visible within the same transaction")
                
                # The record should be committed when the connection closes
            
            # Verify the record was committed
            conn2 = await self.get_connection(skip_begin_transaction=False)
            async with conn2:
                cur2 = await conn2.cursor()
                await cur2.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'transaction_test_false'")
                count = await cur2.fetchone()
                self.assertEqual(count[0], 1, "Record should be committed after transaction")
                
                # Clean up
                await cur2.execute("DELETE FROM aurora_data_api_test WHERE name = 'transaction_test_false'")
        
        # Test with skip_begin_transaction=True (no automatic transactions)
        with self.subTest(skip_begin_transaction=True):
            conn = await self.get_connection(skip_begin_transaction=True)
            async with conn:
                cur = await conn.cursor()
                
                # Insert a test record (should auto-commit immediately)
                await cur.execute(
                    "INSERT INTO aurora_data_api_test(name, doc) VALUES (:name, CAST(:doc AS JSONB))",
                    {"name": "transaction_test_true", "doc": '{"test": "skip_true"}'}
                )
                
                # Record should be immediately visible from another connection
                # because each statement auto-commits when skip_begin_transaction=True
            
            # Verify the record is immediately available (auto-committed)
            conn2 = await self.get_connection(skip_begin_transaction=True)
            async with conn2:
                cur2 = await conn2.cursor()
                await cur2.execute("SELECT COUNT(*) FROM aurora_data_api_test WHERE name = 'transaction_test_true'")
                count = await cur2.fetchone()
                self.assertEqual(count[0], 1, "Record should be auto-committed immediately with skip_begin_transaction=True")
                
                # Clean up
                await cur2.execute("DELETE FROM aurora_data_api_test WHERE name = 'transaction_test_true'")


class TestAuroraDataAPIConformance(PEP249ConformanceTestMixin, CoreAuroraDataAPITest):
    """Conformance test class for asynchronous tests. Sets up connection to run tests."""

    driver = async_
    connection = None

    def setUp(self):
        """Setup mock objects for connection and cursor as member variables."""
        self.connection = self.driver.connect(
            database=self.db_name, aurora_cluster_arn=self.cluster_arn, secret_arn=self.secret_arn
        )

    async def test_cursor_interface(self):
        """[Cursor] Runs formal interface checks on the cursor."""
        cur = await self.connection.cursor()
        self._assert_cursor_interface(cur)


# Remove these classes to avoid instantiation errors
del AsyncTestCase
del CoreAuroraDataAPITest
del BaseAuroraDataAPITest

if __name__ == "__main__":
    unittest.main()
