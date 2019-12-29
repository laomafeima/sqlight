import unittest
import warnings
import importlib

from sqlight.connection import Connection
from sqlight.platforms import Platform
from sqlight.err import Error
from .config import MYSQL_CLIENT_URL, PYMYSQL_URL, POSTGRESQL_URL,\
        sqlite_test_table, mysql_test_table, postgresql_test_table


class TestConnection(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore")
        self.test_cons = []
        self.test_cons.append(
            Connection.create_from_dburl(
                "sqlite:///:memory:?isolation_level=DEFERRED"))

        try:
            importlib.import_module("MySQLdb.cursors")
        except ImportError:
            pass
        else:
            self.test_cons.append(Connection.create_from_dburl(
                MYSQL_CLIENT_URL + "?autocommit=False"))

        try:
            importlib.import_module("pymysql.cursors")
        except ImportError:
            pass
        else:
            self.test_cons.append(Connection.create_from_dburl(
                    PYMYSQL_URL + "?autocommit=False&connect_timeout=1"))
        try:
            importlib.import_module("psycopg2")
        except ImportError:
            pass
        else:
            self.test_cons.append(Connection.create_from_dburl(
                    POSTGRESQL_URL + "?autocommit=False"))

    def tearDown(self):
        for c in self.test_cons:
            c.connect()
            c.execute("drop table if exists test")
            c.commit()

    def test_loop(self):
        for c in self.test_cons:
            self.t_connect(c)
            self.t_execute(c)
            self.t_rollback(c)
            self.t_commit(c)
            self.t_close_rollback(c)
            self.t_close(c)

    def t_connect(self, c):
        c.connect()

    def t_close(self, c):
        c.connect()
        if c.dburl.platform is not Platform.SQLite:
            c.execute("drop table test")
        c.close()

    def t_execute(self, c):
        """创建表"""
        if c.dburl.platform is Platform.MySQL:
            c.execute(mysql_test_table)
        elif c.dburl.platform is Platform.SQLite:
            c.execute(sqlite_test_table)
        elif c.dburl.platform is Platform.PostgreSQL:
            c.execute(postgresql_test_table)
        c.commit()

    def t_rollback(self, c):
        if c.dburl.platform is Platform.PostgreSQL:  # TODO why don't begin?
            c.begin()
        name = "test1"
        c.execute_lastrowid("insert into test (name) values (%s)", name)
        c.rollback()
        row = c.get("select * from test where name = %s", name)
        self.assertEqual(row, None)

    def t_commit(self, c):
        name = "test2"
        c.execute_lastrowid("insert into test (name) values (%s)", name)
        c.commit()
        row = c.get("select * from test where name = %s", name)
        self.assertIn(row.id, [1, 2])
        self.assertEqual(row.name, name)

    def t_close_rollback(self, c):
        name = "test3"
        c.execute_lastrowid("insert into test (name) values (%s)", name)
        c.close()
        with self.assertRaises(Error):
            c.get("select * from test where name = %s", name)
