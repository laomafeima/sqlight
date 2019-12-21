import unittest
import warnings
import importlib

from sqlight.connection import Connection
from sqlight.platforms import Platform
from sqlight.err import Error
from .config import MYSQL_CLIENT_URL, PYMYSQL_URL, sqlite_test_table,\
                    mysql_test_table


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

    def tearDown(self):
        for c in self.test_cons:
            c.connect()
            c.execute("drop table if exists test")

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

    def t_rollback(self, c):
        id = c.execute_lastrowid("insert into test (name) values ('test1')")
        self.assertEqual(id, 1)
        c.rollback()
        row = c.get("select * from test where id = %s", id)
        self.assertEqual(row, None)

    def t_commit(self, c):
        id = c.execute_lastrowid("insert into test (name) values ('test2')")
        self.assertIn(id, [1, 2])
        c.commit()
        row = c.get("select * from test where id = %s", id)
        self.assertEqual(row.id, id)
        self.assertEqual(row.name, "test2")

    def t_close_rollback(self, c):
        id = c.execute_lastrowid("insert into test (name) values ('test3')")
        self.assertIn(id, [2, 3])
        c.close()
        with self.assertRaises(Error):
            c.get("select * from test where id = %s", id)
