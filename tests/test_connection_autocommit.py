import unittest
import warnings
import importlib

from sqlight.connection import Connection
from sqlight.platforms import Platform
from sqlight.err import Error, ProgrammingError, DatabaseError
from .config import MYSQL_CLIENT_URL, PYMYSQL_URL, POSTGRESQL_URL,\
        sqlite_test_table, mysql_test_table, postgresql_test_table


class TestConnectionAutoCommit(unittest.TestCase):

    def setUp(self):
        warnings.simplefilter("ignore")
        self.test_cons = []
        self.test_cons.append(
            Connection.create_from_dburl(
                "sqlite:///:memory:?autocommit=True"))

        try:
            importlib.import_module("MySQLdb.cursors")
        except ImportError:
            pass
        else:
            self.test_cons.append(Connection.create_from_dburl(
                MYSQL_CLIENT_URL + "?autocommit=True"))

        try:
            importlib.import_module("pymysql.cursors")
        except ImportError:
            pass
        else:
            self.test_cons.append(Connection.create_from_dburl(
                    PYMYSQL_URL + "?autocommit=True&connect_timeout=1"))
        try:
            importlib.import_module("psycopg2")
        except ImportError:
            pass
        else:
            self.test_cons.append(Connection.create_from_dburl(
                    POSTGRESQL_URL + "?autocommit=True"))

    def tearDown(self):
        for c in self.test_cons:
            c.connect()
            c.execute("drop table if exists test")

    def test_loop(self):
        for c in self.test_cons:
            self.t_connect(c)
            self.t_execute(c)
            self.t_execute_lastrowid(c)
            self.t_executemany(c)
            self.t_execute_rowcount(c)
            self.t_query(c)
            self.t_get(c)
            self.t_iter(c)
            self.t_rollback(c)
            self.t_commit(c)
            self.t_close(c)
            self.t_after_close(c)

    def t_connect(self, c):
        c.connect()

    def t_close(self, c):
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

    def t_execute_lastrowid(self, c):
        """插入数据"""
        name = "test1"
        c.execute_lastrowid("insert into test (name) values (%s)", name)
        row = c.get("select * from test where name = %s", name)
        self.assertEqual(row.id, 1)

    def t_executemany(self, c):
        """批量插入数据"""
        values = [
            ["test2"],
            ["test3"],
        ]
        count = c.executemany("insert into test (name) values (%s)", values)
        self.assertEqual(count, 2)
        last = c.get("select * from test where id = %s", 3)
        self.assertEqual(last.id, 3)
        self.assertEqual(last.name, "test3")

    def t_execute_rowcount(self, c):
        """更新数据"""
        count = c.execute_rowcount("update test set name = %s where id = %s",
                                   "test3_after", 3)
        self.assertEqual(count, 1)
        last = c.get("select * from test where id = %s", 3)
        self.assertEqual(last.id, 3)
        self.assertEqual(last.name, "test3_after")

    def t_query(self, c):
        """查询数据"""
        last = c.query("select * from test where id = %(id)s", id=3)
        self.assertEqual(len(last), 1)
        self.assertEqual(last[0].id, 3)
        self.assertEqual(last[0].name, "test3_after")
        self.assertEqual(
                c.get_last_executed(),
                "select * from test where id = 3")

    def t_get(self, c):
        """查询数据"""
        self.assertRaises(ProgrammingError,
                          c.get,
                          "select * from test where id > %(id)s",
                          id=1)
        with self.assertRaises(DatabaseError):
            c.get("select * from test2 where nn = 0")

    def t_iter(self, c):
        """查询数据"""
        iters = c.iter("select * from test where id > %(id)s", id=0)
        all_rows = []
        id = 0
        for i in iters:
            id += 1
            all_rows.append(i)
            self.assertEqual(i.id, id)
            if id == 3:
                self.assertEqual(i.name, "test3_after")
            else:
                self.assertEqual(i.name, "test%d" % id)
        self.assertEqual(len(all_rows), 3)

    def t_rollback(self, c):
        name = "test4"
        c.begin()
        c.execute_lastrowid("insert into test (name) values (%s)", name)
        c.rollback()
        row = c.get("select * from test where name = %s", name)
        self.assertEqual(row, None)

    def t_commit(self, c):
        c.begin()
        name = "test5"
        c.execute_lastrowid("insert into test (name) values (%(name)s)",
                            name=name)
        c.commit()
        row = c.get("select * from test where name = %s", name)
        self.assertIn(row.id, [4, 5])
        self.assertEqual(row.name, name)

    def t_after_close(self, c):
        with self.assertRaises(Error):
            c.get("select * from test")
