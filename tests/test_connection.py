import unittest
import warnings

from sqlight.connection import Connection
from sqlight.platforms import Platform
from sqlight.err import Error, ProgrammingError, DatabaseError


class TestConnection(unittest.TestCase):
    sqlite_test_table = """
        CREATE TABLE test(
            id INTEGER  PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
    """
    mysql_test_table = """
        CREATE TABLE test(
            id int(11) PRIMARY KEY AUTO_INCREMENT,
            name char(64)
        )
    """

    def setUp(self):
        warnings.simplefilter("ignore")

        self.sqlite = Connection.create_from_dburl(
            "sqlite:///:memory:?isolation_level=DEFERRED")

        self.pymysql = Connection.create_from_dburl(
            "mysql+pymysql://root:123456@127.0.0.1:3306/test" +
            "?autocommit=False&connect_timeout=1")

        self.mysqlclient = Connection.create_from_dburl(
            "mysql+mysqlclient://root:123456@127.0.0.1:3306/test" +
            "?autocommit=False")

    def tearDown(self):
        pass

    def test_loop(self):
        for c in [self.sqlite, self.pymysql, self.mysqlclient]:
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
            c.execute(self.mysql_test_table)
        elif c.dburl.platform is Platform.SQLite:
            c.execute(self.sqlite_test_table)

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
