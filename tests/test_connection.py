import unittest

from sqlight.connection import Connection
from sqlight.platforms import Platform


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
        self.sqlite_autocommit = Connection.create_from_dburl(
                "sqlite:///:memory:?isolation_level=None")

        self.pymysql_autocommit = Connection.create_from_dburl(
                "mysql+pymysql://root:123456@127.0.0.1:3306/test?autocommit=True&connect_timeout=1")

        self.mysqlclient_autocommit = Connection.create_from_dburl(
                "mysql+mysqlclient://root:123456@127.0.0.1:3306/test?autocommit=True")

    def tearDown(self):
        pass

    def test_loop(self):
        for c in [self.sqlite_autocommit,
                  self.pymysql_autocommit,
                  self.mysqlclient_autocommit]:
            self.t_connect(c)
            self.t_execute(c)
            self.t_execute_lastrowid(c)
            self.t_executemany(c)
            self.t_execute_rowcount(c)
            self.t_query(c)
            self.t_iter(c)
            self.t_close(c)

    def t_connect(self, c):
        c.connect()

    def t_close(self, c):
        c.execute("drop table test")
        c.close()

    def t_execute(self, c):
        """创建表"""
        if c.dburl.platform is Platform.MySQL:
            c.execute(self.mysql_test_table)
        elif c.dburl.platform is Platform.SQLite:
            c.execute(self.sqlite_test_table)

    def t_execute_lastrowid(self, c):
        """插入数据"""
        id = c.execute_lastrowid("insert into test (name) values ('test1')")
        self.assertEqual(id, 1)

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
        count = c.execute_rowcount("update test set name = %s where id = %s", "test3_after", 3)
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



