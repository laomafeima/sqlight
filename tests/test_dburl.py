import unittest

from sqlight.dburl import DBUrl
from sqlight.platforms import Platform, Driver
from sqlight.err import NotSupportedError


class TestDBUrl(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_tiny(self):
        dburl = DBUrl.get_from_url("mysql://scott:tiger@localhost:3306/foo")
        self.assertEqual(dburl.platform, Platform.MySQL)
        self.assertEqual(dburl.driver, Driver.PYMYSQL)
        self.assertEqual(dburl.username, "scott")
        self.assertEqual(dburl.password, "tiger")
        self.assertEqual(dburl.hostname, "localhost")
        self.assertEqual(dburl.port, 3306)
        self.assertEqual(dburl.database, "foo")
        self.assertEqual(dburl.args, None)
        self.assertEqual(dburl.get_args(), {
                'host': 'localhost',
                'port': 3306,
                'user': "scott",
                "password": "tiger",
                "database": "foo",
            })

    def test_driver(self):
        dburl = DBUrl.get_from_url(
            "mysql+mysqlclient://scott:tiger@localhost:3306/foo")
        self.assertEqual(dburl.platform, Platform.MySQL)
        self.assertEqual(dburl.driver, Driver.MYSQLCLIENT)
        self.assertEqual(dburl.username, "scott")
        self.assertEqual(dburl.args, None)
        self.assertEqual(dburl.get_args(), {
                'host': 'localhost',
                'port': 3306,
                'user': "scott",
                "passwd": "tiger",
                "db": "foo",
            })

    def test_with_args(self):
        dburl = DBUrl.get_from_url(
            "mysql+mysqlclient://scott:tiger@localhost:3306/foo?" +
            "autocommit=False&isolation_level=DEFERRED&connect_timeout=10")
        self.assertEqual(dburl.platform, Platform.MySQL)
        self.assertEqual(dburl.driver, Driver.MYSQLCLIENT)
        self.assertEqual(dburl.username, "scott")
        self.assertEqual(dburl.args, {
            "autocommit": False,
            "isolation_level": "DEFERRED",
            "connect_timeout": 10,
        })
        self.assertEqual(dburl.get_args(), {
                'host': 'localhost',
                'port': 3306,
                'user': "scott",
                "passwd": "tiger",
                "db": "foo",
                "autocommit": False,
                "isolation_level": "DEFERRED",
                "connect_timeout": 10,
            })

    def test_postgresql(self):
        dburl = DBUrl.get_from_url(
            "postgresql://scott:tiger@localhost:5432/foo?" +
            "autocommit=False&isolation_level=DEFERRED&connect_timeout=10")
        self.assertEqual(dburl.platform, Platform.PostgreSQL)
        self.assertEqual(dburl.driver, Driver.PSYCOPG)
        self.assertEqual(dburl.username, "scott")
        self.assertEqual(dburl.args, {
            "autocommit": False,
            "isolation_level": "DEFERRED",
            "connect_timeout": 10,
        })
        self.assertEqual(dburl.get_args(), {
                'host': 'localhost',
                'port': 5432,
                'user': "scott",
                "password": "tiger",
                "database": "foo",
                "autocommit": False,
                "isolation_level": "DEFERRED",
                "connect_timeout": 10,
            })

    def test_sqlite(self):
        dburl = DBUrl.get_from_url(
            "sqlite:///~/foo.db?autocommit=False&isolation_level=DEFERRED")
        self.assertEqual(dburl.platform, Platform.SQLite)
        self.assertEqual(dburl.driver, Driver.SQLITE)
        self.assertEqual(dburl.username, None)
        self.assertEqual(dburl.password, None)
        self.assertEqual(dburl.hostname, None)
        self.assertEqual(dburl.port, None)
        self.assertEqual(dburl.args, {
            "autocommit": False,
            "isolation_level": "DEFERRED"
        })
        args = dict()
        args["database"] = "~/foo.db"
        args["autocommit"] = False
        args["isolation_level"] = "DEFERRED"
        self.assertEqual(dburl.get_args(), args)

    def test_string2bool(self):
        self.assertEqual(DBUrl.string2bool("None"), None)
        self.assertEqual(DBUrl.string2bool("True"), True)
        self.assertEqual(DBUrl.string2bool("False"), False)

    def test_error(self):
        self.assertRaises(NotSupportedError, DBUrl.get_from_url,
                          "mongodb://scott:tiger@localhost:3306/foo")

        self.assertRaises(NotSupportedError, DBUrl.get_from_url,
                          "mysql+dev://scott:tiger@localhost:3306/foo")
