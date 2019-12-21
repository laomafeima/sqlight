import unittest
import importlib

from sqlight.platforms.sqlite import SQLite
from sqlight.platforms.keywords import Driver
from sqlight.err import ProgrammingError
from sqlight.platforms.factory import get_driver

try:
    importlib.import_module("MySQLdb.cursors")
except ImportError:
    MySQLDB = None
else:
    from sqlight.platforms.mysqlclient import MySQLDB

try:
    importlib.import_module("pymysql.cursors")
except ImportError:
    PyMySQL = None
else:
    from sqlight.platforms.pymysql import PyMySQL


class TestFactory(unittest.TestCase):

    def test_get_driver(self):
        self.assertEqual(get_driver(Driver.SQLITE), SQLite)
        if PyMySQL is not None:
            self.assertEqual(get_driver(Driver.PYMYSQL), PyMySQL)
        if MySQLDB is not None:
            self.assertEqual(get_driver(Driver.MYSQLCLIENT), MySQLDB)
        self.assertRaises(ProgrammingError, get_driver, Driver.PSYCOPG)
