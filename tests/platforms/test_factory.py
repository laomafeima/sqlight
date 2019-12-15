import unittest

from sqlight.platforms.keywords import Driver
from sqlight.platforms.mysqlclient import MySQLDB
from sqlight.platforms.pymysql import PyMySQL
from sqlight.platforms.sqlite import SQLite
from sqlight.err import ProgrammingError

from sqlight.platforms.factory import get_driver


class TestFactory(unittest.TestCase):

    def test_get_driver(self):
        self.assertEqual(get_driver(Driver.SQLITE), SQLite)
        self.assertEqual(get_driver(Driver.PYMYSQL), PyMySQL)
        self.assertEqual(get_driver(Driver.MYSQLCLIENT), MySQLDB)
        self.assertRaises(ProgrammingError, get_driver, Driver.PSYCOPG)
