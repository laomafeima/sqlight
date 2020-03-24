from sqlight.platforms.keywords import Driver
from sqlight.platforms.db import DB
from sqlight.err import ProgrammingError, NotSupportedError

try:
    import MySQLdb.cursors
except ImportError:
    MySQLDB = None
else:
    from sqlight.platforms.mysqlclient import MySQLDB

try:
    import pymysql.cursors
except ImportError:
    PyMySQL = None
else:
    from sqlight.platforms.pymysql import PyMySQL

try:
    import psycopg2
except ImportError:
    Psycopg2 = None
else:
    from sqlight.platforms.psycopg import Psycopg2

try:
    import sqlite3
except ImportError:
    SQLite = None
else:
    from sqlight.platforms.sqlite import SQLite


def get_driver(driver: Driver) -> DB:
    if driver is Driver.MYSQLCLIENT:
        if MySQLDB is None:
            _raise_not_supported_driver(driver)
        return MySQLDB
    elif driver is Driver.PYMYSQL:
        if PyMySQL is None:
            _raise_not_supported_driver(driver)
        return PyMySQL
    elif driver is Driver.PSYCOPG:
        if Psycopg2 is None:
            _raise_not_supported_driver(driver)
        return Psycopg2
    elif driver is Driver.SQLITE:
        if SQLite is None:
            _raise_not_supported_driver(driver)
        return SQLite

    raise ProgrammingError("Unknown driver [{}]".format(driver))


def _raise_not_supported_driver(driver: Driver):
    raise NotSupportedError(
            "driver [{}] is not installed. please install".format(
                driver.value[0]))
