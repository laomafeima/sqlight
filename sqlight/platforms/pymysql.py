import time

from functools import wraps
from typing import NoReturn, Iterator, List

import pymysql.cursors
import pymysql.err

import sqlight.err as err

from sqlight.row import Row
from sqlight.platforms.db import DB


def exce_converter(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            r = func(*args, **kwargs)
            return r
        except Exception as e:
            if isinstance(e, pymysql.err.NotSupportedError):
                raise err.NotSupportedError(e) from e
            elif isinstance(e, pymysql.err.ProgrammingError):
                raise err.ProgrammingError(e) from e
            elif isinstance(e, pymysql.err.InternalError):
                raise err.InternalError(e) from e
            elif isinstance(e, pymysql.err.IntegrityError):
                raise err.IntegrityError(e) from e
            elif isinstance(e, pymysql.err.OperationalError):
                raise err.OperationalError(e) from e
            elif isinstance(e, pymysql.err.DataError):
                raise err.DataError(e) from e
            elif isinstance(e, pymysql.err.DatabaseError):
                raise err.DatabaseError(e) from e
            elif isinstance(e, pymysql.err.InterfaceError):
                raise err.InterfaceError(e) from e
            elif isinstance(e, pymysql.err.Error):
                raise err.Error(e) from e
            elif isinstance(e, pymysql.err.Warning):
                raise err.Warning(e) from e
            else:
                raise e
    return wrapper


class PyMySQL(DB):
    def __init__(self,
                 host: str = None,
                 port: int = None,
                 database: str = None,
                 user: str = None,
                 password: str = None,
                 max_idle_time=7 * 3600,
                 connect_timeout=0,
                 sql_mode="TRADITIONAL",
                 **kwargs):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        args = dict(database=database,
                    connect_timeout=connect_timeout,
                    sql_mode=sql_mode,
                    **kwargs)
        if user is not None:
            args["user"] = user
        if password is not None:
            args["password"] = password

        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            args["host"] = host
            args["port"] = port

        self._last_executed = None
        self._db = None
        self._db_args = args
        self.reconnect()

    @exce_converter
    def reconnect(self) -> NoReturn:
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = pymysql.connect(**self._db_args)

    @exce_converter
    def connect(self) -> NoReturn:
        self._last_use_time = time.time()
        self.reconnect()

    @exce_converter
    def begin(self) -> NoReturn:
        self._ensure_connected()
        self._db.begin()

    @exce_converter
    def commit(self) -> NoReturn:
        self._ensure_connected()
        self._db.commit()

    @exce_converter
    def rollback(self) -> NoReturn:
        self._ensure_connected()
        self._db.rollback()

    @exce_converter
    def get_last_executed(self) -> str:
        return self._last_executed

    @exce_converter
    def iter(self, query: str, *parameters, **kwparameters) -> Iterator[Row]:
        self._ensure_connected()
        cursor = pymysql.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            self._cursor_close(cursor)

    @exce_converter
    def query(self, query: str, *parameters, **kwparameters) -> List[Row]:
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            self._cursor_close(cursor)

    @exce_converter
    def get(self, query: str, *parameters, **kwparameters) -> Row:
        rows = self.query(query, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise err.ProgrammingError(
                    "Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    @exce_converter
    def execute_lastrowid(self, query: str, *parameters,
                          **kwparameters) -> int:
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            self._cursor_close(cursor)

    @exce_converter
    def execute_rowcount(self, query: str, *parameters, **kwparameters) -> int:
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            self._cursor_close(cursor)

    @exce_converter
    def executemany_rowcount(self, query: str, parameters: Iterator) -> int:
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            self._cursor_close(cursor)

    @exce_converter
    def close(self) -> NoReturn:
        if self._db is not None:
            self._db.close()
            self._db = None

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None
                or (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self) -> pymysql.cursors.Cursor:
        self._ensure_connected()
        return self._db.cursor()

    def _cursor_close(self, cursor) -> NoReturn:
        if getattr(cursor, "_last_executed", None):
            self._last_executed = cursor._last_executed
        cursor.close()

    def _execute(self, cursor, query, parameters, kwparameters) -> int:
        return cursor.execute(query, kwparameters or parameters)
