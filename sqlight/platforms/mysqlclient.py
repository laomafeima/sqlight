import time

from functools import wraps
from typing import NoReturn, Iterator, List

import MySQLdb.cursors

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
            if isinstance(e, MySQLdb.NotSupportedError):
                raise err.NotSupportedError(e) from e
            elif isinstance(e, MySQLdb.ProgrammingError):
                raise err.ProgrammingError(e) from e
            elif isinstance(e, MySQLdb.InternalError):
                raise err.InternalError(e) from e
            elif isinstance(e, MySQLdb.IntegrityError):
                raise err.IntegrityError(e) from e
            elif isinstance(e, MySQLdb.OperationalError):
                raise err.OperationalError(e) from e
            elif isinstance(e, MySQLdb.DataError):
                raise err.DataError(e) from e
            elif isinstance(e, MySQLdb.DatabaseError):
                raise err.DatabaseError(e) from e
            elif isinstance(e, MySQLdb.InterfaceError):
                raise err.InterfaceError(e) from e
            elif isinstance(e, MySQLdb.Error):
                raise err.Error(e) from e
            elif isinstance(e, MySQLdb.Warning):
                raise err.Warning(e) from e
            else:
                raise e
    return wrapper


class MySQLDB(DB):
    def __init__(self,
                 host: str = None,
                 port: int = None,
                 db: str = None,
                 user: str = None,
                 passwd: str = None,
                 autocommit: bool = False,
                 max_idle_time=7 * 3600,
                 connect_timeout=0,
                 sql_mode="TRADITIONAL",
                 **kwargs):
        self.host = host
        self.database = db
        self.autocommit = autocommit
        self.max_idle_time = float(max_idle_time)

        args = dict(db=db,
                    autocommit=autocommit,
                    connect_timeout=connect_timeout,
                    sql_mode=sql_mode,
                    **kwargs)
        if user is not None:
            args["user"] = user
        if passwd is not None:
            args["passwd"] = passwd

        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            args["host"] = host
            args["port"] = port

        self._last_executed = None
        self._db = None
        self._db_args = args
        self._closed = False  # connect close flag

    @exce_converter
    def connect(self) -> NoReturn:
        if not self._closed:
            self.close()
        self._db = MySQLdb.connect(**self._db_args)
        self._last_use_time = time.time()
        self._closed = False

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

    def get_last_executed(self) -> str:
        return self._last_executed

    @exce_converter
    def iter(self, query: str, *parameters, **kwparameters) -> Iterator[Row]:
        self._ensure_connected()
        cursor = MySQLdb.cursors.SSCursor(self._db)
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
            self._closed = True

    def _ensure_connected(self):
        if (time.time() - self._last_use_time > self.max_idle_time):
            self.connect()
        self._last_use_time = time.time()

    def _cursor(self) -> MySQLdb.cursors.Cursor:
        self._ensure_connected()
        return self._db.cursor()

    def _cursor_close(self, cursor) -> NoReturn:
        if getattr(cursor, "_last_executed", None):
            self._last_executed = cursor._last_executed.decode()
        cursor.close()

    def _execute(self, cursor, query, parameters, kwparameters) -> int:
        return cursor.execute(query, kwparameters or parameters)
