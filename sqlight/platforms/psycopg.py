from functools import wraps
from typing import NoReturn, Iterator, List

import psycopg2

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
            if isinstance(e, psycopg2.NotSupportedError):
                raise err.NotSupportedError(e) from e
            elif isinstance(e, psycopg2.ProgrammingError):
                raise err.ProgrammingError(e) from e
            elif isinstance(e, psycopg2.InternalError):
                raise err.InternalError(e) from e
            elif isinstance(e, psycopg2.IntegrityError):
                raise err.IntegrityError(e) from e
            elif isinstance(e, psycopg2.OperationalError):
                raise err.OperationalError(e) from e
            elif isinstance(e, psycopg2.DataError):
                raise err.DataError(e) from e
            elif isinstance(e, psycopg2.DatabaseError):
                raise err.DatabaseError(e) from e
            elif isinstance(e, psycopg2.InterfaceError):
                raise err.InterfaceError(e) from e
            elif isinstance(e, psycopg2.Error):
                raise err.Error(e) from e
            elif isinstance(e, psycopg2.Warning):
                raise err.Warning(e) from e
            else:
                raise e
    return wrapper


class Psycopg2(DB):
    def __init__(self,
                 host: str = None,
                 port: int = None,
                 database: str = None,
                 user: str = None,
                 password: str = None,
                 autocommit: bool = False,
                 init_command: str = None,
                 **kwargs):
        self.host = host
        self.database = database
        self.autocommit = autocommit
        self.init_command = init_command

        args = dict(database=database, **kwargs)
        if user is not None:
            args["user"] = user

        if password is not None:
            args["password"] = password

        if host is not None:
            args["host"] = host

        if port is not None:
            args["port"] = port

        self._last_executed = None
        self._db = None
        self._db_args = args
        self._closed = False  # connect close flag

    @exce_converter
    def connect(self) -> NoReturn:
        if not self._closed:
            self.close()
        self._db = psycopg2.connect(**self._db_args)
        if self.autocommit:
            self._db.autocommit = self.autocommit
        self._closed = False
        if self.init_command is not None:
            cursor = self._cursor()
            self.execute_rowcount(cursor, self.init_command)
            if not self.autocommit:
                self.commit()

    @exce_converter
    def begin(self) -> NoReturn:
        cursor = self._cursor()
        self._execute(cursor, "BEGIN", [], {})

    @exce_converter
    def commit(self) -> NoReturn:
        cursor = self._cursor()
        self._execute(cursor, "COMMIT", [], {})

    @exce_converter
    def rollback(self) -> NoReturn:
        cursor = self._cursor()
        self._execute(cursor, "ROLLBACK", [], {})

    def get_last_executed(self) -> str:
        return self._last_executed

    @exce_converter
    def iter(self, query: str, *parameters, **kwparameters) -> Iterator[Row]:
        cursor = self._cursor()
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

    def _cursor(self) -> psycopg2.extensions.cursor:
        return self._db.cursor()

    def _cursor_close(self, cursor) -> NoReturn:
        if getattr(cursor, "query", None):
            self._last_executed = cursor.query.decode()
        cursor.close()

    def _execute(self, cursor, query, parameters, kwparameters) -> int:
        return cursor.execute(query, kwparameters or parameters)
