import sqlite3
import re

from functools import wraps
from typing import NoReturn, Iterator, List

import sqlight.err as err

from sqlight.platforms.db import DB
from sqlight.entity import Row


def exce_converter(cls):
    orig_getattribute = cls.__getattribute__

    def new_getattribute(self, name: str):
        return func_converter(orig_getattribute(self, name))
    cls.__getattribute__ = new_getattribute

    def func_converter(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                r = func(*args, **kwargs)
                return r
            except Exception as e:
                if isinstance(e, sqlite3.NotSupportedError):
                    raise err.NotSupportedError(e) from e
                elif isinstance(e, sqlite3.ProgrammingError):
                    raise err.ProgrammingError(e) from e
                elif isinstance(e, sqlite3.IntegrityError):
                    raise err.IntegrityError(e) from e
                elif isinstance(e, sqlite3.OperationalError):
                    raise err.OperationalError(e) from e
                elif isinstance(e, sqlite3.DatabaseError):
                    raise err.DatabaseError(e) from e
                elif isinstance(e, sqlite3.Error):
                    raise err.Error(e) from e
                elif isinstance(e, sqlite3.Warning):
                    raise err.Warning(e) from e
                else:
                    raise e
    return cls


@exce_converter
class SQLite(DB):
    """
    A lightweight wrapper around SQLite.
    default open autocommit mode
    """

    re_compile = re.compile(r'(?<!%)%s', 0)

    def __init__(self, database: str, **args):

        if "isolation_level" not in args:
            args["isolation_level"] = None
        self._database = database
        self._args = args
        self._db = None
        self.isolation_level = args["isolation_level"]

        self._last_executed = None

    def connect(self) -> NoReturn:
        """
        connect to SQLite.
        """
        self._db = sqlite3.connect(self._database, **self._args)
        self._db.set_trace_callback(self._trace_callback)

    def begin(self) -> NoReturn:
        if self.isolation_level is None:
            self._execute("BEGIN")

    def commit(self) -> NoReturn:
        if self.isolation_level is None:
            self._execute("COMMIT")
        else:
            self._db.commit()

    def rollback(self) -> NoReturn:
        if self.isolation_level is None:
            self._execute("ROLLBACK")
        else:
            self._db.rollback()

    def get_last_executed(self) -> str:
        return self._last_executed

    def iter(self, query: str, *parameters) -> Iterator(Row):
        """Returns an iterator for the given query and parameters."""
        try:
            cursor = self._execute(query, parameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query: str, *parameters) -> List[Row]:
        """Returns a row list for the given query and parameters."""
        try:
            cursor = self._execute(query, parameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def get(self, query: str, *parameters) -> Row:
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, *parameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise err.ProgrammingError(
                    "Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    def execute_lastrowid(self, query: str, *parameters) -> int:
        """Executes the given query, returning the lastrowid from the query."""
        try:
            cursor = self._execute(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query: str, *parameters) -> int:
        """Executes the given query, returning the rowcount from the query."""
        try:
            cursor = self._execute(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany_lastrowid(self, query: str, parameters: Iterator) -> int:
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        try:
            cursor = self._executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query: str, parameters: Iterator) -> int:
        """Executes the given query against all the given param sequences.
        We return the rowcount from the query.
        """
        try:
            cursor = self._executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def _execute(self, query: str, parameters: List) -> sqlite3.Cursor:
        query = self.format_to_qmark(query)
        return self._db.execute(query, parameters)

    def _executemany(self, query: str, parameters: Iterator) -> sqlite3.Cursor:
        query = self.format_to_qmark(query)
        return self._db.executemany(query, parameters)

    def _trace_callback(self, last_executed: str):
        self._last_executed = last_executed

    def close(self):
        """Closes connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    @classmethod
    def format_to_qmark(cls, query: str) -> str:
        return cls.re_compile.sub('?', query).replace('%%', '%')
