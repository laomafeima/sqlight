import sqlite3

from functools import wraps
from typing import NoReturn, Iterator, List, Dict

import sqlight.err as err

from sqlight.platforms.db import DB
from sqlight.row import Row


def exce_converter(func):
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
    return wrapper


class SQLite(DB):
    def __init__(self,
                 database: str = None,
                 init_command: str = None,
                 autocommit: bool = False,
                 **kwargs):
        if "isolation_level" not in kwargs and autocommit:
            kwargs["isolation_level"] = None
        self._database = database
        self._args = kwargs
        self._db = None
        self.init_command = init_command
        self.autocommit = autocommit

        self._last_executed = None
        self._closed = False  # connect close flag

    @exce_converter
    def connect(self) -> NoReturn:
        if not self._closed:
            self.close()
        self._db = sqlite3.connect(self._database, **self._args)
        self._db.set_trace_callback(self._trace_callback)
        self._closed = False
        if self.init_command is not None:
            cursor = self._cursor()
            self.execute_rowcount(cursor, self.init_command)
            if not self.autocommit:
                self.commit()

    @exce_converter
    def begin(self) -> NoReturn:
        if self.autocommit:
            cursor = self._cursor()
            self._execute(cursor, "BEGIN", [], {})

    @exce_converter
    def commit(self) -> NoReturn:
        if self.autocommit:
            cursor = self._cursor()
            self._execute(cursor, "COMMIT", [], {})
        else:
            self._db.commit()

    @exce_converter
    def rollback(self) -> NoReturn:
        if self.autocommit:
            cursor = self._cursor()
            self._execute(cursor, "ROLLBACK", [], {})
        else:
            self._db.rollback()

    @exce_converter
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
            cursor.close()

    @exce_converter
    def query(self, query: str, *parameters, **kwparameters) -> List[Row]:
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

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
            cursor.close()

    @exce_converter
    def execute_rowcount(self, query: str, *parameters, **kwparameters) -> int:
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    @exce_converter
    def executemany_rowcount(self, query: str, parameters: Iterator) -> int:
        cursor = self._cursor()
        try:
            self._executemany(cursor, query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def _execute(self, cursor: sqlite3.Cursor, query: str, parameters: List,
                 kwparameters: Dict) -> NoReturn:
        if kwparameters:
            query = self.pyformat_to_named(query, kwparameters)
        else:
            query = self.format_to_qmark(query, parameters)
        cursor.execute(query, kwparameters or parameters)

    def _executemany(self, cursor: sqlite3.Cursor, query: str,
                     parameters: Iterator) -> NoReturn:
        if parameters:
            p = None
            for p in parameters:
                break
            # 获取 Iterable 的第一个元素，为了确定是named 还是 qmark
            if isinstance(p, dict):
                query = self.pyformat_to_named(query, p)
            else:
                query = self.format_to_qmark(query, p)
        cursor.executemany(query, parameters)

    def _trace_callback(self, last_executed: str):
        self._last_executed = last_executed

    def _cursor(self) -> sqlite3.Cursor:
        if self._db is None:
            raise err.Error("not connected.")
        return self._db.cursor()

    @exce_converter
    def close(self):
        if self._db is not None:
            self._db.close()
            self._closed = True

    @classmethod
    def format_to_qmark(cls, query: str, parameters: List) -> str:
        # return re.compile(r'(?<!%)%s', 0).sub('?', query).replace('%%', '%')
        try:
            query = query % tuple('?' for _ in range(len(parameters)))
        except Exception as e:
            raise err.ProgrammingError(e) from e
        return query.replace('%%', '%')

    @classmethod
    def pyformat_to_named(cls, query: str, parameters: Dict) -> str:
        try:
            query = query % {i: ":" + i for i in parameters.keys()}
        except Exception as e:
            raise err.ProgrammingError(e) from e
        return query
