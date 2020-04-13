from typing import NoReturn, Iterator, List, Dict

from sqlight.dburl import DBUrl
from sqlight.err import ProgrammingError
from sqlight.platforms.factory import get_driver
from sqlight.platforms.db import DB
from sqlight.row import Row


class Connection:

    @classmethod
    def create_from_dburl(cls, url: str) -> 'Connection':
        """
        create connect from dburl
        """
        dburl = DBUrl.get_from_url(url)
        driver_cls = get_driver(dburl.driver)
        driver = driver_cls(**dburl.get_args())
        c = cls(driver)
        c.dburl = dburl
        return c

    def __init__(self, driver: DB):
        self._db = driver
        self.dburl = None

    def __del__(self):
        self.close()

    def connect(self) -> NoReturn:
        """
        connect to DB
        """
        self._db.connect()

    def begin(self) -> NoReturn:
        """
        begin a transaction. if not in autocommit mode,
        it will open a new transaction.
        """
        self._db.begin()

    def commit(self) -> NoReturn:
        """
        commit transaction.
        """
        self._db.commit()

    def rollback(self) -> NoReturn:
        """
        rollback transaction.
        """
        self._db.rollback()

    def iter(self, query: str, *parameters, **kwparameters) -> Iterator[Row]:
        """Returns an iterator for the given query and parameters."""
        return self._db.iter(query, *parameters, **kwparameters)

    def query(self, query: str, *parameters, **kwparameters) -> List[Row]:
        """Returns a row list for the given query and parameters."""
        return self._db.query(query, *parameters, **kwparameters)

    def get(self, query: str, *parameters, **kwparameters) -> Row:
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        return self._db.get(query, *parameters, **kwparameters)

    def execute(self, query: str, *parameters, **kwparameters) -> NoReturn:
        """Executes the given query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query: str, *parameters,
                          **kwparameters) -> int:
        """Executes the given query, returning the lastrowid from the query."""
        return self._db.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_rowcount(self, query: str, *parameters, **kwparameters) -> int:
        """Executes the given query, returning the rowcount from the query."""
        return self._db.execute_rowcount(query, *parameters, **kwparameters)

    def executemany(self, query: str, parameters: Iterator[Dict]) -> int:
        """Executes the given query against all the given param sequences.
        We return the rowcount from the query.
        Raises:
            ProgrammingError: when parameters is empty.
        """
        if not parameters:
            raise ProgrammingError("Parameters are not allowed to be empty.")
        return self._db.executemany_rowcount(query, parameters)

    def close(self):
        """Closes connection."""
        self._db.close()

    def get_last_executed(self):
        """Get last executed."""
        return self._db.get_last_executed()

    update = delete = execute_rowcount
    updatemany = executemany
    insert = execute_lastrowid
    insertmany = executemany
