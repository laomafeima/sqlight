from typing import NoReturn, Iterator, List, Dict

from sqlight.dburl import DBUrl
from sqlight.platforms.factory import get_driver
from sqlight.platforms.db import DB
from sqlight.row import Row


class Connection:

    @classmethod
    def create_from_dburl(cls, url: str) -> 'Connection':
        dburl = DBUrl.get_from_url(url)
        driver_cls = get_driver(dburl.driver)
        driver = driver_cls(**dburl.get_args())
        c = cls(driver)
        c.dburl = dburl
        return c

    def __init__(self, driver: DB):
        self._db = driver
        self.dburl = None

    def connect(self) -> NoReturn:
        self._db.connect()

    def begin(self) -> NoReturn:
        self._db.begin()

    def commit(self) -> NoReturn:
        self._db.commit()

    def rollback(self) -> NoReturn:
        self._db.rollback()

    def iter(self, query: str, *parameters, **kwparameters) -> Iterator[Row]:
        return self._db.iter(query, *parameters, **kwparameters)

    def query(self, query: str, *parameters, **kwparameters) -> List[Row]:
        return self._db.query(query, *parameters, **kwparameters)

    def get(self, query: str, *parameters, **kwparameters) -> Row:
        return self._db.get(query, *parameters, **kwparameters)

    def execute(self, query: str, *parameters, **kwparameters) -> NoReturn:
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query: str, *parameters,
                          **kwparameters) -> int:
        return self._db.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_rowcount(self, query: str, *parameters, **kwparameters) -> int:
        return self._db.execute_rowcount(query, *parameters, **kwparameters)

    def executemany(self, query: str, parameters: Iterator[Dict]) -> int:
        return self._db.executemany_rowcount(query, parameters)

    def close(self):
        self._db.close()

    def get_last_executed(self):
        return self._db.get_last_executed()

    def __del__(self):
        self.close()

    update = delete = execute_rowcount
    updatemany = executemany
    insert = execute_lastrowid
    insertmany = executemany
