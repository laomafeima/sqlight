from abc import ABCMeta, abstractmethod
from typing import NoReturn, Iterator, List

from sqlight.entity import Row


class DB(metaclass=ABCMeta):

    @abstractmethod
    def connect(self) -> NoReturn:
        pass

    @abstractmethod
    def begin(self) -> NoReturn:
        pass

    @abstractmethod
    def commit(self) -> NoReturn:
        pass

    @abstractmethod
    def rollback(self) -> NoReturn:
        pass

    @abstractmethod
    def close(self) -> NoReturn:
        pass

    @abstractmethod
    def get_last_executed(self) -> str:
        pass

    @abstractmethod
    def iter(self, query, *parameters) -> Iterator[Row]:
        pass

    @abstractmethod
    def query(self, query, *parameters) -> List[Row]:
        pass

    @abstractmethod
    def get(self, query, *parameters) -> Row:
        pass

    @abstractmethod
    def execute_lastrowid(self, query, *parameters) -> int:
        pass

    @abstractmethod
    def execute_rowcount(self, query, *parameters) -> int:
        pass

    @abstractmethod
    def executemany_lastrowid(self, query, parameters) -> int:
        pass

    @abstractmethod
    def executemany_rowcount(self, query, parameters) -> int:
        pass
