from enum import Enum

from sqlight.err import NotSupportedError


class Platform(Enum):
    SQLite = "sqlite"
    PostgreSQL = "postgresql"
    MariaDB = "mariadb"
    MySQL = "mysql"

    @classmethod
    def get_platform(cls, platform: str) -> 'Platform':
        for member in list(cls):
            if member.value == platform.lower():
                return member

        raise NotSupportedError("db[{}] is not supported.".format(platform))


class Driver(Enum):
    PYMYSQL = ("pymysql", (Platform.MySQL, Platform.MariaDB,))
    MYSQLCLIENT = ("mysqlclient", (Platform.MySQL, Platform.MariaDB,))
    PSYCOPG = ("psycopg", (Platform.PostgreSQL,))
    SQLITE = ("sqlite", (Platform.SQLite,))

    @classmethod
    def get_driver(cls, platform: Platform, driver: str) -> 'Driver':
        for member in list(Driver):
            if platform in member.value[1]:
                if driver is None or member.value[0] == driver.lower():
                    return member

        raise NotSupportedError("driver[{}] is not supported.".format(driver))
