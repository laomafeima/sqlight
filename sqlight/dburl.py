from urllib.parse import urlparse, parse_qs
from typing import List, Dict

from sqlight.platforms import Platform, Driver


class DBUrl(object):

    def __init__(self):
        self.raw_url = None
        self.platform = None
        self.driver = None
        self.hostname = None
        self.port = None
        self.username = None
        self.password = None
        self.database = None
        self.args = None

    def parse_url(self, url: str):
        self.raw_url = url
        url_info = urlparse(url)
        self.parse_scheme(url_info.scheme)

        self.hostname = url_info.hostname
        self.port = url_info.port
        self.username = url_info.username
        self.password = url_info.password

        self.parse_path(url_info.path)
        self.parse_query(url_info.query)

    def parse_path(self, path: str):
        self.database = path.lstrip("/")

    def parse_query(self, query: str):
        if not query:
            return

        args = parse_qs(query)
        self.args = {}
        for k, v in args.items():
            self.args[k] = self.args_type_conver(v)

    def parse_scheme(self, scheme: str):
        scheme_info = scheme.split("+")
        self.platform = Platform.get_platform(scheme_info[0])
        driver = None
        if len(scheme_info) > 1:
            driver = scheme_info[1]

        self.driver = Driver.get_driver(self.platform, driver)

    def args_type_conver(self, values: List[str]) -> object:
        value = values[0]
        if value in ["True", "False", "None"]:
            value = self.string2bool(value)
        elif value.isdigit():
            value = int(value)
        return value

    def get_args(self) -> Dict:
        if self.driver is Driver.SQLITE:
            return self._get_sqlite_args()
        elif self.driver is Driver.PYMYSQL:
            return self._get_pymysql_args()
        elif self.driver is Driver.MYSQLCLIENT:
            return self._get_mysqlclient_args()
        elif self.driver is Driver.PSYCOPG:
            return self._get_psycopg_args()

    def _get_sqlite_args(self):
        args = dict()
        args['database'] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    def _get_pymysql_args(self):
        args = dict()
        args["host"] = self.hostname
        args["port"] = self.port
        args["user"] = self.username
        args["password"] = self.password
        args["database"] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    def _get_mysqlclient_args(self):
        args = dict()
        args["host"] = self.hostname
        args["port"] = self.port
        args["user"] = self.username
        args["passwd"] = self.password
        args["db"] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    def _get_psycopg_args(self):
        args = dict()
        args["host"] = self.hostname
        args["port"] = self.port
        args["user"] = self.username
        args["password"] = self.password
        args["database"] = self.database
        if self.args is not None:
            args.update(self.args)
        return args

    @staticmethod
    def string2bool(v: str) -> bool:
        if v == "False":
            return False
        elif v == "True":
            return True
        elif v == "None":
            return None

    @staticmethod
    def get_from_url(url: str) -> 'DBUrl':
        dburl = DBUrl()
        dburl.parse_url(url)
        return dburl
