MYSQL_CLIENT_URL = "mysql+mysqlclient://root:123456@127.0.0.1:3306/test"
PYMYSQL_URL = "mysql+pymysql://root:123456@127.0.0.1:3306/test"

sqlite_test_table = """
        CREATE TABLE test(
            id INTEGER  PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
    """
mysql_test_table = """
        CREATE TABLE test(
            id int(11) PRIMARY KEY AUTO_INCREMENT,
            name char(64)
        )
    """


