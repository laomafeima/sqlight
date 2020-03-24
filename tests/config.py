MYSQL_CLIENT_URL = "mysql+mysqlclient://root:123456@127.0.0.1:3306/test"
PYMYSQL_URL = "mysql+pymysql://root:123456@127.0.0.1:3306/test"
POSTGRESQL_URL = "postgresql://postgres:123456@127.0.0.1:5432/test"

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


postgresql_test_table = """
        CREATE TABLE test(
            id SERIAL PRIMARY KEY ,
            name VARCHAR
        )
"""
