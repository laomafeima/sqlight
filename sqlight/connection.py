class Connection:
    def __init__(self):
        pass

    def connect(self):
        pass

    def reconnect(self):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def iter(self, query, *parameters):
        pass

    def query(self, query, *parameters):
        pass

    def get(self, query, *parameters):
        pass

    def execute_lastrowid(self, query, *parameters):
        pass

    def execute_rowcount(self, query, *parameters):
        pass

    def executemany(self, query, parameters):
        pass

    def executemany_lastrowid(self, query, parameters):
        pass

    def executemany_rowcount(self, query, parameters):
        pass

    def close(self):
        pass

    def __del__(self):
        self.close()

    update = delete = execute_rowcount
    updatemany = executemany_rowcount
    insert = execute_lastrowid
    insertmany = executemany_lastrowid
