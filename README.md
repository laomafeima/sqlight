# sqlight
A lightweight wrapper around SQLite, MySQL, PostgreSQL.


## INSTALL

```
pip3 install sqlight
```

## USGAE

```
import sqlight

conn = sqlight.Connection("sqlite:///:memory:?isolation_level=DEFERRED")
conn.connect()
result = conn.get("select * from test where id = ?", 1)

```
For more examples, please read to tests.py

