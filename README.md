# SQLight
A lightweight wrapper around SQLite, MySQL, PostgreSQL.


## Install

```
pip3 install sqlight
```

## Usgae

```
import sqlight

conn = sqlight.Connection("sqlite:///:memory:?isolation_level=DEFERRED")
conn.connect()
result = conn.get("select * from test where id = ?", 1)

```
For more examples, please read to tests

