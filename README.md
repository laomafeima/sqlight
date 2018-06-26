# sqlight
A lightweight wrapper around SQLite.


## INSTALL

```
pip3 install sqlight
```

## USGAE

```
import sqlight

conn = sqlight.Connection("./test.db")
conn.connect()
result = conn.get("select * from test where id = ?", 1)

```
For more examples, please read to tests.py

