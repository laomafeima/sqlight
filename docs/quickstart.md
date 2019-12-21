# Quick start

## Install

```
pip3 install sqlight
```

## Usage

```
import sqlight

conn = sqlight.Connection("sqlite:///:memory:?isolation_level=DEFERRED")
conn.connect()
result = conn.get("select * from test where id = ?", 1)
```
