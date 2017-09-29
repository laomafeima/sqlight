#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import unittest.mock
import sqlight
import os


class TestEasySQLite(unittest.TestCase):
    def setUp(self):
        self.test_class = sqlight.Connection("./test.db")
        self.test_class.execute_lastrowid("""
        CREATE TABLE test(
            id INTEGER  PRIMARY KEY AUTOINCREMENT,
            var TEXT
        );
        """)

    def tearDown(self):
        self.test_class.close()
        os.remove("./test.db")

    def test_init(self):
        self.assertTrue(os.path.isfile("./test.db"))

    def test_execute_rowcount(self):
        sql = "INSERT INTO test (id, var) VALUES (?, ?)"
        result = self.test_class.execute_rowcount(sql, 1, "var1")
        self.assertEqual(result, 1)
        result = self.test_class.get("select * from test where id = ?", 1)
        self.assertEqual(result.id, 1)
        self.assertEqual(result.var, "var1")
        sql = "INSERT INTO test (var) VALUES (?)"
        self.test_class.executemany_rowcount(sql, [("v5",), ("v6",)])
        sql = "UPDATE test SET var = ?"
        result = self.test_class.execute_rowcount(sql, "test")
        self.assertGreater(result, 0)

    def test_execute_lastrowid(self):
        sql = "INSERT INTO test (id, var) VALUES (?, ?)"
        result = self.test_class.execute_lastrowid(sql, 2, "var2")
        self.assertEqual(result, 2)
        result = self.test_class.get("select * from test where id = ?", 2)
        self.assertEqual(result.var, "var2")

    def test_executemany_rowcount(self):
        sql = "INSERT INTO test (id, var) VALUES (?, ?)"
        result = self.test_class.executemany_rowcount(sql,
                                                      [(3, "v3"), (4, "v4")])
        self.assertEqual(result, 2)

    def test_executemany_lastrowid(self):
        pass

    def test_executemany(self):
        sql = "INSERT INTO test (var) VALUES (?)"
        result = self.test_class.executemany_rowcount(sql,
                                                      [("v5",), ("v6",)])
        self.assertEqual(result, 2)

    def test_get(self):
        sql = "INSERT INTO test (var) VALUES (?)"
        self.test_class.executemany_rowcount(sql, [("var7",), ("var8",)])
        sql = "select * from test where var = ?"
        row = self.test_class.get(sql, "var7")
        self.assertEqual(row.var, "var7")
        row = self.test_class.get(sql, "var-7")
        self.assertEqual(row, None)
        with self.assertRaises(Exception):
            self.test_class.get("select * from test where id > 0")

    def test_query(self):
        self.test_class.delete("delete from `test` where id > ?", 0)
        sql = "INSERT INTO test (var) VALUES (?)"
        self.test_class.executemany_rowcount(sql, [("var9",), ("var10",)])
        sql = "select * from test where id > ?"
        result = self.test_class.query(sql, 0)
        self.assertEqual(len(result), 2)
        self.assertIn(result[0].var, ["var9", "var10"])
        sql = "select * from test where id < ?"
        result = self.test_class.query(sql, 1)
        self.assertEqual(len(result), 0)

    def test_iter(self):
        self.test_class.delete("delete from `test` where id > ?", 0)
        sql = "INSERT INTO test (var) VALUES (?)"
        self.test_class.executemany_rowcount(sql, [("var11",), ("var12",)])
        sql = "select * from test where id > ?"
        result = self.test_class.iter(sql, 0)
        for i in result:
            self.assertIn(i.var, ["var11", "var12"])

        sql = "select * from test where id < ?"
        self.test_class.iter(sql, 1)


class TestRow(unittest.TestCase):
    def setUp(self):
        self.test_class = sqlight.Row()

    def test___getattr__(self):
        self.test_class["name"] = "test"
        self.assertEqual("test", self.test_class.name)
        self.test_class["name"] = "test1"
        self.assertEqual("test1", self.test_class.name)
        with self.assertRaises(AttributeError):
            self.test_class.age


if __name__ == '__main__':
    unittest.main()
