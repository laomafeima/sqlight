import unittest

from sqlight.row import Row


class TestRow(unittest.TestCase):

    def test_keys(self):
        row = Row(zip(["name", "age"], [1, 19]))
        self.assertEqual(list(row.keys()), ["name", "age"])
        self.assertEqual(row.name, 1)
        self.assertEqual(row["name"], 1)
        self.assertEqual(list(row.get_changed()), [])
        row.name = 1
        self.assertEqual(list(row.get_changed()), [])
        row.name = 2
        self.assertEqual(list(row.get_changed()), ["name"])
        row.name = 3
        self.assertEqual(list(row.get_changed()), ["name"])
        self.assertEqual(row.name, 3)
        with self.assertRaises(AttributeError):
            row.test_name = 3

        with self.assertRaises(AttributeError):
            row["test_name"] = 3

        with self.assertRaises(AttributeError):
            row.test_name
