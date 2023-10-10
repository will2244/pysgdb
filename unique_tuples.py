from typing import Set, List, Tuple, Any
import unittest

def unique_tuples(a: Set[Tuple[Any, Any]], b: Set[Tuple[Any, Any]]) -> Tuple[List[Tuple[Any, Any]], List[Tuple[Any, Any]]]:
    c = list(a - b)
    d = list(b - a)
    return c, d

class TestUniqueTuples(unittest.TestCase):

    def test_both_sets_empty(self):
        a, b = set(), set()
        c, d = unique_tuples(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])

    def test_one_set_empty(self):
        a = {(1, 2), (3, 4)}
        b = set()
        c, d = unique_tuples(a, b)
        self.assertEqual(c, [(1, 2), (3, 4)])
        self.assertEqual(d, [])

    def test_no_common_tuples(self):
        a = {(1, 2), (3, 4)}
        b = {(5, 6), (7, 8)}
        c, d = unique_tuples(a, b)
        self.assertEqual(c, [(1, 2), (3, 4)])
        self.assertEqual(d, [(5, 6), (7, 8)])

    def test_some_common_tuples(self):
        a = {(1, 2), (3, 4), (5, 6)}
        b = {(5, 6), (7, 8)}
        c, d = unique_tuples(a, b)
        self.assertEqual(c, [(1, 2), (3, 4)])
        self.assertEqual(d, [(7, 8)])

    def test_all_common_tuples(self):
        a = {(1, 2), (3, 4)}
        b = {(1, 2), (3, 4)}
        c, d = unique_tuples(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])

if __name__ == '__main__':
    unittest.main()
