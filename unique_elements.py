import unittest
from typing import List, Any, Tuple
from copy import deepcopy

def unique_elements(a: List[Any], b: List[Any]) -> Tuple[List[Any], List[Any]]:
    b_copy = deepcopy(b)
    c = []
    for item in a:
        if item in b_copy:
            b_copy.remove(item)
        else:
            c.append(item)
    d = [item for item in b_copy if item not in a]
    return c, d


class TestUniqueElements(unittest.TestCase):

    def test_both_lists_empty(self):
        a, b = [], []
        c, d = unique_elements(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])

    def test_one_list_empty(self):
        a = [1, 2, 3]
        b = []
        c, d = unique_elements(a, b)
        self.assertEqual(c, [1, 2, 3])
        self.assertEqual(d, [])

    def test_no_common_elements(self):
        a = [1, 2, 3]
        b = [4, 5, 6]
        c, d = unique_elements(a, b)
        self.assertEqual(c, [1, 2, 3])
        self.assertEqual(d, [4, 5, 6])

    def test_some_common_elements(self):
        a = [1, 2, 3, 4]
        b = [3, 4, 5, 6]
        c, d = unique_elements(a, b)
        self.assertEqual(c, [1, 2])
        self.assertEqual(d, [5, 6])

    def test_all_common_elements(self):
        a = [1, 2, 3]
        b = [1, 2, 3]
        c, d = unique_elements(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])

    def test_with_duplicates(self):
        a = [1, 2, 2, 3, 3]
        b = [2, 3, 3, 4, 4]
        c, d = unique_elements(a, b)
        self.assertEqual(c, [1, 2])
        self.assertEqual(d, [4, 4])

if __name__ == '__main__':
    unittest.main()
