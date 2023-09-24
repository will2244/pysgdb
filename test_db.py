import unittest
from datetime import datetime
from db import DB


class TestDB(unittest.TestCase):

    def make_new_db(self):
        self.db = DB({
            "nodes": {
                "Person": {"name": "str"},
                "Movie": {"title": "str"},
                "Play": {"title": "str"},
                "Showing": {"date": "datetime", "theater": "str"},
                "Ticket": {"seat": "str"}
            },
            "links": {
                ("Person", "has", "Ticket"),
                ("Ticket", "for", "Showing"),
                ("Showing", "of", "Movie"),
                ("Showing", "of", "Play")
            }
        })


    def test_init(self):
        self.make_new_db()
        self.assertIn("nodes", self.db.db)
        self.assertIn("schema", self.db.db)
        self.assertIn("->", self.db.db)
        self.assertIn("<-", self.db.db)

        self.assertIn("Person", self.db.db["nodes"])
        self.assertIn("Movie", self.db.db["nodes"])

        self.assertIn("Person", self.db.db["->"]["has"])
        self.assertIn("Ticket", self.db.db["->"]["for"])
        self.assertIn("Showing", self.db.db["->"]["of"])

        self.assertIn("Ticket", self.db.db["<-"]["has"])
        self.assertIn("Showing", self.db.db["<-"]["for"])
        self.assertIn("Movie", self.db.db["<-"]["of"])
        self.assertIn("Play", self.db.db["<-"]["of"])


    def test_create(self):
        self.make_new_db()
        self.db.create("Person", [{"name": "Test User"}])
        self.assertEqual(len(self.db.db["nodes"]["Person"]), 1)

        # Check with wrong attribute name
        with self.assertRaises(AssertionError):
            self.db.create("Person", [{"nam": "Test User"}])

        # Check with wrong attribute type
        with self.assertRaises(AssertionError):
            self.db.create("Person", [{"name": 123}])

        # Check with wrong node_name
        with self.assertRaises(AssertionError):
            self.db.create("Perso", [{"name": "Test User"}])

        # Check with empty attributes
        with self.assertRaises(AssertionError):
            self.db.create("Person", [])

    def test_delete(self):
        self.make_new_db()
        
        # Create some nodes
        self.db.create("Person", [{"name": "Test User"}]) # Id - 0
        self.db.create("Ticket", [{"seat": "A1"}])       # Id - 1
        self.db.create("Showing", [{"date": datetime(2023, 9, 24), "theater": "Palace"}]) # Id - 2
        
        # Link nodes
        self.db.link("Person", ["0"], "has", "Ticket", ["1"])
        self.db.link("Ticket", ["1"], "for", "Showing", ["2"])
        
        # Test deletion
        self.db.delete("Person", ["0"])
        
        # Check if node is deleted
        self.assertNotIn("0", self.db.db["nodes"]["Person"])
        
        # Check if links are deleted
        self.assertNotIn("0", self.db.db["->"]["has"]["Person"])
        self.assertNotIn("0", self.db.db["<-"]["has"]["Ticket"])
        
        # Try deleting non-existent node
        with self.assertRaises(AssertionError):
            self.db.delete("Person", ["0"])
        
        # Try deleting with non-existent node name
        with self.assertRaises(AssertionError):
            self.db.delete("InvalidNode", ["0"])


    def test_link(self):
        self.make_new_db()

        # Creating Nodes to link
        self.db.create("Person", [{"name": "Test User"}])  # ID: 0
        self.db.create("Ticket", [{"seat": "A1"}])  # ID: 1

        # Valid Linking
        self.db.link("Person", ["0"], "has", "Ticket", ["1"])
        
        # Checking the -> direction
        self.assertIn("0", self.db.db["->"]["has"]["Person"])
        self.assertIn("Ticket", self.db.db["->"]["has"]["Person"]["0"])
        self.assertIn("1", self.db.db["->"]["has"]["Person"]["0"]["Ticket"])
        
        # Checking the <- direction
        self.assertIn("1", self.db.db["<-"]["has"]["Ticket"])
        self.assertIn("Person", self.db.db["<-"]["has"]["Ticket"]["1"])
        self.assertIn("0", self.db.db["<-"]["has"]["Ticket"]["1"]["Person"])

        # Test linking with invalid node names
        with self.assertRaises(AssertionError):
            self.db.link("InvalidNode", ["0"], "has", "Ticket", ["1"])
        with self.assertRaises(AssertionError):
            self.db.link("Person", ["0"], "has", "InvalidNode", ["1"])

        # Test linking with invalid link name
        with self.assertRaises(AssertionError):
            self.db.link("Person", ["0"], "invalidLink", "Ticket", ["1"])

        # Test linking with non-existing IDs
        with self.assertRaises(AssertionError):
            self.db.link("Person", ["10"], "has", "Ticket", ["1"])
        with self.assertRaises(AssertionError):
            self.db.link("Person", ["0"], "has", "Ticket", ["10"])


    def test_unlink(self):
        self.make_new_db()

        # Creating Nodes to link
        self.db.create("Person", [{"name": "Test User"}])  # ID: 0
        self.db.create("Ticket", [{"seat": "A1"}])  # ID: 1

        # Creating a link to later unlink
        self.db.link("Person", ["0"], "has", "Ticket", ["1"])
        # Unlinking
        self.db.unlink("Person", ["0"], "has", "Ticket", ["1"])

        # Verify that the link is removed in both directions
        self.assertNotIn("0", self.db.db["->"]["has"]["Person"])
        self.assertNotIn("1", self.db.db["<-"]["has"]["Ticket"])
        
        # Test unlinking with invalid node names
        with self.assertRaises(AssertionError):
            self.db.unlink("InvalidNode", ["0"], "has", "Ticket", ["1"])
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", ["0"], "has", "InvalidNode", ["1"])

        # Test unlinking with invalid link name
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", ["0"], "invalidLink", "Ticket", ["1"])

        # Test unlinking with non-existing IDs
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", ["10"], "has", "Ticket", ["1"])
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", ["0"], "has", "Ticket", ["10"])


if __name__ == '__main__':
    unittest.main()