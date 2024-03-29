import unittest
from datetime import datetime
from pysgdb import DB, _unique_elements, _unique_tuples
import os


class TestDB(unittest.TestCase):

    def make_new_db(self):
        self.db = DB()
        self.db.migrate({
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
        person_id = self.db.create("Person", [{"name": "Test User"}])[0]
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]
        showing_id = self.db.create("Showing", [{"date": datetime(2023, 9, 24), "theater": "Palace"}])[0]
        
        # Link nodes
        self.db.link("Person", [person_id], "has", "Ticket", [ticket_id])
        self.db.link("Ticket", [ticket_id], "for", "Showing", [showing_id])
        
        # Test deletion
        self.db.delete("Person", [person_id])
        
        # Check if node is deleted
        self.assertNotIn(person_id, self.db.db["nodes"]["Person"])
        
        # Check if links are deleted
        self.assertNotIn(person_id, self.db.db["->"]["has"]["Person"])
        self.assertNotIn(person_id, self.db.db["<-"]["has"]["Ticket"])
        
        # Try deleting non-existent node
        with self.assertRaises(AssertionError):
            self.db.delete("Person", [person_id])
        
        # Try deleting with non-existent node name
        with self.assertRaises(AssertionError):
            self.db.delete("InvalidNode", [person_id])


    def test_link(self):
        self.make_new_db()

        # Creating Nodes to link
        person_id = self.db.create("Person", [{"name": "Test User"}])[0] 
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]

        # Valid Linking
        self.db.link("Person", [person_id], "has", "Ticket", [ticket_id])
        
        # Checking the -> direction
        self.assertIn(person_id, self.db.db["->"]["has"]["Person"])
        self.assertIn("Ticket", self.db.db["->"]["has"]["Person"][person_id])
        self.assertIn(ticket_id, self.db.db["->"]["has"]["Person"][person_id]["Ticket"])
        
        # Checking the <- direction
        self.assertIn(ticket_id, self.db.db["<-"]["has"]["Ticket"])
        self.assertIn("Person", self.db.db["<-"]["has"]["Ticket"][ticket_id])
        self.assertIn(person_id, self.db.db["<-"]["has"]["Ticket"][ticket_id]["Person"])

        # Test linking with invalid node names
        with self.assertRaises(AssertionError):
            self.db.link("InvalidNode", [person_id], "has", "Ticket", [ticket_id])
        with self.assertRaises(AssertionError):
            self.db.link("Person", [person_id], "has", "InvalidNode", [ticket_id])

        # Test linking with invalid link name
        with self.assertRaises(AssertionError):
            self.db.link("Person", [person_id], "invalidLink", "Ticket", [ticket_id])

        # Test linking with non-existing IDs
        with self.assertRaises(AssertionError):
            self.db.link("Person", ["10"], "has", "Ticket", [ticket_id])
        with self.assertRaises(AssertionError):
            self.db.link("Person", [person_id], "has", "Ticket", ["10"])


    def test_unlink(self):
        self.make_new_db()

        # Creating Nodes to link
        person_id = self.db.create("Person", [{"name": "Test User"}])[0]
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]

        # Creating a link to later unlink
        self.db.link("Person", [person_id], "has", "Ticket", [ticket_id])
        # Unlinking
        self.db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])

        # Verify that the link is removed in both directions
        self.assertNotIn(person_id, self.db.db["->"]["has"]["Person"])
        self.assertNotIn(ticket_id, self.db.db["<-"]["has"]["Ticket"])
        
        # Test unlinking with invalid node names
        with self.assertRaises(AssertionError):
            self.db.unlink("InvalidNode", [person_id], "has", "Ticket", [ticket_id])
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", [person_id], "has", "InvalidNode", [ticket_id])

        # Test unlinking with invalid link name
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", [person_id], "invalidLink", "Ticket", [ticket_id])

        # Test unlinking with non-existing IDs
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", ["10"], "has", "Ticket", [ticket_id])
        with self.assertRaises(AssertionError):
            self.db.unlink("Person", [person_id], "has", "Ticket", ["10"])


    def test_get(self):
        self.make_new_db()
        
        # Create nodes for testing
        person_id = self.db.create("Person", [{"name": "Test User"}])[0]
        movie_id = self.db.create("Movie", [{"title": "The Movie"}])[0]
        
        # Getting data from the existing node
        person_data = self.db.get("Person", [person_id], ["name"])
        self.assertEqual(person_data, [["Test User"]])
        
        # Getting data from the existing node
        movie_data = self.db.get("Movie", [movie_id], ["title"])
        self.assertEqual(movie_data, [["The Movie"]])
        
        # Non-existent ID
        with self.assertRaises(AssertionError):
            self.db.get("Person", ["2"], ["name"])
            
        # Non-existent node name
        with self.assertRaises(AssertionError):
            self.db.get("InvalidNode", [person_id], ["name"])
            
        # Non-existent attribute name
        with self.assertRaises(AssertionError):
            self.db.get("Person", [person_id], ["invalid_attribute"])

        # Getting data from all instances of a node
        person_data = self.db.get("Person", None, ["id", "name"])
        assert person_data == [['0', 'Test User']]
        self.db.create("Person", [{"name": "Another test user"}])
        person_data2 = self.db.get("Person", None, ["id", "name"])
        assert person_data2 == [['0', 'Test User'], ['2', 'Another test user']]


    def test_traverse(self):
        self.make_new_db()
        
        # Adding some data for testing
        person_id = self.db.create("Person", [{"name": "Test User"}])[0]
        movie_id = self.db.create("Movie", [{"title": "movie1"}])[0]
        showing_id = self.db.create("Showing", [{"date": datetime.now(), "theater": "theater1"}])[0]
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]

        # Creating links
        self.db.link("Person", person_id, "has", "Ticket", ticket_id)
        self.db.link("Ticket", ticket_id, "for", "Showing", showing_id)
        self.db.link("Showing", showing_id, "of", "Movie", movie_id)

        # Traverse from Person to Ticket
        tickets = self.db.traverse("Person", [person_id], "->", "has", "Ticket")
        self.assertEqual(tickets, [ticket_id])

        # Traverse from Ticket to Showing
        showings = self.db.traverse("Ticket", [ticket_id], "->", "for", "Showing")
        self.assertEqual(showings, [showing_id])

        # Traverse from Showing to Movie
        movies = self.db.traverse("Showing", [showing_id], "->", "of", "Movie")
        self.assertEqual(movies, [movie_id])

        # Negative Tests
        # Invalid Source Node
        with self.assertRaises(AssertionError):
            self.db.traverse("InvalidNode", [person_id], "->", "has", "Ticket")

        # Invalid Target Node
        with self.assertRaises(AssertionError):
            self.db.traverse("Person", [person_id], "->", "has", "InvalidNode")

        # Invalid Direction
        with self.assertRaises(AssertionError):
            self.db.traverse("Person", [person_id], "<>", "has", "Ticket")

        # Invalid Link
        with self.assertRaises(AssertionError):
            self.db.traverse("Person", [person_id], "->", "holds", "Ticket")


    def test_migrate_link(self):
        self.make_new_db()
        new_schema = {
            "nodes": {
                "Person": {"name": "str"},
                "Movie": {"title": "str"},
                "Play": {"title": "str"},
                "Showing": {"date": "datetime", "theater": "str"},
                "Ticket": {"seat": "str"}
            },
            "links": {
                # ("Person", "has", "Ticket"), removed this one
                ("Ticket", "for", "Showing"),
                ("Showing", "of", "Movie"),
                ("Showing", "of", "Play"),
                ("Person", "enjoyed", "Play") # added this one
            }
        }
        person_id = self.db.create("Person", [{"name": "Test User"}])[0]
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]
        self.db.link("Person", person_id, "has", "Ticket", ticket_id)

        # test invalid migration - link data still exists
        with self.assertRaises(AssertionError):
            self.db.migrate(new_schema)

        # test valid link migration
        self.db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])
        self.db.migrate(new_schema)

        # test that link is removed
        assert ("Person", "has", "Ticket") not in self.db.db["schema"]["links"]
        assert "has" not in self.db.db["->"]
        assert "has" not in self.db.db["<-"]
        assert "has" not in self.db.db["node_links"]["Person"]

        # test that link is added
        assert ("Person", "enjoyed", "Play") in self.db.db["schema"]["links"]
        assert "enjoyed" in self.db.db["->"]
        assert "enjoyed" in self.db.db["<-"]
        assert "enjoyed" in self.db.db["node_links"]["Person"]


    def test_migrate_node(self):
        self.make_new_db()

        # invalid because "Person" node is deleted while links still exist referencing "Person"
        invalid_new_schema = {
            "nodes": {
                # deleted: "Person": {"name": "str"},
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
        }
        person_id = self.db.create("Person", [{"name": "Test User"}])[0]
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]
        self.db.link("Person", person_id, "has", "Ticket", ticket_id)

        # test invalid migration - link data still exists
        with self.assertRaises(AssertionError):
            self.db.migrate(invalid_new_schema)

        valid_new_schema = {
            "nodes": {
                # deleted: "Person": {"name": "str"},
                "Movie": {"title": "str"},
                "Play": {"title": "str"},
                "Showing": {"date": "datetime", "theater": "str"},
                "Ticket": {"seat": "str"},
                "Actor": {"bio": "str"} # new actor node
            },
            "links": {
                # deleted: ("Person", "has", "Ticket"),
                ("Ticket", "for", "Showing"),
                ("Showing", "of", "Movie"),
                ("Showing", "of", "Play")
            }
        }
        # should fail because there is a has: Person -> Ticket connection
        with self.assertRaises(AssertionError):
            self.db.migrate(valid_new_schema)

        # remove the connection and object so the migration works
        self.db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])
        self.db.delete("Person", [person_id])
        self.db.migrate(valid_new_schema)

        # test node removal
        assert "Person" not in self.db.db["schema"]["nodes"]
        assert "Person" not in self.db.db["nodes"]

        # test node insertion
        assert {"bio": "str"} == self.db.db["schema"]["nodes"]["Actor"]
        assert "Actor" in self.db.db["nodes"]


    def test_save_and_load_db(self):
        # test save and load save database
        self.make_new_db()
        person_id = self.db.create("Person", [{"name": "Test User"}])[0] 
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]
        self.db.link("Person", [person_id], "has", "Ticket", [ticket_id])
        self.db.create("Showing", [
            {"date": datetime(2000, 1, 1), "theater": "Theater 5"},
            {"date": datetime(2010, 1, 1), "theater": "Theater 2"}
        ])
        
        folder = "."
        db_filename = "test_save_and_load_db"
        self.db.save(folder, db_filename)
        before = self.db.db

        self.db = DB()
        self.db.load(folder, db_filename)
        os.remove(os.path.join(folder, db_filename))

        assert before == self.db.db
        self.db.create("Person", [{"name": "Test User 2"}])
        assert before != self.db.db

    def test_readme_snippets(self):
        self.make_new_db()
        new_person_ids = self.db.create("Person", [{"name": "Bob"}, {"name":"Alice"}])
        assert new_person_ids == ['0','1']

        new_showing_ids = self.db.create("Showing", [
            {"date": datetime(2000, 1, 1), "theater": "Theater 5"},
            {"date": datetime(2010, 1, 1), "theater": "Theater 2"}
        ])
        showing_data = self.db.get("Showing", new_showing_ids, ["date", "theater"])
        assert showing_data == [[datetime(2000, 1, 1), "Theater 5"], [datetime(2010, 1, 1), "Theater 2"]]


        person_data = self.db.get("Person", new_person_ids, ["name"])
        assert person_data == [["Bob"], ["Alice"]]

        person_id = self.db.create("Person", [{"name": "Test User"}])[0] 
        ticket_id = self.db.create("Ticket", [{"seat": "A1"}])[0]

        self.db.link("Person", [person_id], "has", "Ticket", [ticket_id])

        this_persons_tickets = self.db.traverse("Person", [person_id], "->", "has", "Ticket")[0]
        assert this_persons_tickets == ticket_id

        this_tickets_persons = self.db.traverse("Ticket", [ticket_id], "<-", "has", "Person")[0]
        assert this_tickets_persons == person_id

        self.db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])

        self.db.delete("Person", [person_id])


class TestUniqueElements(unittest.TestCase):

    def test_both_lists_empty(self):
        a, b = [], []
        c, d = _unique_elements(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])

    def test_one_list_empty(self):
        a = [1, 2, 3]
        b = []
        c, d = _unique_elements(a, b)
        self.assertEqual(c, [1, 2, 3])
        self.assertEqual(d, [])

    def test_no_common_elements(self):
        a = [1, 2, 3]
        b = [4, 5, 6]
        c, d = _unique_elements(a, b)
        self.assertEqual(c, [1, 2, 3])
        self.assertEqual(d, [4, 5, 6])

    def test_some_common_elements(self):
        a = [1, 2, 3, 4]
        b = [3, 4, 5, 6]
        c, d = _unique_elements(a, b)
        self.assertEqual(c, [1, 2])
        self.assertEqual(d, [5, 6])

    def test_all_common_elements(self):
        a = [1, 2, 3]
        b = [1, 2, 3]
        c, d = _unique_elements(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])

    def test_with_duplicates(self):
        a = [1, 2, 2, 3, 3]
        b = [2, 3, 3, 4, 4]
        c, d = _unique_elements(a, b)
        self.assertEqual(c, [1, 2])
        self.assertEqual(d, [4, 4])


class TestUniqueTuples(unittest.TestCase):

    def test_both_sets_empty(self):
        a, b = set(), set()
        c, d = _unique_tuples(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])

    def test_one_set_empty(self):
        a = {(1, 2), (3, 4)}
        b = set()
        c, d = _unique_tuples(a, b)
        self.assertEqual(c, [(1, 2), (3, 4)])
        self.assertEqual(d, [])

    def test_no_common_tuples(self):
        a = {(1, 2), (3, 4)}
        b = {(5, 6), (7, 8)}
        c, d = _unique_tuples(a, b)
        self.assertEqual(c, [(1, 2), (3, 4)])
        self.assertEqual(d, [(5, 6), (7, 8)])

    def test_some_common_tuples(self):
        a = {(1, 2), (3, 4), (5, 6)}
        b = {(5, 6), (7, 8)}
        c, d = _unique_tuples(a, b)
        self.assertEqual(c, [(1, 2), (3, 4)])
        self.assertEqual(d, [(7, 8)])

    def test_all_common_tuples(self):
        a = {(1, 2), (3, 4)}
        b = {(1, 2), (3, 4)}
        c, d = _unique_tuples(a, b)
        self.assertEqual(c, [])
        self.assertEqual(d, [])


if __name__ == '__main__':
    unittest.main()