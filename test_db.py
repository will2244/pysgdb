import unittest
from datetime import datetime
from db import DB


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


    def test_get_data(self):
        self.make_new_db()
        
        # Create nodes for testing
        self.db.create("Person", [{"name": "Test User"}])
        self.db.create("Movie", [{"title": "The Movie"}])
        
        # Valid case: Getting data from the existing node
        person_data = self.db.get_data("Person", ["0"], ["name"])
        self.assertEqual(person_data, [["Test User"]])
        
        # Valid case: Getting data from the existing node
        movie_data = self.db.get_data("Movie", ["1"], ["title"])
        self.assertEqual(movie_data, [["The Movie"]])
        
        # Invalid case: Non-existent ID
        with self.assertRaises(AssertionError):
            self.db.get_data("Person", ["2"], ["name"])
            
        # Invalid case: Non-existent node name
        with self.assertRaises(AssertionError):
            self.db.get_data("InvalidNode", ["0"], ["name"])
            
        # Invalid case: Non-existent attribute name
        with self.assertRaises(AssertionError):
            self.db.get_data("Person", ["0"], ["invalid_attribute"])


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
        self.db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])

        # test valid link migration
        self.db.migrate(new_schema)


        # TODO: assert that new link is in the db schema
        # TODO: assert that the removed link is not in the schema



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
                ("Showing", "of", "Play"),
                ("Actor", "acted_in", "Play") # new Actor link
            }
        }
        # should fail because there is a has: Person -> Ticket connection
        with self.assertRaises(AssertionError):
            self.db.migrate(valid_new_schema)

        # remove that connection and migration should work
        self.db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])

        self.db.migrate(valid_new_schema)

        # TODO: assert that new node is in the db schema
        # TODO: assert that the removed node is not in the schema



if __name__ == '__main__':
    unittest.main()