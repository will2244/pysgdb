# pysgdb

Python Simple Graph Database

A simple, type safe, in-memory, graph database written in pure python. No external dependencies.

## ⚠️ Warning ⚠️

**Warning**: This project is not maintained. pysgdb was not designed to be used in serious settings. This is a personal side project only.

**Purpose**: I threw this together because I enjoy thinking about data modeling and wanted to make a simple graph database. Publishing it for archival purposes and if anyone wants to use the code.

My next build will focus on a new graph-SQL-object query language I have been working on.

## Installation

Just copy the `pysgdb.py` file directly to your workspace. It's only one file and has no external dependencies.

### Initialize a new database

```python
from pysgdb import DB

db = DB()
```

## Schema
You cannot write to pysgdb without first specifying what the database will store. You do this by supplying a schema to the database.

Schema in pysgdb is declarative. Meaning each time you want to update the schema, you need to supply the **entire** schema, new changes included. pysgdb will internally run the migration from it's current schema to the schema you specify.

```python
my_scehma = {
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
}
db.migrate(my_schema)
```

A schema is a python dictionary. There are two main constructs in pysgdb:
1. Nodes
2. Links

A Node is just a blueprint for something - similar to a type or table. Nodes are capitalized and have attributes. A Link is a 'connection'. Links connect nodes together with semantics.

If you want to update the schema, just make the schema modifications you want and then call `db.migrate(new_schema)`

## Create

Create two new Person nodes. Ids are returned:

```python
new_person_ids = db.create("Person", [{"name": "Bob"}, {"name": "Alice"}])
```

## Get

Get the names of all Person nodes:
```python
person_data = db.get("Person", new_person_ids, ["name"])
assert person_data == [["Bob"], ["Alice"]]
```

Getting two or more fields looks like this:
```python
new_showing_ids = self.db.create("Showing", [
    {"date": datetime(2000, 1, 1), "theater": "Theater 5"},
    {"date": datetime(2010, 1, 1), "theater": "Theater 2"}
])
showing_data = db.get("Showing", new_showing_ids, ["date", "theater"])
assert showing_data == [[datetime(2000, 1, 1), "Theater 5"], [datetime(2010, 1, 1), "Theater 2"]]
```

Supplying `None` to the ids argument will fetch all instances of the node:

```python
db.get("Showing", None, ["id", "date", "theater"])
assert showing_data == [[datetime(2000, 1, 1), "Theater 5"], [datetime(2010, 1, 1), "Theater 2"]]
```

## Link

Give a Person a Ticket:

```python
person_id = db.create("Person", [{"name": "Test User"}])[0] 
ticket_id = db.create("Ticket", [{"seat": "A1"}])[0]

db.link("Person", [person_id], "has", "Ticket", [ticket_id])
```

Notice how the ids are in a list. If you supply many ids, they will all be linked to each other.

## Traverse

To get from an instance of one node to an instance of another type of node, you need to Traverse. The following example shows the query: "Get all tickets that this person has"

```python
this_persons_tickets = db.traverse("Person", [person_id], "->", "has", "Ticket")[0]
assert this_persons_tickets == ticket_id
```
The second parameter is a list so many ids can be supplied at once - similar to a join operation.

Links can also be traversed in reverse to the way the link way created, so saying "Get all Person ids that have a ticket" would look like this:

```python
this_tickets_persons = db.traverse("Ticket", [ticket_id], "<-", "has", "Person")[0]
assert this_tickets_persons == person_id
```

Where the arrow `->`, `<-` denotes the requested direction.

This multidirection functionality isn't really nessesary for the scope of this project, but it will be used extensively in the new query language of my next build.

A small preview of the new query language - the following lines represent the two traverse examples above:

- `Person->has:Ticket`
- `Ticket<-has:Person`

## Unlink

This person is no longer linked to this ticket:
```python
db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])
```

Notice how the ids are in a list. If you supply many ids, they will all be unlinked from each other.

## Delete

```python
# this also deletes any links to or from person_id
db.delete("Person", [person_id])
```

## Save database to file

```python
folder = "."
db_filename = "database_data"
db.save(folder, db_filename)
```

## Load database from file

```python
new_db = DB()
new_db.load(folder, db_filename)
```

## Indexes, filters, unique, etc..

These features will not be supported in this build. The existing features support only the most critical input/output requirements of the db. All advanced data manipulation needs to be handled manually on the raw query results.