# pysgdb

Python Simple Graph Database

A simple, type safe, in-memory, graph database written in pure python. No external dependencies.

## ⚠️ Warning ⚠️

**Warning**: This project is not maintained. pysgdb was not designed to be used in serious settings. This is a personal side project only.

**Purpose**: I threw this together because I enjoy thinking about data modeling and wanted to make a simple graph database. Publishing it for archival purposes and if anyone wants to use the code.

My next build will focus on a new graph-SQL-object query language I have been working on.

### Installation

Just copy the `pysgdb.py` file directly to your workspace. It's only one file and has no external dependencies.

### Initialize a new database

```python
from pysgdb import DB

db = DB()
```

### Schema
Schema in pysgdb is declarative. Meaning each time you want to update the schema, you need to supply the **entire** schema. pysgdb will internally run the migration from it's current schema to the schema you specify.

Here is how to update the schema:

```python
db.migrate({
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
```

A schema is a python dictionary. There are two main constructs in this graph database:
1. Nodes
2. Links

A Node is a 'thing', or an 'entity'. Nodes in the schema are capitalized. Nodes have attributes. A Link is a 'connection'. Links connect nodes together with semantics.

### Create

Create two new Person nodes:

```python
new_person_ids = db.create("Person", [{"name": "Bob"}, {"name": "Alice"}])
```

### Get

Get the names of all Person nodes:
```python
person_data = db.get_data("Person", new_person_ids, ["name"])
assert person_data == [["Bob"], ["Alice"]]
```

### Link

Give a Person a Ticket.

```python
person_id = db.create("Person", [{"name": "Test User"}])[0] 
ticket_id = db.create("Ticket", [{"seat": "A1"}])[0]

db.link("Person", [person_id], "has", "Ticket", [ticket_id])
```

### Traverse

Get all tickets that this person has. The second parameter is a list so many Ids can be supplied at once - similar to a join operation.

```python
this_persons_tickets = db.traverse("Person", [person_id], "->", "has", "Ticket")[0]
assert this_persons_tickets == ticket_id
```


Going in the other direction would look like this:

```python
this_tickets_persons = db.traverse("Ticket", [ticket_id], "<-", "has", "Person")[0]
assert this_tickets_persons == person_id
```

This multidirection functionality isn't really nessesary for the scope of this project, but it will be used extensively in the new query language of my next build.

A small preview of the new query language - the following lines represent the two traverse examples above:

- `Person->has:Ticket`
- `Ticket<-has:Person`

### Unlink

Remove the possibility that a Person could have a Ticket.
```python
db.unlink("Person", [person_id], "has", "Ticket", [ticket_id])
```

### Delete

```python
# this also deletes any links to or from person_id
db.delete("Person", [person_id])
```

### Save database to file

```python
folder = "."
db_filename = "database_data"
db.save(folder, db_filename)
```

### Load database from file

```python
new_db = DB()
new_db.save(folder, db_filename)
```

### Indexes, filters, unique, etc..

These features will not be supported in this build. The existing features support only the most critical input/output requirements of the db. All advanced data manipulation needs to be handled manually on the raw query results.