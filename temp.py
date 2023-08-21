import sqlite3
import ray

connection = sqlite3.connect("example.db")
connection.cursor().execute("CREATE TABLE movie(title, year, score)")
dataset = ray.data.from_items([
    {"title": "Monty Python and the Holy Grail", "year": 1975, "score": 8.2}, 
    {"title": "And Now for Something Completely Different", "year": 1971, "score": 7.5}
])

dataset.write_sql(
    "INSERT INTO movie VALUES(?, ?, ?)", lambda: sqlite3.connect("example.db")
)

result = connection.cursor().execute("SELECT * FROM movie ORDER BY year")
print(result.fetchall())

# [('And Now for Something Completely Different', 1971, 7.5), ('Monty Python and the Holy Grail', 1975, 8.2)]
#                                       
import os
os.remove("example.db")