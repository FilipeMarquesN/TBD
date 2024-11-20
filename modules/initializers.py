from pymongo import MongoClient
from sqlalchemy import text
from pathlib import Path as path_of
from json import load


"""
Create the collections with the data validation
stored in 

Requires database, a object type from MongoClient
"""
def init_collections(database, schema_dir):
    schemas = path_of(schema_dir)
    for f in schemas.iterdir() :
        if f.name[-5:] == ".json" :
            with open(str(f), "r") as json_schema :
                schema = load(json_schema)
                database.create_collection(f.name[:-5], validator=schema)

"""
Create the collections with the data validation
stored in 

Requires engine, a object type from MongoClient
"""
def init_tables(engine, schema_dir):
    schemas = path_of(schema_dir)
    with engine.connect() as conn:
        for f in schemas.iterdir() :
            if f.name[-4:] == ".sql" :
                with open(str(f), "r") as sqlfile :
                    query = text(sqlfile.read())
                    conn.execute(query)