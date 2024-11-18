from pymongo import MongoClient
from pathlib import Path as path_of

"""
Create the collections with the data validation
stored in 

Requires database, a object type from MongoClient
"""
def init_collections(database, schema_dir):
    schemas = path_of(schema_dir)
    for f in data_dir.iterdir() :
        if f.name[-5:] == ".json" :
            with open(str(f), "r") as json_schema :
                database.create_collection(f.name[:-5], validator=json_schema)
