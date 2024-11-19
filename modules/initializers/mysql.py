from sqlalchemy import text
from pathlib import Path as path_of

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