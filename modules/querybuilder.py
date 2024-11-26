from pathlib import Path as path_of, PurePath as concat_path
from json import load

'''
builds insert queries for MySQL based on the mappings.
Returns dict mapping table_name to respective query
'''
def build_SQL_insert_queries(environment):
    map_path = path_of(environment["PATH_MAPPINGS"])
    mappings = [f for f in map_path.iterdir() if f.name[-5:] == '.json']
    queries = {}
    for file in mappings:
        table_name = file.name[:-5].lower()
        with open(str(file), "r") as map_file:
            attributes = load(map_file)
            queries[table_name] = f"INSERT INTO {table_name} VALUES ({", ".join([":"+attributes[i] for i in attributes])})" 
    return queries