from drivers import MongoDatabaseWrapper as mongo, MySQLDatabaseWrapper as mysql, mongo_mysql_insert as generate_schema
from pathlib import Path as path_of
from json import load

'''
Note: generate schema creates the new mongo schema
based on the SQL schema, using joins as optimizations
'''

'''
Outputs the results as a list followed by an ellipsis
if there's more than 5 records
'''
def shortenOutput(results):
    if len(results) > 5:
        return results[:5] + ["..."]
    return results


class MongoExecutor():

    def __init_schema__(self):
        for schema_file in self.schema_path.iterdir():
            if schema_file.name[-5:] == ".json" :
                with open(str(schema_file), "r") as schema_f:
                    collection = schema_file.name[:-5].lower()
                    schema = load(schema_f)
                    self.client.schema(collection, schema)

    def __init_data__(self):
        raise NotImplementedError

    def __init__(self, environment, database_name):
        self.database_name = database_name
        self.client = mongo(environment, self.database_name)
        self.query_path = path_of(environment["__WORK_DIR__"],f"mongo/{database_name}/queries")
        self.schema_path = path_of(environment["__WORK_DIR__"],f"mongo/{database_name}/schema")
        self.map_path = path_of(environment["__WORK_DIR__"],f"mongo/{database_name}/mappings")
        self.index_path = path_of(environment["__WORK_DIR__"],f"mongo/indexes")
        __init_schema__()
        

    '''
    Expose the client to execute singular queries (I.E: execute from IDLE)
    '''
    def client(self):
        return self.client

    def run_queries(self): 
        for query_dir in self.query_path.iterdir() :
            query_type = str(query_dir.name)
            for query_file in query_dir.iterdir():
                if query_file.name[-5:] == ".json" :
                    print(f"Mongo: Executing {query_type} query: {query_file.name}")
                    with open(str(query_file), "r") as file:
                        contents = load(file)
                        collection = contents["collection"]
                        query = contents["query"]
                        if query_type == "insert" :
                            num_rows, time = self.client.insert(collection, query)
                            print(f"Mongo: Inserted {num_rows} in {time} seconds\n")
                        elif query_type == "find" :
                            results, time = self.client.find(collection, query)
                            print(f"Mongo: Found {len(results)} rows in {time} seconds")
                            print(f"Mongo: Records:\n{"\n".join([",".join(row) for row in results])}\n")
                        elif query_type == "update":
                            num_rows, time = self.client.update(collection, query)
                            print(f"Mongo: Updated {num_rows} in {time} seconds\n")
                        else:
                            raise NotImplementedError
    
    def reset(self):
        print(f"Mongo: Resetting database {self.database_name}")
        self.client.reset()

    def index_run(self):
        for index_file in self.index_path.iterdir():
            print(f"Mongo: Applying index {index_file.name}")
            with open(str(index_file), "r") as index_f:
                # Apply index
                contents = load(index_f)
                collection = contents["collection"]
                index = contents["index"]
                self.client.index(collection, index)
                # Execute queries
                self.run_queries()
                # reset the database
                self.reset()
        print(f"Mongo: Aggregating all indexes and testing a single execution")
        for index_file in self.index_path.iterdir():
            with open(str(index_file), "r") as index_f:
                contents = load(index_f)
                collection = contents["collection"]
                index = contents["index"]
                self.client.index(collection, index)
        self.run_queries()
        self.reset()



class MySQLExecutor():

    def __init_schema__(self):
        for schema_file in self.schema_path.iterdir():
            if schema_file.name[-4:] == ".sql" :
                with open(str(schema_file), "r") as schema_f:
                    self.client.schema(schema_f.read())

    def __data__(self):
        raise NotImplementedError

    def __init___(self, environment, database_name):
        self.database_name = database_name
        self.client = mysql(environment, self.database_name)
        self.query_path = path_of(environment["__WORK_DIR__"],f"mysql/{database_name}/queries")
        self.schema_path = path_of(environment["__WORK_DIR__"],f"mysql/{database_name}/schema")
        self.map_path = path_of(environment["__WORK_DIR__"],f"mysql/{database_name}/mappings")
        self.index_path = path_of(environment["__WORK_DIR__"],f"mysql/indexes")
        __init_schema__()

    def __build_SQL_queries__(self):
        mappings = [f for f in self.map_path.iterdir() if f.name[-5:] == '.json']
        queries = {}
        for file in mappings:
            table_name = file.name[:-5].lower()
            with open(str(file), "r") as map_file:
                attributes = load(map_file)
                queries[table_name] = f"INSERT INTO {table_name} VALUES ({", ".join([":"+attributes[i] for i in attributes])})" 
        
        order = ["users","books","ratings","tags","book_tags","to_read"] if "users" in queries \
                else ["books","ratings","tags","book_tags","to_read"] # Perhaps the only hardcoded bit. MySQL Would be upset
        
        return [(table,queries[table]) for table in order] # returns list of tuples table and query

    '''
    Expose the client to execute singular queries (I.E: execute from IDLE)
    '''
    def client(self):
        return self.client

    def run_queries(self):
        for query_dir in self.query_path.iterdir() :
            query_type = str(query_dir.name)
            for query_file in query_dir.iterdir():
                if query_file.name[-4:] == ".sql" :
                    print(f"MySQL: Executing {query_type} query: {query_file.name}")
                    with open(str(query_file), "r") as file:
                        query = file.read()
                        if query_type == "insert" :
                            num_rows, time = self.client.insert(query)
                            print(f"MySQL: Inserted {num_rows} in {time} seconds\n")
                        elif query_type == "find" :
                            results, time = self.client.find(query)
                            print(f"MySQL: Found {len(results)} rows in {time} seconds")
                            print(f"MySQL: Records:\n{"\n".join([",".join(row) for row in results])}\n")
                        elif query_type == "update":
                            num_rows, time = self.client.update(query)
                            print(f"MySQL: Updated {num_rows} in {time} seconds\n")
                        else:
                            raise NotImplementedError
    
    def reset(self):
        print(f"MySQL: Resetting database {self.database_name}")
        self.client.reset()

    def index_run(self):
        for index_file in self.index_path.iterdir():
            print(f"MySQL: Applying index {index_file.name}")
            with open(str(index_file), "r") as index_f:
                # Apply index
                index = index_f.read()
                self.client.index(index)
                # Execute queries
                self.run_queries()
                # reset the database
                self.reset()
        print(f"MySQL: Aggregating all indexes and testing a single execution")
        for index_file in self.index_path.iterdir():
            with open(str(index_file), "r") as index_f:
                index = index_f.read()
                self.client.index(index)
        self.run_queries()
        self.reset()