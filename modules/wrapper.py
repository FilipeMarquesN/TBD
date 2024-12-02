'''
Modules responsible for loading the database
parameters and establishing the connection
'''
from .drivers import get_client
from .drivers import get_engine
'''
Modules responsible for loading the schemas
for the databases
'''
from .initializers import init_collections
from .initializers import init_tables
'''
Utilities for this class
'''
from json import load
from time import time
from sqlalchemy import text
from modules.querybuilder import build_SQL_insert_queries
from copy import deepcopy as copy
from pathlib import Path as path_of

'''
Abstracts away from the concrete database
implementations
'''
class DatabaseWrapper():

    def __init__(self, operations):
        self.ops = operations

    '''
    returns True if database is initalized
    '''
    def is_init(self):
        return self.ops["init"]()

    '''
    Parameters:
    - dataset: list of pairs (name, dataframe) as returned
    by dataset.to_frames
    '''
    def insert_dataframe(self, dataset):
        return self.ops["insert_dataset"](dataset)

    
    '''
    Parameters:
    - query_file: file path to a json or sql file that contains
    valid queries
    '''
    def insert(self, query_file):
        return self.ops["insert"](dataset)

    '''
    Returns a pair containing the result of the query and the time to execute

    Parameters:
    - query_file: file path to a json or sql file that contains
    valid queries
    '''
    def query(self, query_file):
        return self.ops["query"](query_file)

    '''
    Needed for mongo
    - Parameters:
    ?
    '''
    def update(self,query_file):
        return self.ops["update"](query_file)

    '''
    Executes the index queries defined in the external folder
    '''
    def index(self):
        self.ops["index"]()


def getMongoWrapper(environment):
    client = get_client(environment)

    def is_init():
        result = client["books"].find({"Id":{"$gt":0}}).to_list()
        return len(result) != 0
    
    def insert_data(dataset):
        for collection in dataset:
            print(f"Inserting collection {collection}")
            print("Inserting in batches of 1000 records (avoid Mongo 16MB limitation)")
            records = dataset[collection].to_dict(orient="records")
            inserted = 0
            while(inserted < len(records)):
                client[collection]. \
                    insert_many(records[inserted:(inserted+1000)], ordered=False).inserted_ids
                inserted = inserted + 1000

    def insert(query_file):
        with open(query_file, "r") as query_json:
            query = load(query_json)
            
            if(type(query["query"]) == list):
                start = time()

                client[query["collection"]].insert_many(query["query"]).inserted_ids
                end = time() - start
                return (result, end)

            elif(type(query["query"]) == dict):
                start = time()
                client[query["collection"]].insert_one(query["query"]).inserted_ids
                end = time() - start
                return (result, end)


    def query(query_file):
        with open(query_file, "r") as query_json:
            query = load(query_json)
            
            if(type(query["query"]) == list):
                start = time()
                result = client[query["collection"]].aggregate(query["query"]).to_list()
                end = time() - start
                return (result, end)
            
            elif(type(query["query"]) == dict):
                start = time()
                result = client[query["collection"]].find(query["query"]).to_list()
                end = time() - start
                return (result, end)
                
    def update(query_file):
        with open(query_file, "r") as query_json:
            query = load(query_json)

            if(type(query["query"]) == list):
                if("$" in list(query["query"][0].keys())[0]):
                    pipeline = query["query"]
                    cpipeline = copy(pipeline)
                    cpipeline.pop(len(cpipeline)-1)
                    result = len(client[query["collection"]].aggregate(cpipeline).to_list())
                    start = time()
                    client[query["collection"]].aggregate(pipeline)
                    end = time() - start
                else:
                    start = time()
                    result = client[query["collection"]].update_many(query["query"]).modified_count
                    end = time() - start
                return (result, end)

            elif(type(query["query"]) == dict):
                start = time()
                result = client[query["collection"]].update_one(query["query"]).modified_count
                end = time() - start
                return (result, end)

    def index():
        index_path = path_of(environment["PATH_INDEX_MONGO"])
        indexes = [f for f in index_path.iterdir() if f.name[-5:] == ".json"]
        for index_file in indexes:
            print(f"Mongo: Applying index {index_file.name}")
            with open(str(index_file), "r") as query_json:
                query = load(query_json)
                coll = query["collection"]
                index = query["index"] # should be a dict/JSON object
                client[coll].create_index([(key,index[key]) for key in index]) # packs everything

            
    return DatabaseWrapper(
        {
            "init": is_init,
            "insert_dataset": insert_data ,
            "insert": insert,
            "query": query,
            "update": update,
            "index": index
        }
    )


def getMySQLWrapper(environment):
    engine = get_engine(environment)

    def is_init():
        with engine.connect() as conn:
            try:
                result = conn.execute(text("SELECT * FROM books LIMIT 1;")).fetchall()
                #print("books is empty " + str(result))
                return len(result) != 0
            except Exception as e:
                print(e)
                return False
    
    if not is_init():
        init_tables(engine,environment["PATH_SCHEMA_MYSQL"])

    def insert_data(dataset):
        order = ["books","ratings","tags","book_tags","to_read"] #non agnostic to simplify
        queries =  build_SQL_insert_queries(environment)
        with engine.connect() as conn:
            for dataframe in order:
                print(f"Inserting data for {dataframe}")
                stmt = queries[dataframe]
                try:
                    data = dataset[dataframe].to_dict('records')
                    print(f"Inserting {len(data)} records")
                    conn.execute(text(stmt),data)
                except Exception as e:
                    print(e)
            conn.commit()

    def insert(query_file):
        with open(query_file, "r") as query_sql:
            with engine.connect() as conn:   
                start = time()
                result = conn.execute(text(query_sql.read())).inserted_primary_key
                end = time() - start
                return (result, end)

    def query(query_file):
        with open(query_file, "r") as query_sql:
            with engine.connect() as conn:   
                start = time()
                result = conn.execute(text(query_sql.read())).fetchall()
                end = time() - start
                return (result, end)

    def update(query_file):
        with open(query_file, "r") as query_sql:
            with engine.connect() as conn:   
                start = time()
                conn.execute(text(query_sql.read())) # Visit this in a bit to return everything fetchall() and all that garbage
                result = conn.execute(text("SELECT ROW_COUNT();")).fetchone().tuple()[0]
                end = time() - start
                return (result, end)

    def index():
        with engine.connect() as conn:
            index_path = path_of(environment["PATH_INDEX_MYSQL"])
            indexes = [f for f in index_path.iterdir() if f.name[-4:] == ".sql"]
            for index_file in indexes:
                print(f"MySQL: Applying index {index_file.name}")
                with open(str(index_file), "r") as query_sql:
                    result = conn.execute(text(query_sql.read()))
    
    return DatabaseWrapper(
        {
            "init": is_init,
            "insert_dataset": insert_data,
            "insert": insert,
            "query": query, # through sql alchemy, these methods should functionally do the same
            "update": update,
            "index": index
        }
    )

'''
Returns 2 database Wrappers
'''
def getWrappers(environment):
    return (getMongoWrapper(environment),getMySQLWrapper(environment))
