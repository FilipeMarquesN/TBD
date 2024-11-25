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




def getMongoWrapper(environment):
    client = get_client(environment)
    
    def is_init():
        result = client["books"].find({"Id": {"$gt":0}}).to_list()
        coll_check = [coll["name"].upper() for coll in client.list_collections().to_list()]
        return "BOOKS" in coll_check and len(result) != 0

    if not is_init():
        init_collections(client,environment["PATH_SCHEMA_MONGO"])

    def insert_data(dataset):
        for collection in dataset:
            print(f"Inserting collection {collection}")
            print("Inserting in batches of 500 records (avoid Mongo 16MB limitation)")
            to_send = []
            for index, row in dataset[collection].iterrows():
                if index+1 % 500 != 0 :
                    to_send.append(row.to_dict())
                else:
                    #try:
                        client[collection].insert_many(documents=to_send, ordered=False).inserted_ids()
                        to_send = []
                        print("*",end=" ")
                    #except Exception as e:
                    #    print(f"Couldn't insert batch no#{int(index%500)}:\nReason: {e}"
                
            #client[collection]. \
            #insert_many(dataframe.to_dict(orient="records"), ordered=False).inserted_ids()

    def insert(query_file):
        with open(query_file, "r") as query_json:
            query = load(query_json)
            
            if(type(query["query"]) == list):
                start = time()

                client[query["collection"]].insert_many(query["query"]).inserted_ids()
                end = time() - start
                return (result, end)

            elif(type(query["query"]) == dict):
                start = time()
                client[query["collection"]].insert_one(query["query"]).inserted_ids()
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
                start = time()
                client[query["collection"]].update_many(query["query"]).modified_count()
                end = time() - start
                return (result, end)

            elif(type(query["query"]) == dict):
                start = time()
                client[query["collection"]].update_one(query["query"]).modified_count()
                end = time() - start
                return (result, end)
            
    return DatabaseWrapper(
        {
            "init": is_init,
            "insert_dataset": insert_data ,
            "insert": insert,
            "query": query,
            "update": update
        }
    )


def getMySQLWrapper(environment):
    engine = get_engine(environment)

    def is_init():
        with engine.connect() as conn:
            try:
                result = conn.execute(text("SELECT * FROM books LIMIT 1;")).fetchall()
                print("books is empty " + str(result))
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
                for insert_data in dataset[dataframe].to_dict('records') :
                    try:
                        conn.execute(text(stmt),insert_data)
                    except Exception as e:
                        print(f"Couldn't insert row {insert_data}.\nReason: {e}")

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
                result = conn.execute(text(query_sql.read())) # Visit this in a bit to return everything fetchall() and all that garbage
                end = time() - start
                return (result, end)
    
    return DatabaseWrapper(
        {
            "init": is_init,
            "insert_dataset": insert_data,
            "insert": query,
            "query": query, # through sql alchemy, these methods should functionally do the same
            "update": query
        }
    )

'''
Returns 2 database Wrappers
'''
def getWrappers(environment):
    return (getMongoWrapper(environment),getMySQLWrapper(environment))
