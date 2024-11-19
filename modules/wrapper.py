'''
Modules responsible for loading the database
parameters and establishing the connection
'''
from .drivers.mongo import get_client
from .drivers.mysql import get_engine
'''
Modules responsible for loading the schemas
for the databases
'''
from .initializers.mongo import init_collections
from .initializers.mysql import init_tables
'''
Utilities for this class
'''
from json import load
from time import time
from sqlalchemy import text

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
    init_collections(client,environment["PATH_SCHEMA_MONGO"])

    def is_init():
        result = client["Books"].find().to_list()
        #print(result)
        return len(result) != 0

    def insert_data(dataset):
        for collection, dataframe in dataset:
            print(f"Inserting collection {collection}")
            for index, row in dataframe.iterrows():
                try:
                    client[collection].insert_one(row.to_dict())
                except Exception as e:
                    print(f"Couldn't insert record no#{index}:{row}\nReason: {e}")
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
    init_tables(engine,environment["PATH_SCHEMA_MYSQL"])

    def is_init():
        with engine.connect() as conn:
            try:
                result = conn.execute(text("SELECT * FROM Users LIMIT 1;")).fetchall()
                #print(result)
                return len(result) != 0
            except:
                return False

    def insert_data(dataset):
        for collection, dataframe in dataset:
            print(f"Inserting collection {collection}")
            dataframe.to_sql(collection, con=engine, if_exists='replace', index=False, method='multi')
            # ISSUE: Hes gonna complain about the file order fix this later

    def query(query_file):
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
