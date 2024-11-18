'''
Modules responsible for loading the database
parameters and establishing the connection
'''
from drivers.mongo import get_client
from drivers.mysql import get_engine
'''
Modules responsible for loading the schemas
for the databases
'''
from initializers.mongo import init_collections
from initializers.mysql import init_tables
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
    Parameters:
    - dataset: list of pairs (name, dataframe) as returned
    by dataset.to_frames
    '''
    def insert(self, dataset):
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
    def update(self):
        raise NotImplementedError




def getMongoWrapper(environment):
    client = get_client(environment)
    init_collections(client,environment["PATH_SCHEMA_MONGO"])

    def insert(dataset):
        for collection, dataframe in dataset:
            client[collection]. \
            insert_many(dataframe.to_dict(orient="records"), ordered=False)

    def query(query_file):
        with open(query_file, "r") as query_json:
            query = load(query_json)
            
            if(type(query["query"]) == list):
                start = time()
                result = client[query["collection"]].aggregate(query["query"])
                end = time() - start
                return (result, end)
            
            elif(type(query["query"]) == dict):
                start = time()
                result = client[query["collection"]].find(query["query"])
                end = time() - start
                return (result, end)
                
    return DatabaseWrapper(
        {
            "insert": insert ,
            "query": query
        }
    )


def getMySQLWrapper(environemnt):
    engine = get_engine(environment)
    init_tables(engine,environment["PATH_SCHEMA_MYSQL"])

    def insert(dataset):
        for collection, dataframe in dataset:
            dataframe.to_sql(collection, con=engine, if_exists='replace', index=False, method='multi')
            # ISSUE: Hes gonna complain about the file order fix this later

    def query(query_file):
        with open(query_file, "r") as query_sql:
            with engine.connect() as conn:   
                start = time()
                result = conn.execute(text(query_sql.read()))
                end = time() - start
                return (result, end)
    
    return DatabaseWrapper(
        {
            "insert": insert,
            "query": query
        }
    )


'''
Returns 2 database Wrappers
'''
def getWrappers(environment):
    return (getMongoWrapper(environment),getMySQLWrapper(environment))
