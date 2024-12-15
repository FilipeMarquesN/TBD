from pymongo import MongoClient
from sqlalchemy import create_engine, text
from time import time

'''
Abstraction from the Mongo connection
'''
class MongoDatabaseWrapper():

    def __init__(self, environment, database_name):
        url = f"mongodb://{\
        environment["MONGO_USER"]}:{environment["MONGO_PASSWORD"]\
            }@{environment["MONGO_HOST"]}:{environment["MONGO_PORT"]}"
        self.client = MongoClient(url)
        self.name = database_name
        self.db = self.client[self.name]
        self.schema_dict = {}

    def insert_dataset(self, dataset = None):
        if dataset != None:
            self.dataset = dataset
        if self.dataset == None:
            raise Exception(f"Mongo: {self.name}: No insertion dataset was defined for database {self.name}")
        for collection in self.dataset:
            print(f"Mongo: {self.name}: Inserting collection {collection}")
            print(f"Mongo: {self.name}: Inserting in batches of 1000 records (avoid Mongo 16MB limitation)")
            records = dataset[collection].to_dict(orient="records")
            inserted = 0
            while(inserted < len(records)):
                self.dbclient[collection]. \
                    insert_many(records[inserted:(inserted+1000)], ordered=False).inserted_ids
                inserted = inserted + 1000

    def schema(self, collection, schema):
        if collection not in self.schema_dict:
            self.schema_dict[collection] = schema
        self.client.create_collection(collection, validator=schema)

    def reset(self):
        self.client.drop_database(self.name)
        self.db = self.client[self.name]
        for collection in self.schema_dict : # Reinitialize the schema
            self.schema(collection, self.schema_dict[collection]) 
        insert_dataset()

    def insert(collection, query):
        if(type(query) == list):
            if("$" in list(query[0].keys())[0]): # If the list is an aggregate list and not a list of objects
                pipeline = query
                cpipeline = copy(pipeline)
                if list(query[0].keys())[len(query[0]-1)] == "$merge" :  
                    cpipeline.pop(len(cpipeline)-1) # If the pipeline ends in $merge, pop it so we can calculate updated records
                else :
                    print(f"Mongo: {self.name}: Warning: insert query for collection {collection} contains no $merge step. Won't produce any inserted records")
                result = len(self.db[collection].aggregate(cpipeline).to_list())
                start = time()
                self.db[collection].aggregate(pipeline)
                end = time() - start
            else:
                start = time()
                self.db[collection].insert_many(query).inserted_ids  ## Change this to use the solution from class Might not report the correct number of inserted records
                end = time() - start
            return (result, end)

        elif(type(query) == dict):
            start = time()
            self.db[collection].insert_one(query).inserted_ids
            end = time() - start
            return (result, end)


    def find(collection, query):    
        if(type(query) == list):
            start = time()
            result = self.db[collection].aggregate(query).to_list()
            end = time() - start
            return (result, end)
        
        elif(type(query) == dict):
            start = time()
            result = self.db[collection].find(query).to_list()
            end = time() - start
            return (result, end)
                
    def update(collection, query):
        if(type(query) == list):
            if("$" in list(query[0].keys())[0]): # If the list is an aggregate list and not a list of objects
                pipeline = query
                cpipeline = copy(pipeline)
                if list(query[0].keys())[len(query[0]-1)] == "$merge" :  
                    cpipeline.pop(len(cpipeline)-1) # If the pipeline ends in $merge, pop it so we can calculate updated records
                else :
                    print(f"Mongo: {self.name}: Warning: update query for collection {collection} contains no $merge step. Won't produce any updated records")
                result = len(self.db[collection].aggregate(cpipeline).to_list())
                start = time()
                self.db[collection].aggregate(pipeline)
                end = time() - start
            else:
                start = time()
                result = self.db[collection].update_many(query).modified_count
                end = time() - start
            return (result, end)

        elif(type(query) == dict):
            start = time()
            result = self.db[collection].update_one(query).modified_count
            end = time() - start
            return (result, end)

    def index(collection, index):
        self.db[collection].create_index(index) 


'''
Abstraction from the MySQL connection
'''
class MySQLDatabaseWrapper():

    def __get_db_url__(self):
        return f"{self.url}/{self.name}"

    def __init__(self, environment, database_name):
        self.url = f'mysql://{environment["MYSQL_USER"]}:{ environment["MYSQL_PASSWORD"]\
                    }@{environment["MYSQL_HOST"]}:{environment["MYSQL_PORT"]}'
        self.name = database_name
        with create_engine(db_url).connect() as conn :
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.name}"))
        self.engine = create_engine(__get_db_url__())

    '''
    insert_queries is a list of tuples of (table_name,SQL_query_string)
    ordered by table_name to avoid foreign key constraint breaks
    '''
    def insert_dataset(self, dataset = None, insert_queries = None):
        if dataset != None:
            self.dataset = dataset
        if self.dataset == None:
            raise Exception(f"MySQL: {self.name}: No insertion dataset was defined for database {self.name}")
        if insert_queries != None:
            self.insert_queries = insert_queries
        if self.insert_queries == None:
            raise Exception(f"MySQL: {self.name}: No insertion queries were dfined for database {self.name}")
        with self.engine.connect as conn:
            for pair in self.insert_queries:
                table, stmt = pair
                print(f"MySQL: {self.name}: Inserting data for {table}")
                try:
                    data = self.dataset[table].to_dict('records')
                    print(f"MySQL: {self.name}: Inserting {len(data)} records")
                    conn.execute(text(stmt),data)
                except Exception as e:
                    print(e)
            conn.commit()
            

    def schema(self, schema = None):
        if schema != None :
            self.schema_string = schema
        if self.schema_string == None:
            raise Exception(f"No schema was defined for database {self.name}")
        with self.engine.connect() as conn:
            conn.execute(text(self.schema_string))

    def reset(self):
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE {self.name};CREATE DATABASE {self.name};"))
        self.engine = create_engine(__get_db_url__())
        self.schema()
        insert_dataset()

    def insert(query):
        with self.engine.connect() as conn:   
            start = time()
            conn.execute(text(query))
            result = conn.execute(text("SELECT ROW_COUNT();")).fetchone().tuple()[0]
            end = time() - start
            return (result, end)

    def query(query):
        with self.engine.connect() as conn:   
            start = time()
            result = conn.execute(text(query)).fetchall()
            end = time() - start
            return (result, end)

    def update(query):
        with self.engine.connect() as conn:   
            start = time()
            conn.execute(text(query))
            result = conn.execute(text("SELECT ROW_COUNT();")).fetchone().tuple()[0]
            end = time() - start
            return (result, end)

    def index(index):
        with self.engine.connect() as conn:
            result = conn.execute(text(index))


'''
build mongo data from joining sql tables and returning that as a dataset
(Delivery 2 Optmization)
'''
def mongo_mysql_insert(db_wrapper):
    raise NotImplementedError