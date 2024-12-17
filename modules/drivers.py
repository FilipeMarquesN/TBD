from pymongo import MongoClient
from sqlalchemy import create_engine, text
from time import time
from copy import deepcopy as copy

'''
Abstraction from the Mongo connection
'''
class MongoDatabaseWrapper():

    def __init__(self, environment, database_name):
        url = f"mongodb://{\
        environment["MONGO_USER"]}:{environment["MONGO_PASSWORD"]\
            }@{environment["MONGO_HOST"]}:{environment["MONGO_PORT"]}"
        self.client = MongoClient(url)
        self.client.drop_database(database_name)
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
            records = self.dataset[collection].to_dict(orient="records") if type(self.dataset[collection]) != list else self.dataset[collection]
            inserted = 0
            while(inserted < len(records)):
                self.db[collection]. \
                    insert_many(records[inserted:(inserted+1000)], ordered=False).inserted_ids
                inserted = inserted + 1000

    def schema(self, collection, schema):
        if collection not in self.schema_dict:
            self.schema_dict[collection] = schema
        self.db.create_collection(collection, validator=schema)

    def reset(self):
        self.client.drop_database(self.name)
        self.db = self.client[self.name]
        for collection in self.schema_dict : # Reinitialize the schema
            self.schema(collection, self.schema_dict[collection]) 
        self.insert_dataset()

    def insert(self,collection, query):
        if(type(query) == list):
            if("$" in list(query[0].keys())[0]): # If the list is an aggregate list and not a list of objects
                pipeline = query
                cpipeline = copy(pipeline)
                if list(query[0].keys())[len(query[0])-1] == "$merge" :  
                    cpipeline.pop(len(cpipeline)-1) # If the pipeline ends in $merge, pop it so we can calculate updated records
                #else :
                #    print(f"Mongo: {self.name}: Warning: insert query for collection {collection} contains no $merge step. Won't produce any inserted records")
                result = len(self.db[collection].aggregate(cpipeline).to_list())
                start = time()
                self.db[collection].aggregate(pipeline)
                end = time() - start
            else:
                start = time()
                result = self.db[collection].insert_many(query).inserted_ids  ## Change this to use the solution from class Might not report the correct number of inserted records
                end = time() - start
            return (result, end)

        elif(type(query) == dict):
            start = time()
            result = self.db[collection].insert_one(query).inserted_id
            end = time() - start
            return (result, end)


    def find(self,collection, query):    
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
                
    def update(self,collection, query):
        if(type(query) == list):
            if("$" in list(query[0].keys())[0]): # If the list is an aggregate list and not a list of objects
                pipeline = query
                cpipeline = copy(pipeline)
                if list(query[0].keys())[len(query[0])-1] == "$merge" :  
                    cpipeline.pop(len(cpipeline)-1) # If the pipeline ends in $merge, pop it so we can calculate updated records
                #else :
                #    print(f"Mongo: {self.name}: Warning: update query for collection {collection} contains no $merge step. Won't produce any updated records")
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

    def index(self,collection, index):
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
        with create_engine(self.url).connect() as conn :
            conn.execute(text(f"DROP DATABASE IF EXISTS {self.name}; CREATE DATABASE IF NOT EXISTS {self.name}"))
        self.engine = create_engine(self.__get_db_url__())

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
        with self.engine.connect() as conn:
            for pair in self.insert_queries:
                table, stmt = pair
                print(f"MySQL: {self.name}: Inserting data for {table}")
                try:
                    data = self.dataset[table].to_dict('records')
                    print(f"MySQL: {self.name}: Inserting {len(data)} records")
                    conn.execute(text(stmt),data)
                except KeyError as e:
                    continue
                except Exception as e:
                    print(str(e))
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
            conn.execute(text(f"DROP DATABASE IF EXISTS {self.name};CREATE DATABASE IF NOT EXISTS {self.name};"))
        self.engine = create_engine(self.__get_db_url__())
        self.schema()
        self.insert_dataset()

    def insert(self,query):
        with self.engine.connect() as conn:   
            start = time()
            conn.execute(text(query))
            result = conn.execute(text("SELECT ROW_COUNT();")).fetchone().tuple()[0]
            end = time() - start
            return (result, end)

    def find(self,query):
        with self.engine.connect() as conn:   
            start = time()
            result = conn.execute(text(query)).fetchall()
            end = time() - start
            return (result, end)

    def update(self,query):
        with self.engine.connect() as conn:   
            start = time()
            conn.execute(text(query))
            result = conn.execute(text("SELECT ROW_COUNT();")).fetchone().tuple()[0]
            end = time() - start
            return (result, end)

    def index(self,index):
        with self.engine.connect() as conn:
            result = conn.execute(text(index))


'''
build mongo data from joining sql tables and returning that as a dataset
(Delivery 2 Optmization)
'''
def mongo_mysql_insert(db_wrapper):
    dataset = {}

    books_fields = ["Id","BookId","BestBook","BookTitleId","BooksCount","Isbn","Isbn13","Authors","PublicationYear","OriginalTitle","Title","LanguageCode",
    "Rating","RatingCount","BookTitleRatingCount","BookTitleReviewsCount","Ratings1","Ratings2","Ratings3","Ratings4","Ratings5","ImageURL","SmallImageURL",
    "Tags","UserRatings"]

    user_fields = ["UserId","UserName","UserSurname","Readlist","Ratings"]

    rating_row = 12

    with db_wrapper.engine.connect() as conn:
        books = conn.execute(text('''SELECT * FROM books''')).fetchall()
        books1 = []
        for book in books:
            tags = conn.execute(text(f"SELECT t.TagName FROM book_tags bt LEFT JOIN tags t ON bt.TagId = t.Id WHERE bt.GoodreadsBookId = {book[0]}")).fetchall() 
            tags = [tag[0] for tag in tags]
            ratings = conn.execute(text(f"SELECT UserId, Rating FROM ratings WHERE BookId = {book[0]}")).fetchall()
            books1.append([*book[:12], float(book[12]), *book[13:], list(tags) , [{"UserId":rating[0],"Rating":float(rating[1])} for rating in ratings] if len(ratings) != 0 else []])
        dataset["books"] = [{books_fields[i]:book[i] for i in range(len(book))} for book in books1]
        users = conn.execute(text('''
        SELECT p1.UserId, p1.UserName, p1.UserSurname, p1.Readlist, GROUP_CONCAT(CONCAT(CONVERT(r.BookId,CHAR),",",CONVERT(r.Rating,CHAR)) SEPARATOR '|') AS Ratings
        FROM (SELECT u.UserId, u.UserName, u.UserSurname, GROUP_CONCAT(CONVERT(tr.BookId,CHAR) SEPARATOR ' , ') AS Readlist 
        FROM users u LEFT JOIN to_read tr ON u.UserId = tr.UserId GROUP BY u.UserId) p1 LEFT JOIN ratings r ON p1.UserId = r.UserId GROUP BY p1.UserId;
        ''')).fetchall()
        read_index = len(users[0]) - 2
        ratings_index = len(users[0]) - 1
        # Ratings columns is incomplete sometimes therefore we must treat it
        u1 = []
        for user in users:
            try:
                u1.append((*user[:read_index], user[read_index].split(' , ') if user[read_index] != None else []\
            , [{"BookId":rating.split(',')[0],"Rating":float(rating.split(',')[1])} for rating in user[ratings_index].split('|')] if user[ratings_index] != None else [] ))
            except Exception as e:
                ratings = conn.execute(text(f"SELECT BookId, Rating FROM ratings WHERE UserId = {user[0]}")).fetchall()
                u1.append((*user[:read_index], user[read_index].split(' , ') if user[read_index] != None else []\
            , [{"BookId":rating[0],"Rating":float(rating[1])} for rating in ratings] ))
        dataset["users"] = [{user_fields[i]:user[i] for i in range(len(user))} for user in u1]
        return dataset
