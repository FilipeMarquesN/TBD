from .initializer import get_initialized_wrappers
from pathlib import Path as path_of, PurePath as concat_path

def remove_ext(filename):
    if filename[-4:] == ".sql":
        return filename[:-4] # remove '.sql'
    else: 
        return filename[:-5] # remove '.json'

def padOutput(results):
    if len(results) > 5:
        return results[:5] + ["..."]
    return results

'''
Query Executor:
Takes in Wrappers for the databases and then executes
the respective 
'''
class QueryExecutor():

    def __init__(self, environment):
        mongo, mysql = get_initialized_wrappers(environment)
        self.mongo = mongo
        self.mysql = mysql
        self.mongo_path = environment["PATH_QUERY_MONGO"]
        self.mysql_path = environment["PATH_QUERY_MYSQL"]
        
    '''
    selector unimplemented
    '''
    def execute_find(self, selector=-1):
        target_mongo = path_of(concat_path(path_of(self.mongo_path),path_of("find")))
        target_mysql = path_of(concat_path(path_of(self.mysql_path),path_of("find")))
        # dictionaries mapping the filename to the file path (use the filename as key to run both tests at once)

        queries_mongo = {remove_ext(file.name):str(file) for file in target_mongo.iterdir() if file.name[-5:] == ".json" }
        queries_mysql = {remove_ext(file.name):str(file) for file in target_mysql.iterdir() if file.name[-4:] == ".sql" }
        for query in queries_mongo:
            # execute pairs, throw warnings when theres not respective pair
            if query in queries_mysql:
                results_mongo, time_mongo = self.mongo.query(queries_mongo[query])
                results_mysql, time_mysql = self.mysql.query(queries_mysql[query])
                print(f"Mongo: Query(find) {query} : Found {len(results_mongo)} in {time_mongo} seconds")
                print(f"Mongo: printing the first 5 results:\n{padOutput(results_mongo)}")
                print(f"Mysql: Query(find) {query} : Found {len(results_mysql)} in {time_mysql} seconds")
                print(f"Mysql: printing the first 5 results:\n{padOutput(results_mysql)}")
            else:
                print(f"Warning(find): {query} not found in MySQL Query Directory ")
        for query in queries_mysql:
            # execute pairs, throw warnings when theres not respective pair
            if query not in queries_mongo: # Every matched query ran in first loop. This loop reports queries that exist in mysql but dont exist for mongo
                print(f"Warning(find): {query} not found in Mongo Query Directory ")

    def execute_insert(self, selector=-1):
        target_mongo = path_of(concat_path(path_of(self.mongo_path),path_of("insert")))
        target_mysql = path_of(concat_path(path_of(self.mysql_path),path_of("insert")))
        # dictionaries mapping the filename to the file path (use the filename as key to run both tests at once)
        queries_mongo = {remove_ext(file.name):str(file) for file in target_mongo.iterdir() if file.name[-5:] == ".json" }
        queries_mysql = {remove_ext(file.name):str(file) for file in target_mysql.iterdir() if file.name[-4:] == ".sql" }
        for query in queries_mongo:
            # execute pairs, throw warnings when theres not respective pair
            if query in queries_mysql:
                results_mongo, time_mongo = self.mongo.insert(queries_mongo[query])
                results_mysql, time_mysql = self.mysql.insert(queries_mysql[query])
                print(f"Mongo: Query(insert) {query} : Inserted {len(results_mongo)} in {time_mongo} seconds")
                print(f"Mysql: Query(insert) {query} : Inserted {len(results_mysql)} in {time_mysql} seconds")
            else:
                print(f"Warning(insert): {query} not found in MySQL Query Directory ")
        for query in queries_mysql:
            # execute pairs, throw warnings when theres not respective pair
            if query not in queries_mongo: # Every matched query ran in first loop. This loop reports queries that exist in mysql but dont exist for mongo
                print(f"Warning(insert): {query} not found in Mongo Query Directory ")

    def execute_update(self, selector=-1):
        target_mongo = path_of(concat_path(path_of(self.mongo_path),path_of("update")))
        target_mysql = path_of(concat_path(path_of(self.mysql_path),path_of("update")))
        # dictionaries mapping the filename to the file path (use the filename as key to run both tests at once)
        queries_mongo = {remove_ext(file.name):str(file) for file in target_mongo.iterdir() if file.name[-5:] == ".json" }
        queries_mysql = {remove_ext(file.name):str(file) for file in target_mysql.iterdir() if file.name[-4:] == ".sql" }
        for query in queries_mongo:
            # execute pairs, throw warnings when theres not respective pair
            if query in queries_mysql:
                results_mongo, time_mongo = self.mongo.update(queries_mongo[query])
                results_mysql, time_mysql = self.mysql.update(queries_mysql[query])
                print(f"Mongo: Query(update) {query} : Updated {results_mongo} in {time_mongo} seconds")
                print(f"Mysql: Query(update) {query} : Updated {results_mysql} in {time_mysql} seconds")
            else:
                print(f"Warning(update): {query} not found in MySQL Query Directory ")
        for query in queries_mysql:
            # execute pairs, throw warnings when theres not respective pair
            if query not in queries_mongo: # Every matched query ran in first loop. This loop reports queries that exist in mysql but dont exist for mongo
                print(f"Warning(update): {query} not found in Mongo Query Directory ")
