from .wrapper import getWrappers
from .dataset import to_mapped_frames

'''
Returns Initialized Wrappers
'''
def get_initialized_wrappers(environment):
    Mongo, Mysql = getWrappers(environment)

    if not Mongo.is_init() or not Mysql.is_init() : #If the databases aren't initialized with data
        print("Either one or both databases weren't initialized with any data. Inserting datasets")
        dataframes = to_mapped_frames(environment) # Fetch the data from the datasets
        if Mongo.is_init():
            print("Mongo was already initialized")
        else:
            Mongo.insert_dataframe(dataframes) # Initialize Mongo with data
            print("MongoDB initialized with data")
        if Mysql.is_init():
            print("Mysql was already initialized")
        else:
            Mysql.insert_dataframe(dataframes) # Initialize Mysql with data
            print("MySQL initialized with data")    
    else :
        print("Databases are already loaded with data")

    return (Mongo,Mysql)
