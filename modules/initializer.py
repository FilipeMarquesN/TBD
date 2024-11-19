from .wrapper import getWrappers
from .dataset import to_cleaned_frames

'''
Returns Initialized Wrappers
'''
def get_initialized_wrapper(environment):
    Mongo, Mysql = getWrappers(environment)

    if not Mongo.is_init() or not Mysql.is_init() : #If the databases aren't initialized with data
        print("Databases aren't initialized with any data. Inserting datasets")
        dataframes = to_cleaned_frames(environment) # Fetch the data from the datasets

        Mongo.insert_dataframe(dataframes) # Initialize Mongo with data
        print("MongoDB initialized with data")
        Mysql.insert_dataframe(dataframes) # Initialize Mysql with data
        print("MySQL initialized with data")    
    else :
        print("Databases are already loaded with data")

    return (Mongo,Mysql)
