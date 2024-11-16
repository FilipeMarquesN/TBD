from pymongo import MongoClient
"""
Returns a MongoClient object with the desired
database

Parameters:
- environment: dict containing environment variables
""" 
def get_client(environment):
    url = f"mongodb://{\
        environment["MONGO_USER"]}:{environment["MONGO_PASSWORD"]\
            }@{environment["MONGO_HOST"]}:{environment["MONGO_PORT"]}"
    client = MongoClient(url)
    client.drop_database(environment["MONGO_DATABASE"])
    return client[environment["MONGO_DATABASE"]]