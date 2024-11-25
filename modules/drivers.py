from pymongo import MongoClient
from sqlalchemy import create_engine, text


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
    return client[environment["MONGO_DATABASE"]]


"""
Returns an sqlalchemy engine object with the database
set by the environement

Parameters:
- environment: dict containing environment variables
"""
def get_engine(environment) :
    db_url = f'mysql://{\
        environment["MYSQL_USER"]\
    }:{ environment["MYSQL_PASSWORD"]\
        }@{environment["MYSQL_HOST"]}:{environment["MYSQL_PORT"]}'
    with create_engine(db_url).connect() as conn :
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {environment["MYSQL_DATABASE"]}"))
    db_url = f'mysql://{\
        environment["MYSQL_USER"]\
    }:{ environment["MYSQL_PASSWORD"]\
        }@{environment["MYSQL_HOST"]}:{environment["MYSQL_PORT"]}/{\
            environment["MYSQL_DATABASE"]}'
    return create_engine(db_url)
