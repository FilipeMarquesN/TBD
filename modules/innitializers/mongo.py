
from pymongo import MongoClient

"""
Create the collection book with the data validation.

Requires database, a object type from MongoClient
"""
def bookCollectionInit(database):
    database.create_collection("books", validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['ISBN', 'Book-Title','Book-Author'],
        'properties': {
            'ISBN': {
                'bsonType': 'string',
                'description': 'must be a string and is required'
            },
            'Book-Title': {
                'bsonType': 'string',
                'description': 'must be a valid title and is required'
            },
            'Book-Author': {
                'bsonType': 'string',
                'description': 'must be a stringif provided'
            },
            'Year-Of-Publication': {
                'bsonType': 'int',
                'description': 'must be an int if provided'
            },
            'Publisher': {
                'bsonType': 'string',
                'description': 'must be an string if provided'
            },
            'Image-URL-S': {
                'bsonType': 'string',
                'description': 'must be an string if provided'
            },
            'Image-URL-M': {
                'bsonType': 'string',
                'description': 'must be an string if provided'
            },
            'Image-URL-L': {
                'bsonType': 'string',
                'description': 'must be an string if provided'
            }
        }
    }
})
def reviewCollectionInit(database):
    database.create_collection("reviews", validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['User-ID','ISBN', 'Book-Rating'],
        'properties': {
            'User-ID': {
                'bsonType': 'int',
                'description': 'must be a int and is required'
            },
             'ISBN': {
                'bsonType': 'int',
                'description': 'must be a int and is required'
            },
            'Book-Rating': {
                'bsonType': 'int',
                'minimum': 0,
                'maximum':10,
                'description': 'must be a int and is required'
            }
        }
    }
})

def userCollectionInit(database):
    database.create_collection("users", validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['User-ID'],
        'properties': {
            'User-ID': {
                'bsonType': 'int',
                'description': 'must be a int and is required'
            },
             'Location': {
                'bsonType': 'String',
                'description': 'must be a string if provided'
            },
            'Age': {
                'bsonType': 'int',
                'minimum': 0,
                'description': 'must be a int if provided'
            }
        }
    }
})
