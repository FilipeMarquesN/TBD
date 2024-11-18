#Livros que tenham saido depois de 2005
#Mongo

def query_book(query, toPrintOutput = False):
    try:
        # Execute the query
        result = db.users.find(query)
        result_count = db.users.count_documents(query)
        print(f"\n# Results: {result_count}")
        if toPrintOutput:
            for output in result:
                print(output)

    except Exception as e:
        print(f"Error querying: {e}")



### Maybe não sabemos se estass são as melhores
#Livros da Penguin Books lançados depois de 2000
query_books({"Publisher": {"$eq": "Penguin Books"},  "Year-Of-Publication": {"$gt": 2000}},toPrintOutput = True)

#Media de idades dos Users

pipeline = [
    {
        '$group': {
            '_id': '$Location',  
            'average_age': {'$avg': '$Age'},  
            'User_count': {'$sum': 1}  
        }
    },
    {
        '$sort': {'average_discount': 1}  # Sort by average discount in ascending order
    }
]
result = list(db.users.aggregate(pipeline))



##Exemplos

query_Users({"Location": {"$regex": "^usa"},  "Age": {"$gt": 20}},toPrintOutput = True)

pipeline = [
    {
        '$group': {
            '_id': '$Publisher',  
            'average_Year_of_Publication': {'$avg': '$Year-Of-Publication'},  
            'User_count': {'$sum': 1}  
        }
    },
    {
        '$sort': {'average_discount': 1}  # Sort by average discount in ascending order
    }
]
result = list(db.books.aggregate(pipeline))