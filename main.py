from modules.envloader import load_env
from modules.query import QueryExecutor
from pathlib import Path
'''
Note: __file__ is a variable which stores the
path of the executed script as a string
'''
env = load_env(__file__)

if not Path(env["PATH_DATASET"]).exists():
    from dataset_utils.clean_dataset import clean
    print(f"Dataset directory not found."+\
        "\nThis means datasets weren't properly cleaned")
    clean()
    print(f"Successfully cleaned the datasets")

qe = QueryExecutor(env)

'''
At this point you can just run the queries files placed in the
queries directory from here
'''
a = 0
while(a != "leave"):
    a = input("Select an option:\n\t- 'find' : Run all find queries\n\t- 'insert' : Run all insert queries\n\t- 'update' : Run all update queries\n\t- 'index' : Apply Indexes to both databases\n\t- 'all' : Performs a full execution: runs every single option above(Repeats runs after index)\n\t- 'leave' : terminate this loop and quit execution\n$")
    if(a == "find"):
        qe.execute_find()
    elif(a == "insert"):
        qe.execute_insert()
    elif(a == "update"):
        qe.execute_update()
    elif(a == "index"):
        qe.execute_indexes()
    elif(a == "all"):
        qe.index_test()



