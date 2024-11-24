from modules.envloader import load_env
from modules.query import QueryExecutor
'''
Note: __file__ is a variable which stores the
path of the executed script as a string
'''
env = load_env(__file__)

qe = QueryExecutor(env)

"""
'''
At this point you can just run the queries files placed in the
queries directory from here
'''
a = 0
while(a != "leave"):
    a = input("Select an option:\n\t- 'find' : Run all find queries\n\t- 'insert' : Run all insert queries\n\t- 'update' : Run all update queries\n\t- 'leave' : terminate this loop and quit execution\n$")
    if(a == "find"):
        qe.execute_find()
    elif(a == "insert"):
        qe.execute_insert()
    elif(a == "update"):
        qe.execute_update()
"""


