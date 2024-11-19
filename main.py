from modules.envloader import load_env
from modules.initializer import get_initialized_wrapper as getWrappers
'''
Note: __file__ is a variable which stores the
path of the executed script as a string
'''
env = load_env(__file__)

Mongo, Mysql = getWrappers(env)

'''
From here onward evrything should be easy since
you can jus tdo
Mongo.insert()
Mysql.insert()
Mongo.query() ...
Mongo.update() ...

TODO: modules that read from the queries and launch
them on the wrappers
QOL feat: down the line add some mechanism to prevent
adding the same data unnecessarily (no inserts if already exists)
'''
