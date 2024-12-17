from modules.envloader import load_env
from modules.project import getBookset1 as entrega1, getBookset2 as entrega2
'''
Note: __file__ is a variable which stores the
path of the executed script as a string
'''
env = load_env(__file__)

executors = entrega1(env)

print("\n\n First Run: Delivery 1\n\n")

executors["mongo"].run_queries()
executors["mysql"].run_queries()

executors = entrega2(env)

print("\n\n First Run: Delivery 2 (No Index)\n\n")

executors["mongo"].run_queries()
executors["mysql"].run_queries()
executors["mongo"].reset()
executors["mysql"].reset()

print("\n\n Second Run: Delivery 2 (Indexes)\n\n")

executors["mongo"].index_run()
executors["mysql"].index_run()





