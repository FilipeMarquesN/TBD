from modules.envloader import load_env
import modules.dataset as dataset
from modules.wrapper import getWrappers

'''
Note: __file__ is a variable which stores the
path of the executed script as a string
'''
env = load_env(__file__)
frames = dataset.to_frames(env)
Mongo, Mysql = getWrappers(env)

# unimplemented, dataframes need to be cleansed before being sent to databases
#dataset.to_cleaned_frames(env)
