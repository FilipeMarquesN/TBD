from modules.envloader import load_env
import modules.dataset as dataset
from modules.drivers.mongo import get_client
from modules.drivers.mysql import get_engine

'''
Note: __file__ is a variable which stores the
path of the executed script as a string
'''
env = load_env(__file__)
frames = dataset.to_frames(env)

print("Testing if mongo connection works")
try:
    db = get_client(env)
    collec = db["garbage"]
    collec.insert_one({"garbage":"data"})
    print("Inserted garbage data successfully")
except:
    print("Driver doesn't work")

print("Testing if mysql connection works")
try:
    get_engine(env)
    print("Mysql didnt raise any issues")
except:
    print("Driver doens't work")

print(frames)

# unimplemented, dataframes need to be cleansed before being sent to databases
dataset.to_cleaned_frames(env)
