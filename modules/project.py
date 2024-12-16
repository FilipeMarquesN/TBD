from .executors import MongoExecutor as mongo, MySQLExecutor as mysql
import pandas as pd
from pathlib import Path as path_of, PurePath as concat_path
from json import load, dump
from sqlalchemy.sql import null

"""
Returns a list of tuples containing the name of the collection
and the respective dataframe

Parameters:
- environment: dict containing environment variables
"""
def to_frames(environment, version):
    work_dir = environment["__WORK_DIR__"]
    data_dir = path_of(concat_path(path_of(work_dir),path_of(f"dataset/{version}")))
    return {f.name[:-4]:pd.read_csv(str(f)) \
         for f in data_dir.iterdir() if f.name[-4:] == ".csv"}

"""
Returns a dict of Pandas DataFrames which have it's data cleaned

Parameters:
- environment: dict containing environment variables
"""
def to_mapped_frames(environment, version):
    frame_map = to_frames(environment, version)
    # apply mappings
    for key in frame_map :
        try:
            target_file = concat_path(environment["PATH_MAPPINGS"],path_of(key+".json"))
            with open(target_file, "r") as mapping_file:
                print(f"{key}'s mappings file was found. Applying map.")
                mapping = load(mapping_file)
                frame_map[key].rename(columns=mapping,inplace=True)
        except FileNotFoundError:
            print(f"{key} has no mappings file. Skipping.")
        except Exception as e:
            print(e)
    return frame_map






def getBookset1(environment):

    dataset = to_mapped_frames(environment, "bookset_1")
    print("\n")
    return {
        "mongo": mongo(environment,"bookset_1", dataset),
        "mysql": mysql(environment,"bookset_1", dataset)
    }

def getBookset2(environment):

    dataset = to_mapped_frames(environment, "bookset_2")
    print("\n")
    mysql_exec = mysql(environment,"bookset_2", dataset)
    mongo_dataset = mysql_exec.produce_mongo_dataset()
    return {
        "mongo": mongo(environment, "bookset_2", mongo_dataset),
        "mysql": mysql_exec
    }
