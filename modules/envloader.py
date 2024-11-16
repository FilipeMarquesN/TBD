from pathlib import Path as path_of, PurePath as concat_path

"""
Loads up .env file as a python dict.
Avoids using other dependencies like dot-env
Appends __WORK_DIR__ value to env so the other modules
know how to work relative paths

Parameters:
- path_of_script: Path to the script which shares
    it's directory with the environment to be loaded
"""
def load_env(path_of_script):
    work_dir = path_of(path_of_script).parent
    env = str(concat_path(work_dir,path_of(".env")))
    ret = {"__WORK_DIR__":str(work_dir)}
    try:
        with open(env, "r" ) as file:
            for line in file:
                l = line.strip().split("=")
                if len(l) == 2 :
                    key, value = l
                    ret[key] = value
            return ret
    except FileNotFoundError:
        print(f"Error: Environment file doesn't exist.")
    except IOError as e:
        print(f"Error reading environment file.")