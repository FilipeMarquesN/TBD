import pandas as pd
from pathlib import Path as path_of, PurePath as concat_path
"""
Returns a list of tuples containing the name of the collection
and the respective dataframe

Parameters:
- environment: dict containing environment variables
"""
def to_frames(environment):
    work_dir = environment["__WORK_DIR__"]
    data_dir = path_of(concat_path(path_of(work_dir),path_of("dataset")))
    return [(f.name[:-4],pd.read_csv(str(f))) \
         for f in data_dir.iterdir() if f.name[-4:] == ".csv"]

"""
Returns a dict of Pandas DataFrames which have it's data cleaned

Parameters:
- environment: dict containing environment variables
"""
def to_cleaned_frames(environment):
    frames = to_frames(environment)
    for name, frame in frames:
        if name == "Books":
            frame.rename(columns={"Book-Title":"Title",
                                "Book-Author":"Author",
                                "Year-Of-Publication":"YearOfPublication",
                                "Image-URL-S":"ImageSmall",
                                "Image-URL-M":"ImageMedium",
                                "Image-URL-L":"ImageLarge"},inplace=True)
        if name == "Users":
            frame.rename(columns={"User-ID":"ID","Location":"Locale"},inplace=True)
        if name == "Ratings": #only column in our data with invalid data (non existing ISBNs)
            frame.rename(columns={"User-ID":"User","Book-Rating":"Rating"},inplace=True)

    return frames
