from pathlib import Path as path_of, PurePath as concat_path
import pandas as pd
from .books import clean as clean_books
from .book_tags import clean as clean_book_tags
from .ratings import clean as clean_ratings
from .tags import clean as clean_tags
from .to_read import clean as clean_to_read

def clean():
    files = {dataset_path.name[:-4]:dataset_path for dataset_path in \
        path_of(concat_path(path_of(__file__).parent.parent, path_of("dataset_original"))).iterdir()\
            if dataset_path.name[-4:] == ".csv"}

    datasets = {dataset_name:pd.read_csv(files[dataset_name]) for \
        dataset_name in files}

    print("Cleaning books dataset")
    clean_books(datasets["books"])
    print("Cleaning book tags")
    clean_book_tags(datasets["book_tags"])
    print("Cleaning ratings")
    clean_ratings(datasets["ratings"])
    print("Cleaning tags")
    clean_tags(datasets["tags"])
    print("Cleaning to_read")
    clean_to_read(datasets["to_read"])

