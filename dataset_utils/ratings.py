from pathlib import Path as path_of, PurePath as concat_path
import pandas as pd
from json import load


def get_bookdata():
    with open("book_data.json", "r") as discarded:
        return load(discarded)



def clean(dataset):
    bookdata = get_bookdata()
    discard_rows = dataset[dataset["book_id"].isin(bookdata["discarded"])]
    print(f"Discarding {len(discard_rows)}: no such book.\nWould break FK constraints in MySQL")
    dataset = dataset[~dataset["book_id"].isin(bookdata["discarded"])]
    print(f"Dropping duplicates\nWould break PK constraints in MySQL")
    dataset.drop_duplicates(subset = ["book_id","user_id"], inplace=True)
    print(f"Dropping any records which don't have a matching book")
    dataset = dataset[dataset["book_id"].isin(bookdata["list"])]
    print(f"Saving the first 100k rows and dropping the rest.")
    dataset = dataset.head(100000)
    target_file = path_of(concat_path(path_of(__file__).parent.parent, path_of("dataset/ratings.csv")))
    target_file.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(target_file,index=False)