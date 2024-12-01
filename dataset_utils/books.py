from pathlib import Path as path_of, PurePath as concat_path
import pandas as pd
from json import dump


def write_ignore_list(book_dict):
    with open("book_data.json", "w") as discarded:
        dump(book_dict,discarded)


def clean(dataset):
    null_mask = dataset.isnull().any(axis=1)
    null_rows = dataset[null_mask]
    discarded = [id for id in null_rows['id']]
    dataset.dropna(inplace=True)
    print(f"Dropped {len(discarded)} books with null columns")
    book_list = [id for id in dataset['id']]
    book_id_list = [id for id in dataset['book_id']]
    book_dict = {"discarded" : discarded, "list": book_list,"book_id":book_id_list}
    write_ignore_list(book_dict)
    target_file = path_of(concat_path(path_of(__file__).parent.parent, path_of("dataset/books.csv")))
    target_file.parent.mkdir(parents=True, exist_ok=True)
    dataset["original_publication_year"] = dataset["original_publication_year"].astype(int)
    dataset.to_csv(target_file,index=False)


