from pathlib import Path as path_of, PurePath as concat_path
import pandas as pd

def clean(dataset):
    print(f"Saving the first 100k rows and dropping the rest.")
    dataset = dataset.head(100000)
    target_file = path_of(concat_path(path_of(__file__).parent.parent, path_of("dataset/tags.csv")))
    target_file.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(target_file,index=False)


