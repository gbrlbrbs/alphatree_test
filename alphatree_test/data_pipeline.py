import pandas as pd
from alphatree_test.file_reader import FileReader
from typing import Iterable, Callable
from functools import reduce


class DataPipeline():
    def __init__(self, transformations: Iterable[Callable], combinator: Callable, columns_function: Iterable[Iterable[str]]) -> None:
        self.file_reader = FileReader()
        self.transformations = transformations
        self.combinator = combinator
        self.dataframes = []
        self.columns_function = columns_function

    def __get_data(self):
        files = self.file_reader.list_staging_files()
        self.dataframes = list(map(pd.read_csv, files))
    
    def __transform_data(self):
        for df in self.dataframes:
            dfs = [df[columns].apply(f) for f, columns in zip(self.transformations, self.columns_function)]
            df = reduce(self.combinator, dfs)

    def __load_data(self):
        _ = list(map(self.file_reader.create_consolidated_file, self.dataframes))
    
    def start_pipeline(self):
        self.__get_data()
        self.__transform_data()
        self.__load_data()
        self.file_reader.remove_staging_files()