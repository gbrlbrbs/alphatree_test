import alphatree_test.constants as c
import datetime as dt
from pathlib import Path
import pandas as pd
from os.path import getmtime
from time import sleep


class FileReader():
    def __init__(self) -> None:
        self.staging_path = c.DATA_PATH / 'staging'
        self.processed_path = c.DATA_PATH / 'processed'

    def __remove_file(self, file: Path):
        file.unlink(missing_ok=True)

    def list_staging_files(self):
        files = sorted(self.staging_path.glob('*.csv'), key=getmtime, reverse=True)
        return files

    def create_consolidated_file(self, df: pd.DataFrame):
        sleep(1)
        current_time = dt.datetime.now()
        formatted_time = current_time.strftime('%Y_%m_%d_%H_%M_%S')
        df.to_csv(self.processed_path / f'processed_{formatted_time}.csv')

    def remove_staging_files(self):
        staging_files = self.list_staging_files()
        _ = list(map(self.__remove_file, staging_files))

        