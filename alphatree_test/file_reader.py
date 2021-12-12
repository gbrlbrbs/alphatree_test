import alphatree_test.constants as c
import datetime as dt
from pathlib import Path
import pandas as pd

class FileReader():
    def __init__(self) -> None:
        self.current_time = dt.datetime.now()
        self.staging_path = c.DATA_PATH / 'staging'
        self.processed_path = c.DATA_PATH / 'processed'

    def __remove_file(self, file: Path):
        file.unlink(missing_ok=True)

    def list_staging_files(self):
        return self.staging_path.glob('*.csv')

    def create_consolidated_file(self, df: pd.DataFrame):
        formatted_time = self.current_time.strftime('%Y-%m-%d_%H-%M-%S')
        df.to_csv(self.processed_path / f'processed_{formatted_time}.csv')

    def remove_staging_files(self):
        staging_files = self.list_staging_files()
        _ = map(self.__remove_file, staging_files)

        