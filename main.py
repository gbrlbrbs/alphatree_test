import numpy as np
import pandas as pd
from alphatree_test.data_pipeline import DataPipeline

def main():
    transformations = [
        np.log,
        np.sqrt,
        str.lower
    ]
    combinator = lambda x, y: pd.concat([x, y])
    columns = [
        ['num_col1', 'num_col2'],
        ['num_col1', 'num_col3'],
        ['str_col1', 'str_col2']
    ]
    pipeline = DataPipeline(transformations, combinator, columns)
    pipeline.start_pipeline()

if __name__ == '__main__':
    main()