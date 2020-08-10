from pyspark.sql import SparkSession
from mimesis import Person
import itertools


def quick_pyspark_df(cols, num_rows, spark = SparkSession.builder.getOrCreate()):
    valid_cols = ['first_name', 'last_name']
    # add a check to make sure the cols are a subset of valid_cols
    if not (set(cols) <= set(valid_cols)):
        raise ValueError(f"The valid column values are '{valid_cols}'.  You tried to use these cols '{cols}'.")
    en = Person('en')
    mapping = {'first_name': en.first_name, 'last_name': en.last_name}
    def funs():
        return tuple(map(lambda col: mapping[col](), cols))
    data = []
    for _ in itertools.repeat(None, num_rows):
        data.append(funs())
    return spark.createDataFrame(data, cols)



