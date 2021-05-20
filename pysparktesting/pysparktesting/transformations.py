import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import quinn
from typing import Callable

def hello_world(df):
	return df.withColumn("Hello", F.lit("world!"))
def with_clean_first_name(df):
    return df.withColumn(
        "clean_first_name",
        quinn.remove_non_word_characters(F.col("first_name"))
    )
