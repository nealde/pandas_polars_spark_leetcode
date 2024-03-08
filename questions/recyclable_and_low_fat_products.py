import pandas as pd
import polars as pl
from pyspark.sql import SparkSession

from .base_question import Question

class TestRecyclableAndLowFatProducts(Question):
    def setup_problem(self) -> pd.DataFrame:
        '''
        Write a solution to find the ids of products that are both low fat and recyclable.
        '''
        data = [['0', 'Y', 'N'], ['1', 'Y', 'Y'], ['2', 'N', 'Y'], ['3', 'Y', 'Y'], ['4', 'N', 'N']]
        products = pd.DataFrame(data, columns=['product_id', 'low_fats', 'recyclable']).astype({'product_id':'int64', 'low_fats':'category', 'recyclable':'category'})
        return products

    def pandas_solution(self, products):
        return products[(products['low_fats'] == 'Y') & (products['recyclable'] == 'Y')][['product_id']]

    def polars_solution(self, products):
        products = pl.from_pandas(products)
        return products.filter(pl.col(('low_fats')) == 'Y').filter(pl.col(('recyclable')) == 'Y').select(pl.col('product_id'))
    
    def spark_solution(self, products):
        spark = SparkSession.builder.getOrCreate()
        products = spark.createDataFrame(products)
        products.createOrReplaceTempView('products')
        return pd.DataFrame(spark.sql('SELECT product_id FROM products WHERE low_fats = "Y" AND recyclable = "Y"').collect())


# class TestClass:
#     def test_one(self):
#         x = "this"
#         assert "h" in x

#     def test_two(self):
#         x = "hello"
#         assert hasattr(x, "check")