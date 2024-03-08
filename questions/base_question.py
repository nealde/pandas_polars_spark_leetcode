import pandas as pd
import polars as pl
import pyspark
from pyspark.sql import SparkSession

class Question:
    def setup_problem(self):
        raise NotImplementedError
    
    def pandas_solution(self, *args):
        raise NotImplementedError
    
    def polars_solution(self, *args):
        raise NotImplementedError
    
    def spark_solution(self, *args):
        raise NotImplementedError

    def test_answers(self):
        problem = self.setup_problem()
        p1 = self.pandas_solution(problem)
        p2 = self.polars_solution(problem).to_pandas()
        p3 = self.spark_solution(problem)

        p2.index = p1.index
        p3.index = p1.index
        assert p1.equals(p2)
        assert p1.equals(p3)
        
        