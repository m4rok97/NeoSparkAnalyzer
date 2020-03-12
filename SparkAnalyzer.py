from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add
import pyspark
from operator import add

class SparkAnalyzer:

    def __init__(self):
        pass

    def analyze(self):
        sc = pyspark.SparkContext('local[*]', appName="Pi")
        sum = sc.parallelize(range(100)).reduce(lambda x, y: x + y)
        print(sum)

if __name__ == '__main__':
    analyzer = SparkAnalyzer()
    analyzer.analyze()