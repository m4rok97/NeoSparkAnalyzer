from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add
import pyspark
from operator import add
import time
from py2neo import Graph

class SparkAnalyzer:

    def __init__(self):
        self.context = pyspark.SparkContext('local[*]', appName="SparkAnalyzer")

    def analyze(self):
        sum = self.context.parallelize(self.powGenerator(1, 10)).reduce(lambda x, y: x + y)
        print(sum)

    def powGenerator(self, base: int, limit: int):
        for i in range(10):
            yield base ** i

    def suma(self):
        sum = 0
        for i in range(1000000000000):
            sum += i
        print(sum)

    def get

    def glance(self, cummunities, selected_attributes):



if __name__ == '__main__':
    analyzer = SparkAnalyzer()
    init = time.time()
    analyzer.analyze()
    end = time.time() - init
    print('Parallel Time ' + str(end))

    # init = time.time()
    # analyzer.suma()
    # end = time.time() - init
    # print('Normal Time ' + str(end))
