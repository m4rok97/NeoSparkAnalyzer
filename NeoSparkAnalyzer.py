from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from NeoDatabase import *
import pyspark


class NeoSparkAnalyzer:

    def __init__(self, database_directory: str, database_password: str):
        self.database = NeoDatabase(database_directory, database_password)
        self.spark_context = pyspark.SparkContext('local[*]', appName="SparkAnalyzer")

    def analyze(self):
        pass

    def select_attributes(self, attributes_index: list):
        return [database.data_sets[database.current_dataset]['attributes'][i] for i in list]

    def average_difference(self, ):

    def glance(self, communities, selected_attributes):


        self.spark_context.parallelize

if __name__ == '__main__':
    analyzer = NeoSparkAnalyzer('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')