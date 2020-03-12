from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from py2neo import Graph
from NeoDatabase import *


class NeoSparkAnalyzer:

    def __init__(self, database_directory: str, database_password: str):
        self.database = NeoDatabase(database_directory, database_password)

    def analyze(self):
        pass

if __name__ == __main__:
    analyzer = NeoSparkAnalyzer('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')