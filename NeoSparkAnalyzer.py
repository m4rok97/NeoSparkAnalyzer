from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from AnomalyDetection import *
from NeoDatabase import *
import pyspark
import numpy as np

# region NeoSaprk Analyzer Class


class NeoSparkAnalyzer:

    def __init__(self, database_directory: str, database_password: str):
        self.database = NeoDatabase(database_directory, database_password)
        self.spark_context = pyspark.SparkContext('local[*]', appName="SparkAnalyzer")
        self.average_differences = []
        self.select_attributes = []

    def select_attributes(self, attributes_index: list):
        return [self.database.data_sets[self.database.current_dataset]['attributes'][i] for i in list]

    def set_selected_attributes(self, attributes):
        self.select_attributes = attributes

    def show(self):
        print([x for x in self.database.get_communities()])

    def get_communities_with_method(self, method_name: str):
        self.database.apply_community_method(method_name)

    def analyze(self, method: str, selected_attributes: list, use_feature_selection=False, percentile=0.1):
        # Select Method
        anomaly_algorithm = None
        if method == 'Glance':
            anomaly_algorithm = glance

        anomaly_score = []
        result = self.spark_context.parallelize(self.database.get_communities(selected_attributes, use_feature_selection, percentile))\
            .map(anomaly_algorithm).collect()

        for community in result:
            for node_id, anomaly_score in community:
                node = self.database.get_node_by_id(node_id)
                node['anomalyScore'] = anomaly_score
                self.database.update_data(node)

# endregion

# region Main


if __name__ == '__main__':
    analyzer = NeoSparkAnalyzer('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    analyzer.database.load_dataset('Disney')
    analyzer.get_communities_with_method('Louvain')
    analyzer.analyze('Glance', ['neighborsAmount'])
    analyzer.database.set_anomaly_label(0.9)

# endregion
