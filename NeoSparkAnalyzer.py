from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from AnomalyDetection import *
from NeoDatabase import *
import pyspark
import time as tm
import numpy as np

# region NeoSaprk Analyzer Class


class NeoSparkAnalyzer:
    """
    Class that represent the main Analyzer Framework.Whit it you can perfrom comunty detection over
    graph after dafining the datasets path
    """
    def __init__(self, database_directory: str, database_password: str):
        self.database = NeoDatabase(database_directory, database_password)
        self.spark_context = pyspark.SparkContext('local[*]', appName="SparkAnalyzer")
        # self.spark_context = None
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

    @staticmethod
    def apply_CADA():
        analyzer.analyze('CADA', ['neighborsCommunityVector'])

    @staticmethod
    def apply_InterScore():
        analyzer.analyze('Glance', ['outerCommunityNeighborsAmount'], use_cache_data=True)

    @staticmethod
    def apply_SubSpaceGlance():
        analyzer.analyze('Glance', [], use_feature_selection=True)

    @staticmethod
    def apply_Glance_with_all_attributes():
        analyzer.analyze('Glance', [])

    def analyze(self, method: str, selected_attributes: list, use_feature_selection=False, percentile=0.1, use_cache_data=False):
        # Select Method
        anomaly_algorithm = None
        if method == 'Glance':
            anomaly_algorithm = glance
        if method == 'CADA':
            anomaly_algorithm = CADA

        anomaly_score_dict = {}
        result = self.spark_context.parallelize(self.database.get_communities(selected_attributes, use_feature_selection, percentile, use_cache_data))\
            .map(anomaly_algorithm).collect()

        for community in result:
            for node_id, anomaly_score in community:
                # node = self.database.get_node_by_id(node_id)
                # node['anomalyScore'] = anomaly_score
                # self.database.update_data(node)
                anomaly_score_dict[node_id] = anomaly_score

        with open('testing_result.json', 'wt', encoding='utf8') as file:
            json.dump(anomaly_score_dict, file)
# endregion

# region Main


if __name__ == '__main__':
    analyzer = NeoSparkAnalyzer('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    # analyzer.database.load_dataset('Bitcoin')
    # print(analyzer.database.current_dataset)
    t = tm.time()
    analyzer.get_communities_with_method('Louvain')
    # print(analyzer.database.current_dataset)
    # analyzer.analyze('CADA', ['neighborsCommunityVector'])
    # analyzer.database.set_anomaly_label(0.9)
    # analyzer.apply_InterScore()

    # analyzer.analyze('Glance', ['MinPriceUsedItem', 'MinPricePrivateSeller', 'Avg_Helpful', 'Avg_Rating'])
    # analyzer.apply_Glance_with_all_attributes()
    analyzer.apply_SubSpaceGlance()
    t = tm.time() - t
    print(t)

    # from  disney_experiments import test_disney
    # test_disney()
# endregion
