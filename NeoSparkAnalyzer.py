from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from NeoDatabase import *
import pyspark
import numpy as np


class NeoSparkAnalyzer:

    def __init__(self, database_directory: str, database_password: str):
        self.database = NeoDatabase(database_directory, database_password)
        self.spark_context = pyspark.SparkContext('local[*]', appName="SparkAnalyzer")
        self.average_differences = []
        self.select_attributes = []

    def analyze(self):
        pass

    def select_attributes(self, attributes_index: list):
        return [self.database.data_sets[self.database.current_dataset]['attributes'][i] for i in list]

    def set_selected_attributes(self, attributes):
        self.select_attributes = attributes

    def get_average_difference(self, community: int, attribute: str):
        nodes_i = self.database.get_attribute_from_community_nodes(community, attribute)
        community_len = self.database.get_community_len(community).evaluate()
        acc = 0
        for node_i in nodes_i:
            node_i_id = node_i['nodeId']
            node_i_attribute_val = node_i['attribute']
            nodes_j = self.database.get_attribute_from_community_nodes(community, attribute)
            for node_j in nodes_j:
                node_j_id = node_j['nodeId']
                node_j_attribute_val = node_j['attribute']
                if node_i_id != node_j_id:
                    acc += abs(node_i_attribute_val - node_j_attribute_val)
        return acc // community_len

    def shpw(self):
       print ([x for x in self.database.get_communities()])

    def get_communities_with_method(self, method_name: str):
        self.database.apply_community_method(method_name)

    def glance(self, selected_attributes: list, use_feature_selection=False, percentile=0.1):
        anomaly_score = []
        result = self.spark_context.parallelize(self.database.get_communities(selected_attributes, use_feature_selection, percentile))\
            .map(glance_over_community).collect()

        for community in result:
            for node_id, anomaly_score in community:
                node = self.database.get_node_by_id(node_id)
                node['anomalyScore'] = anomaly_score
                self.database.update_data(node)

    def glance_community_analysis(self, community_id):
        print('entra')
        analysis_dict = {}
        community = self.database.get_community_nodes(community_id)
        community_len = self.database.get_community_len(community_id)
        for node_i in community:
            attributes_scores = []
            for attribute in self.selected_attributes:
                average_difference = self.get_average_difference(community_id, attribute)
                acc = 0
                for node_j in self.database.get_community_nodes():
                    node_i_attribute = node_i[attribute]
                    node_j_attribute = node_j[attribute]
                    difference = abs(node_i_attribute - node_j_attribute)
                    if difference > average_difference:
                        acc += 1
                attribute_score = acc / community_len
                attributes_scores.append(attribute_score)
            print(attributes_scores)
            anomaly_score = max(attributes_scores)
            print(anomaly_score)
            node_i['anomaly score'] = anomaly_score
            self.database.update_data(node_i)
        return 1








#region Statics Methods

def get_average_difference(community: list, attribute: str):
    community_len = len(community)
    acc = 0

    for node_i in community:
        node_i_attribute_val = node_i[attribute]
        for node_j in community:
            print('Attribute')
            print(attribute)
            [print('Node data')]
            print(node_i)
            print(node_j)
            node_j_attribute_val = node_j[attribute]
            print('Values')
            print(node_i_attribute_val)
            print(node_j_attribute_val)
            acc += abs(node_i_attribute_val - node_j_attribute_val)
    return acc / community_len**2


def get_average_differences(community: list):
    differences = {}
    for attribute in filter(lambda x: x != 'nodeId', community[0]):
        differences[attribute] = get_average_difference(community, attribute)
    return differences


def glance_over_community(community: list):
    average_differences = get_average_differences(community)
    community_len = len(community)
    ans = []

    for node_i in community:
        print('Node')
        print(node_i)
        attributes_scores = []
        node_i_id = node_i['nodeId']
        for attribute in filter(lambda x: x != 'nodeId', node_i):
            average_difference = average_differences[attribute]
            acc = 0
            for node_j in community:
                node_j_id = node_j['nodeId']
                if node_i_id == node_j_id:
                    continue

                node_i_attribute = node_i[attribute]
                node_j_attribute = node_j[attribute]
                print('difference')
                difference = abs(node_i_attribute - node_j_attribute)
                print('difference: ' + str(difference))
                print('average difference: ' + str(average_difference))

                if difference > average_difference:
                    print('entra')
                    acc += 1
            print('acc: ' + str(acc))
            print('community len: ' + str(community_len))
            attribute_score = acc / community_len
            attributes_scores.append(attribute_score)
            print('attributes scores: ' + str(attributes_scores))
        anomaly_score = max(attributes_scores)
        ans.append((node_i['nodeId'], anomaly_score))
    return ans

#endregion



if __name__ == '__main__':
    analyzer = NeoSparkAnalyzer('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    analyzer.database.load_dataset('Disney')
    analyzer.get_communities_with_method('Louvain')
    analyzer.glance(['MinPriceUsedItem', 'MinPricePrivateSeller', 'Avg_Helpful', 'Avg_Rating'], True)
    analyzer.database.set_anomaly_label(0.9)