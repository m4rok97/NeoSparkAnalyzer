from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from NeoDatabase import *
import pyspark


class NeoSparkAnalyzer:

    def __init__(self, database_directory: str, database_password: str):
        self.database = NeoDatabase(database_directory, database_password)
        self.spark_context = pyspark.SparkContext('local[*]', appName="SparkAnalyzer")
        self.average_differences = []
        self.select_attributes = []

    def analyze(self):
        pass

    def select_attributes(self, attributes_index: list):
        return [database.data_sets[database.current_dataset]['attributes'][i] for i in list]

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

    def glance(self, selected_attributes: list):
        communities_amount = self.database.get_number_of_communities()
        anomaly_score = []
        print('ejecuta glance')
        for community in range(communities_amount):
            dict_values = {}
            for attribute in selected_attributes:
                dict_values[attribute] = self.get_average_difference(community, attribute)
            self.average_differences.append(dict_values)
        print('diffences')
        print(self.average_differences)
        print(list(range(self.database.get_number_of_communities())))
        result= self.spark_context.parallelize(range(self.database.get_number_of_communities()))\
            .map(self.test)
        print(result)
    def test(self, id):
        print(id)


    def glance_community_analysis(self, community_id):
        print('entra')
        analysis_dict = {}
        community = self.database.get_community(community_id)
        community_len = self.database.get_community_len(community_id)
        for node_i in community:
            attributes_scores = []
            for attribute in self.selected_attributes:
                average_difference = self.get_average_difference(community_id, attribute)
                acc = 0
                for node_j in self.database.get_community():
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
if __name__ == '__main__':
    analyzer = NeoSparkAnalyzer('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')

    analyzer.glance(['x', 'y'])