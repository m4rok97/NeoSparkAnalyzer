import py2neo as p2n
from py2neo import Graph
from utils import *
import json
import os
import functools as ft
import numpy as np
from skfeature.utility.construct_W import construct_W

#region NeoDatabase Class

class NeoDatabase:

    def __init__(self, database_directory: str, database_password: str):
        self.database_directory = database_directory
        self.data_sets_registry_directory = database_directory + '/data_sets.json'

        # Initialize the database directory json file
        if 'data_sets.json' not in os.listdir(self.database_directory):
            with open(self.data_sets_registry_directory, 'x', encoding='utf8') as data_sets_registry:
                dic = {}
                json.dump(dic, data_sets_registry)
        with open(self.data_sets_registry_directory, 'rt', encoding='utf8') as data_sets_registry:
            self.data_sets = json.load(data_sets_registry)

        self.graph = Graph(password=database_password)
        self.current_dataset = ''

    def load_dataset(self, data_set_name):
        if not self.current_dataset:
            self.unload_current_dataset()
        if data_set_name in self.data_sets:
            self.graph.run('call apoc.import.graphml(\'%s\', {readLabels: true, storeNodeIds: true})' % self.data_sets[data_set_name]['directory'])
            self.current_dataset = data_set_name
        else:
            Exception('No dataset with the given name')

    def unload_current_dataset(self):
        self.graph.delete_all()
        self.current_dataset = ''

    def apply_community_method(self, method_name='Louvain'):
        print(self.current_dataset)
        print(self.data_sets)
        relationship_name = self.data_sets[self.current_dataset]['relationship_name']

        if method_name == 'Louvain':
            self.graph.run('call algo.louvain.stream(null, "%s") yield nodeId, community match (n) where id(n) = nodeId set n.community = community' % relationship_name).data()
        elif method_name == 'UnionFind':
            self.graph.run('call algo.unionFind.stream(null, "%s") yield nodeId, setId match (n) where id(n) = nodeId set n.setId = setId' % relationship_name).data()
        elif method_name == 'LabelPropagation':
            self.graph.run('call algo.labelPropagation.stream(null, "%s", { iterations: 10}) yield nodeId, label match (n) where id(n) = nodeId set n.setId = setId' % relationship_name).data()

        for node_data in self.get_nodes_id():
            node_id = node_data['nodeId']
            print('NodeId')
            print(node_id)
            result_inner_data = self.get_inner_community_neighbors_of_node_amount(node_id)
            print(result_inner_data)
            print(type(result_inner_data))
            node = result_inner_data['n']
            inner_community_neighbors_amount = result_inner_data['innerCommunityNeighborsAmount']
            node['innerCommunityNeighborsAmount'] = inner_community_neighbors_amount
            self.update_data(node)
            result_outer_data = self.get_outer_community_neighbors_of_node_amount(node_id)
            outer_community_neighbors_amount = result_outer_data['outerCommunityNeighborsAmount']
            node['outerCommunityNeighborsAmount'] = outer_community_neighbors_amount
            self.update_data(node)

            neighbors_community_vector = []
            for community in range(self.get_number_of_communities()):
                neighbors_community_vector.append(self.get_specific_community_neighbors_amount(node_id, community))
                print(neighbors_community_vector)
                print(type(neighbors_community_vector[0]))
            node['neighborsCommunityVector'] = neighbors_community_vector
            self.update_data(node)

        self.save_nodes_attributes()

    def save_dataset(self, dataset_name: str, directory: str, relationship_name: str):
        """
        Save a dataset into the datatsets registry and move the dataset to the the Neo4j
        database's import folder.
        :param dataset_name: The name of the dataset
        :param directory: The current directory of the dataset
        :return: The result of the load
        """
        try:
            new_directory = self.database_directory + '/import'
            copy_file(directory, new_directory)
            self.data_sets[dataset_name] = {'directory': directory.split('/')[-1], 'relationship_name': relationship_name}
            with open(self.data_sets_registry_directory, 'wt', encoding='utf8') as data_sets_registry:
                json.dump(self.data_sets, data_sets_registry)
            return True
        except FileNotFoundError:
            print('Database not found in the given directory')
            return False

    def get_node_by_id(self, node_id: int):
        return self.graph.run('match (n) where id(n) = %s return n' % node_id).evaluate()

    def get_number_of_communities(self):
        return self.graph.run('match (n) return count (distinct n.community)').evaluate()

    def get_communities_list(self):
        return [self.get_community_nodes(i).to_data_frame() for i in range(self.get_number_of_communities())]

    def get_communities(self, attributes: list, use_laplacian_score=False, percentile=0.1):
        for i in range(self.get_number_of_communities()):
            yield self.get_community_nodes(i, attributes, use_laplacian_score).data()

    def get_neighbors_of_node(self, node_id: int):
        return self.graph.run('match (n)--(m) where id(n) = %s return m' % node_id)

    def get_nodes_id(self):
        return self.graph.run('match (n) return id(n) as nodeId')

    def get_neighbors_of_node_amount(self, node_id: int):
        return self.graph.run('match (n)--(m) where id(n) = %s return count(m)' % node_id)

    def get_inner_community_neighbors_of_node_amount(self, node_id: int):
        result = self.graph.run('match (n)--(m) where id(n) = %s and n.community = m.community  return n, count(m) as innerCommunityNeighborsAmount' % node_id)\
            .data()

        if result:
            return result[0]

        node = self.get_node_by_id(node_id)
        return {'n': node, 'innerCommunityNeighborsAmount': 0}

    def get_outer_community_neighbors_of_node_amount(self, node_id: int):
        result = self.graph.run('match (n)--(m) where id(n) = %s and n.community <> m.community return n, count(m) as outerCommunityNeighborsAmount' % node_id)\
            .data()

        if result:
            return result[0]

        node = self.get_node_by_id(node_id)
        return {'n': node, 'outerCommunityNeighborsAmount': 0}

    def get_specific_community_neighbors_amount(self, node_id:int, community:int):
        return self.graph.run('match (n)--(m) where id(n) = %s and m.community = %s return count(m)' % (node_id, community)).evaluate()

    def get_nodes_attributes(self):
        return list(self.graph.run('match (n) return n limit 1').evaluate().keys())

    def get_nodes_amount_of_community(self, community: int):
        return self.graph.run('match (n) where n.community = %s return count(n)' % community).evaluate()

    def get_community_nodes(self, community: int, attributes=[], use_laplacian_score=False, percentile=0.1):
        if not attributes:
            attributes = self.get_nodes_attributes()

        if attributes[0] == 'neighborsAmount':
            return self.graph.run('match (n)--(m) where n.community = %s return count(m) as neighborsAmount, id(n) as '
                                  'nodeId' % community)

        if use_laplacian_score:
            attributes = self.laplacian_score(community, attributes, percentile)

        select_query = ft.reduce(lambda x, y: x + y, ['n.' + attribute + ' as ' + attribute + ', ' for attribute in attributes])
        # end_select_query = 'n.innerCommunityNeighborsAmount as innerCommunityNeighborsAmount, n.outerCommunityNeighborsAmount as outerCommunityNeighborsAmount, id(n) as nodeId'
        end_select_query = 'id(n) as nodeId'
        select_query += end_select_query
        return self.graph.run('match (n) where n.community = %s return ' % community + select_query)

    def get_community_len(self, community: int):
        return self.graph.run('match (n) where n.community = %s return count(n)' % community).evaluate()

    def get_attribute_from_community_nodes(self, community: int, attribute: str):
        return self.graph.run('match (n) where n.community = %s return n.%s as attribute, id(n) as nodeId' % (community, attribute))

    def set_anomaly_label(self, threshold: float):
        self.graph.run('match (n) where n.anomalyScore > %s set n:Rare' % threshold)

    def laplacian_score(self, community:int, attributes: list, percentile=0.1):
        result = []
        nodes_amount = self.get_nodes_amount_of_community(community)
        community_as_matrix = np.empty((nodes_amount, len(attributes)))
        community_nodes = self.get_community_nodes(community)
        node_index = 0
        for node in community_nodes:
            for attribute_index in range(len(attributes)):
                community_as_matrix[node_index, attribute_index] = node[attributes[attribute_index]]
            node_index += 1

        if nodes_amount >= 5:
            w_matrix = construct_W(community_as_matrix)
        else:
            w_matrix = construct_W(community_as_matrix, k=(nodes_amount - 1))

        scores = lap_score(community_as_matrix, W=w_matrix)
        ranked_attributes = feature_ranking(scores)
        boundary = len(attributes) * percentile

        for i in range(len(attributes)):
            if ranked_attributes[i] < boundary:
                result.append(attributes[i])

        return result

    def update_data(self, data):
        self.graph.push(data)

    def save_nodes_attributes(self):
        self.data_sets[self.current_dataset]['attributes'] = self.get_nodes_attributes()
        with open(self.data_sets_registry_directory, 'wt', encoding='utf8') as data_sets_registry:
            json.dump(self.data_sets, data_sets_registry)

#endregion

#region Main

if __name__ == '__main__':
    database = NeoDatabase('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    # database.save_dataset('Disney', 'C:/Users/Administrator/Desktop/Disney.graphml', '_default')
    # database.save_dataset('Airlines', 'C:/Users/Administrator/Desktop/airlines.graphml', 'RELATED')

    # database.load_dataset('Disney')
    # database.unload_current_dataset()
    # database.load_dataset('Airlines')

    # print(database.current_dataset)
    # print(database.data_sets)
    # louvain_result = database.apply_method('Louvain')

    n = database.get_node_by_id(1854)
    dict = {'n': n}
    dict['amount'] = 0
    print(dict)

    # result = database.laplacian_score(0, ['MinPriceUsedItem', 'MinPricePrivateSeller', 'Avg_Helpful', 'Avg_Rating'])
    # print(result)

#endregion

