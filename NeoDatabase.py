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

        # self.graph = Graph(password=database_password, port=11004, max_connections=1000000)
        self.graph = None
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
        relationship_name = self.data_sets[self.current_dataset]['relationship_name']
        # relationship_name = 'RELATED'

        if method_name == 'Louvain':
            self.graph.run('call algo.louvain.stream(null, "%s") yield nodeId, community match (n) where id(n) = nodeId set n.community = community' % relationship_name).data()
        elif method_name == 'UnionFind':
            self.graph.run('call algo.unionFind.stream(null, "%s") yield nodeId, setId match (n) where id(n) = nodeId set n.setId = setId' % relationship_name).data()
        elif method_name == 'LabelPropagation':
            self.graph.run('call algo.labelPropagation.stream(null, "%s", { iterations: 10}) yield nodeId, label match (n) where id(n) = nodeId set n.setId = setId' % relationship_name).data()

        for node_data in self.get_nodes_id():
            node_id = node_data['nodeId']
            result_inner_data = self.get_inner_community_neighbors_of_node_amount(node_id)
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
            node['neighborsCommunityVector'] = neighbors_community_vector
            self.update_data(node)

        self.save_nodes_attributes()


    def get_inner_community_neighbors_amount_dictionary(self):
        for node_data in self.get_nodes_id():
            node_id = node_data['nodeId']

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

    def get_communities(self, attributes: list, use_laplacian_score=False, percentile=0.1, use_cache_data=False):
        if use_cache_data:
            current = 1
            for community in self.load_cache_dictionary('bitcoin_community_data'):
                print('communtiy ', current)
                current += 1
                yield community
        else:
            for i in range(self.get_number_of_communities()):
                yield self.get_community_nodes(i, attributes, use_laplacian_score).data()

    def get_neighbors_of_node(self, node_id: int):
        return self.graph.run('match (n)--(m) where id(n) = %s return m' % node_id)

    def get_nodes_id(self):
        return self.graph.run('match (n) return id(n) as nodeId')

    def get_nodes_without_inner_community_neighbors(self):
        return self.graph.run('match (n) where n.innerCommunityNeighborsAmount is null  return id(n) as nodeId')

    def get_nodes_without_inner_community_neighbors_amount(self):
        return self.graph.run('match (n) where n.innerCommunityNeighborsAmount is null  return count(n)').evaluate()

    def get_neighbors_of_node_amount(self, node_id: int):
        return self.graph.run('match (n)--(m) where id(n) = %s return count(m)' % node_id)

    def get_inner_community_neighbors_of_node_amount(self, node_id: int):
        result = self.graph.run('match (n)--(m) where id(n) = %s and n.community = m.community  return count(m) as innerCommunityNeighborsAmount' % node_id)\
            .data()

        if result:
            return result[0]

        node = self.get_node_by_id(node_id)
        return {'n': node, 'innerCommunityNeighborsAmount': 0}

    def get_all_nodes_and_their_inner_cummonity_neighbors_amount(self):
        return self.graph.run('match (n)--(m) where n.community = m.community return id(n) as id, count(m) as innerCommunityNeighborsAmount ')

    def get_all_nodes_and_their_outer_cummonity_neighbors_amount(self):
        return self.graph.run('match (n)--(m) where n.community <> m.community return id(n) as id, count(m) as outerCommunityNeighborsAmount ')

    def get_outer_community_neighbors_of_node_amount(self, node_id: int):
        result = self.graph.run('match (n)--(m) where id(n) = %s and n.community <> m.community return count(m) as outerCommunityNeighborsAmount' % node_id)\
            .data()

        if result:
            return result[0]

        node = self.get_node_by_id(node_id)
        return {'n': node, 'outerCommunityNeighborsAmount': 0}

    def  get_specific_community_neighbors_amount(self, node_id:int, community:int):
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

    def save_outer_community_amount_dictionary(self, dataset: str):
        dict = {}
        for node in database.get_all_nodes_and_their_outer_cummonity_neighbors_amount():
            dict[node['id']] = node['outerCommunityNeighborsAmount']
        with open(dataset + '_outer_community.json', 'wt', encoding='utf8') as file:
            json.dump(dict, file)

    def save_inner_community_amount_dictionary(self, dataset: str):
        dict = {}
        for node in database.get_all_nodes_and_their_inner_cummonity_neighbors_amount():
            dict[node['id']] = node['innerCommunityNeighborsAmount']
        with open(dataset + '_inner_community.json', 'wt', encoding='utf8') as file:
            json.dump(dict, file)

    def fill_cache_dictionary_with_value(self, dict_name: str, value=0):
        with open(dict_name + '.json', 'rt', encoding='utf8') as file:
            dict = json.load(file)
        node_i = 1
        for node in database.get_nodes_id():
            node_i += 1
            if str(node['nodeId']) not in dict:
                dict[str(node['nodeId'])] = value
        with open(dict_name + '.json', 'wt', encoding='utf8') as file:
            json.dump(dict, file)

    def get_nodes_and_their_communities(self):
        return self.graph.run('match (n) return id(n) as nodeId, n.community as community')

    def get_all_neighbors(self):
        return self.graph.run('match (n)--(m) return distinct id(n) as node_1, id(m) as node_2')

    @staticmethod
    def load_cache_dictionary(dict_name: str):
        with open(dict_name + '.json', 'rt', encoding='utf8') as file:
            dictionary = json.load(file)
        return dictionary

    @staticmethod
    def save_cache_dictionary(dictionary: dict, dict_name: str):
        with open(dict_name + '.json', 'wt', encoding='utf8') as file:
            json.dump(dictionary, file)

    def save_neighbors_pairs(self, dataset_name):
        dictionary = {}
        current = 1

        for pair in self.get_all_neighbors():
            print(current)
            node_1_id = pair['node_1']
            node_2_id = pair['node_2']
            if node_1_id in dictionary:
                dictionary[node_1_id].append(node_2_id)
            else:
                dictionary[node_1_id] = [node_2_id]
            current += 1

        self.save_cache_dictionary(dictionary , dataset_name + '_neighbors_pairs')


    def save_community_dictionaries(self, dataset_name: str):
        nodes_communities_dictionary = {}
        community_nodes_list = [[] for _ in range(database.get_number_of_communities())]

        current = 1
        for node in database.get_nodes_and_their_communities():
            print(current)
            nodes_communities_dictionary[node['nodeId']] = node['community']
            community_nodes_list[node['community']].append(node['nodeId'])
            current += 1

        with open(dataset_name + '_node_community.json', 'wt', encoding='utf8') as file:
            json.dump(nodes_communities_dictionary, file)

        with open(dataset_name + '_community_nodes.json', 'wt', encoding='utf8') as file:
            json.dump(community_nodes_list, file)

    def get_communities_from_dictionaries(self, dataset_name: str):
        community_nodes_list = self.load_cache_dictionary(dataset_name + '_community_nodes')
        outer_community_data = self.load_cache_dictionary(dataset_name + '_outer_community')
        inner_community_data = self.load_cache_dictionary(dataset_name + '_inner_community')

        community_data_list = []
        community_index = 0
        for community in community_nodes_list:
            community_data = []
            community_len = len(community)
            node_index = 1
            for node_id in community:
                print('community ', community_index)
                print('Node ', node_index, ' of ', community_len)
                node_data = {'nodeId': node_id,
                             'outerCommunityNeighborsAmount': outer_community_data[str(node_id)],
                             'innerCommunityNeighborsAmount': inner_community_data[str(node_id)],
                             }
                community_data.append(node_data)
                node_index += 1
            community_data_list.append(community_data)
            community_index += 1
        return community_data_list


    def save_bitcoin_cada_result(self):
        neighbors_dictionary = self.load_cache_dictionary('bitcoin_neighbors_pairs')

        # communities_amount = database.get_number_of_communities()

        # print('communities', communities_amount)

        node_community_dictionary = self.load_cache_dictionary('bitcoin_node_community')

        bitcoin_cada_result = {}

        current = 1
        for node_id in node_community_dictionary:
            neighbors_community_vector = [0 for _ in range(3242)]
            print(current)
            for neighbor_id in neighbors_dictionary[node_id]:
                neighbor_community = node_community_dictionary[str(neighbor_id)]
                neighbors_community_vector[neighbor_community] += 1

            score = 0
            max_value = max(neighbors_community_vector)
            for community_value in neighbors_community_vector:
                score += community_value

            bitcoin_cada_result[node_id] = score
            current += 1

        database.save_cache_dictionary(bitcoin_cada_result, 'bitcoin_cada_result')

#endregion

#region Main

if __name__ == '__main__':
    database = NeoDatabase('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    # database.save_dataset('Disney', 'C:/Users/Administrator/Desktop/Disney.graphml', '_default')
    # database.save_dataset('Airlines', 'C:/Users/Administrator/Desktop/airlines.graphml', 'RELATED')
    # database.save_dataset('Bitcoin', 'C:/Users/Administrator/Documents/School Work/Tesis/Implementation/NeoSparkFramework/Databases_Readers/BitCoin/Bitcoin_2013.graphml', 'RELATED')

    # database.load_dataset('Disney')
    # database.unload_current_dataset()
    # database.load_dataset('Airlines')
    # database.load_dataset('Bitcoin')

    # print(database.current_dataset)
    # print(database.data_sets)
    # louvain_result = database.apply_method('Louvain')

    # result = database.laplacian_score(0, ['MinPriceUsedItem', 'MinPricePrivateSeller', 'Avg_Helpful', 'Avg_Rating'])
    # print(result)
    # while database.get_nodes_without_inner_community_neighbors_amount() > 0:
    #     try:
    #         for node_data in database.get_nodes_without_inner_community_neighbors\
    #                     ():
    #             node_id = node_data['nodeId']
    #             print(node_id)
    #             result_inner_data = database.get_inner_community_neighbors_of_node_amount(node_id)
    #             node = result_inner_data['n']
    #             inner_community_neighbors_amount = result_inner_data['innerCommunityNeighborsAmount']
    #             node['innerCommunityNeighborsAmount'] = inner_community_neighbors_amount
    #             database.update_data(node)
    #
    #     except:
    #         pass

    # dict = {}
    # current = 0
    # for node_data in database.get_nodes_id():
    #     print(current)
    #     node_id = node_data['nodeId']
    #     result_inner_data = database.get_outer_community_neighbors_of_node_amount(node_id)
    #     inner_community_neighbors_amount = result_inner_data['outerCommunityNeighborsAmount']
    #     dict[node_id] = result_inner_data
    #     current += 1

    # dict = {}
    # current = 1
    # for node in database.get_all_nodes_and_their_outer_cummonity_neighbors_amount():
    #     print(current)
    #     dict[node['id']] = node['outerCommunityNeighborsAmount']
    #     current += 1
    #
    # with open('bitcoin_outer_community.json', 'wt', encoding='utf8') as file:
    #     json.dump(dict, file)
    #
    # with open('bitcoin_outer_community.json', 'rt', encoding='utf8') as file:
    #     dict = json.load(file)
    #
    # print(len(dict))
    #
    # counter = 0
    # node_i = 1
    # for node in database.get_nodes_id():
    #     print('Node ', node_i)
    #     node_i += 1
    #     if str(node['nodeId']) not in dict:
    #         dict[str(node['nodeId'])] = 0
    #         counter += 1

    # database.save_inner_community_amount_dictionary('bitcoin')
    # database.fill_cache_dictionary_with_value('bitcoin_inner_community')
    #
    # dict = database.load_cache_dictionary('bitcoin_inner_community')
    # print(len(dict))

    # nodes_communities_dictionary = {}
    # community_nodes_list = [[] for _ in range(database.get_number_of_communities())]
    # community_nodes_dictionary = {}

    # current = 1
    # for node in database.get_nodes_and_their_communities():
    #     print(current)
    #     nodes_communities_dictionary[node['nodeId']] = node['community']
    #     community_nodes_list[node['community']].append(node['nodeId'])
    #     current += 1
    #
    # with open('bitcoin_node_community.json', 'wt', encoding='utf8') as file:
    #     json.dump(nodes_communities_dictionary, file)
    #
    # with open('bitcoin_community_nodes.json', 'wt', encoding='utf8') as file:
    #     json.dump(community_nodes_list, file)

    # with open('bitcoin_outer_community.json', 'wt', encoding='utf8') as file:
    #     json.dump(dict, file)

    # print('Rest ' ,counter, ' nodes')

    # dictionary = database.get_communities_from_dictionaries('bitcoin')
    # with open('bitcoin_community_data.json', 'wt', encoding='utf8') as file:
    #     json.dump(dictionary, file)

    # community_list = database.load_cache_dictionary('bitcoin_community_data')
    # for community in community_list:
    #     for node in community:
    #         for attribute in node:
    #             if node[attribute] is None:
    #                 print('None in ', attribute, ' at node ', node['nodeId'])

    # database.save_neighbors_pairs('bitcoin')
    #
    # database.fill_cache_dictionary_with_value('bitcoin_neighbors_pairs', [])


#endregion

