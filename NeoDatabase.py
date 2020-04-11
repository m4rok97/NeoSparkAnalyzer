import py2neo as p2n
from py2neo import Graph
from utils import *
import json
import os
import functools as ft

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
            self.graph.run('call apoc.import.graphml(\'%s\', {})' % self.data_sets[data_set_name]['directory'])
            self.current_dataset = data_set_name
        else:
            Exception('No dataset with the given name')

    def unload_current_dataset(self):
        self.graph.delete_all()
        self.current_dataset = ''

    def apply_method(self, method_name='Louvain'):
        print(self.current_dataset)
        print(self.data_sets)
        relationship_name = self.data_sets[self.current_dataset]['relationship_name']

        if method_name == 'Louvain':
            self.graph.run('call algo.louvain.stream(null, "%s") yield nodeId, community match (n) where id(n) = nodeId set n.community = community' % relationship_name).data()
        elif method_name == 'UnionFind':
            self.graph.run('call algo.unionFind.stream(null, "%s") yield nodeId, setId match (n) where id(n) = nodeId set n.setId = setId' % relationship_name).data()
        elif method_name == 'LabelPropagation':
            self.graph.run('call algo.labelPropagation.stream(null, "%s", { iterations: 10}) yield nodeId, label match (n) where id(n) = nodeId set n.setId = setId' % relationship_name).data()

        self.save_nodes_attributes()

        # call algo.louvain.stream(null, "RELATED") yield nodeId, community match (n) where id(n) = nodeId set n.communiy = community

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

    def get_communities(self, attributes: list):
        for i in range(self.get_number_of_communities()):
            yield self.get_community_nodes(i, attributes).data()

    def get_neighbors_of_node(self, node_id: int):
        return self.graph.run('match (n)--(m) where id(n) = %s return m' % node_id)

    def get_nodes_attributes(self):
        return list(self.graph.run('match (n) return n limit 1').evaluate().keys())

    def get_community_nodes(self, community: int, attributes):
        select_query = ft.reduce(lambda x, y: x + y, ['n.' + attribute + ' as ' + attribute + ', ' for attribute in attributes])
        end_select_query = 'id(n) as nodeId '
        select_query += end_select_query
        return self.graph.run('match (n) where n.community = %s return ' % community + select_query )

    def get_community_len(self, community: int):
        return self.graph.run('match (n) where n.community = %s return count(n)' % community)

    def get_attribute_from_community_nodes(self, community: int, attribute: str):
        return self.graph.run('match (n) where n.community = %s return n.%s as attribute, id(n) as nodeId' % (community, attribute))

    def update_data(self, data):
        self.graph.push(data)

    def save_nodes_attributes(self):
        print('Attributes')
        print(self.get_nodes_attributes())
        self.data_sets[self.current_dataset]['attributes'] = self.get_nodes_attributes()
        with open(self.data_sets_registry_directory, 'wt', encoding='utf8') as data_sets_registry:
            json.dump(self.data_sets, data_sets_registry)

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

    for community in database.get_communities():
        print(community[0])
        break