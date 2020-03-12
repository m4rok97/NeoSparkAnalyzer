import py2neo as p2n
from py2neo import Graph
from utils import *
import json
import os

class NeoDatabase:

    def __init__(self, database_directory: str, database_password: str):
        self.database_directory = database_directory
        self.data_sets_registry_directory = database_directory + '/data_sets.json'

        # Initialize the database directory json file
        if 'data_sets.json' not in os.listdir(self.database_directory):
            print(os.listdir(self.database_directory))
            with open(self.data_sets_registry_directory, 'x', encoding='utf8') as data_sets_registry:
                dic = {}
                json.dump(dic, data_sets_registry)
        with open(self.data_sets_registry_directory, 'rt', encoding='utf8') as data_sets_registry:
            self.data_sets = json.load(data_sets_registry)

        self.graph = Graph(password=database_password)
        self.current_dataset = ''

    def load_dataset(self, dataset_name):
        if not self.current_dataset:
            self.unload_current_dataset()
        if dataset_name in self.data_sets:
            self.graph.run('call apoc.import.graphml(\'%s\', {})' % self.data_sets[dataset_name])
        else:
            Exception('No dataset with the given name')

    def unload_current_dataset(self):
        self.graph.delete_all()
        self.current_dataset = ''

    def apply_method(self):
        pass

    def save_dataset(self, dataset_name: str, directory: str):
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
            self.data_sets[dataset_name] = directory.split('/')[-1]
            with open(self.data_sets_registry_directory, 'wt', encoding='utf8') as data_sets_registry:
                json.dump(self.data_sets, data_sets_registry)
            return True
        except FileNotFoundError:
            print('Database not found in the given directory')
            return False


if __name__ == '__main__':
    database = NeoDatabase('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    # database.save_dataset('Disney', 'C:/Users/Administrator/Desktop/Disney.graphml')
    # database.save_dataset('Airlines', 'C:/Users/Administrator/Desktop/airlines.graphml')

    # database.load_dataset('Disney')
    database.unload_current_dataset()
    database.load_dataset('Airlines')