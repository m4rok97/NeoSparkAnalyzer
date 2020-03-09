import py2neo as p2n
from py2neo import Graph
from utils import *
import json
import os

class NeoDatabase:

    def __init__(self, database_directory: str, database_password: str):
        # Initialize the database directory json file
        if 'datasets.json' not in os.listdir():
            with open('datasets.json', 'wt', encoding='utf8') as datasets_registry:
                dic = {}
                json.dump(dic, datasets_registry)
        with open('datasets.json', 'rt') as datasets_registry:
            self.data_sets = json.load(datasets_registry)

        self.database_directory = database_directory
        self.graph = Graph(password=database_password)
        self.current_dataset = ''

    def load_dataset(self, dataset_name):
        if not self.current_dataset:
            self.unload_current_dataset()
        if dataset_name in self.data_sets:
            self.graph.run('call apoc.import.graphml(\'{0}\', {})'.format(self.data_sets[dataset_name]))
        else:
            Exception('No dataset with the given name')

    def unload_current_dataset(self):
        self.graph.delete_all()
        self.current_dataset = ''

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
            self.data_sets[dataset_name] = new_directory

            return True
        except Exception:
            return False


if __name__ == '__main__':
    database = NeoDatabase('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    database.save_dataset('Disney', 'C:/Users/Administrator/Desktop/Disney.graphml')