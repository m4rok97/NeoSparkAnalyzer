from json_reader import BitcoinJsonReader
from py2neo import Graph, Node, Relationship
import os
import networkx as nx
import pickle

def read_addresses_relation_network_in_memory(graph, json_reader, file_path):
    # reading the json file
    json_reader.read_block_file(file_path)

    # reading all transactions
    for transaction in json_reader.get_json_transaction_list():
        transaction_time = transaction['time']
        # Reading only the transactions in the desired time-span
        # analyze only the confirmed transactions
        if json_reader.is_confirmed_transaction(transaction) and \
                not json_reader.is_coinbase_transaction(transaction):
            inputs = transaction['inputs']
            inputs_len = len(inputs)

            # check if the first node address is in the graph
            if not (inputs[0]['address'] in graph.nodes):
                # add the node in case it is not in the graph
                graph.add_node(inputs[0]['address'])

            # iterate over the transaction inputs
            for i in range(0, inputs_len - 1):
                # check if the node is in the graph
                node_address = inputs[i]['address']
                node_neighbor_addres = inputs[i + 1]['address']

                # check if the neighbor is in the graph the node is not checked because it was the neighbor
                # from the previous iteration
                if not (node_neighbor_addres in graph.nodes):
                    graph.add_node(node_neighbor_addres)

                # if there is not an edge between the two nodes then add it
                if not graph.has_edge(node_address, node_neighbor_addres):
                    graph.add_edge(node_address, node_neighbor_addres)

            # iterate over the outputs, because some addresses can be only outputs in the time-span
            for output in transaction['outputs']:
                output_address = output['address']
                if output_address not in graph.nodes:
                    graph.add_node(output_address)

    # closing the block file
    json_reader.close_block_file()


def create_addresses_relation_network_from_json_file(files_folder_path):
    print('='*30)
    print('Analyzing files')
    # creating a BitcoinJsonReader
    json_reader = BitcoinJsonReader()
    # creating an empty directed graph
    graph = nx.Graph()
    total = len(os.listdir(files_folder_path))
    current = 1
    for filename in os.listdir(files_folder_path):
        print('Current ', current,' / Total ', total)
        read_addresses_relation_network_in_memory(graph, json_reader, os.path.join(files_folder_path, filename))
        current += 1
    return graph


def get_list_of_connected_components(graph):
    return list(nx.connected_components(graph))


def create_address_user_dictionary(connected_components_list):
    result = {}
    cc_count = len(connected_components_list)
    for cc_index in range(0, cc_count):
        for addr in connected_components_list[cc_index]:
            result[addr] = cc_index

    return result


def save_address_user_dict_to_file(dictionary, file_path):
    with open(file_path, 'wb') as file_handle:
        pickle.dump(dictionary, file_handle, pickle.HIGHEST_PROTOCOL)


def read_address_user_dict_from_file(file_path):
    with open(file_path, 'rb') as file_handle:
        return pickle.load(file_handle)


def build_user_network_from_json_data(graph, json_reader, file_path, address_user_dict):
    # reading the json file
    json_reader.read_block_file(file_path)

    # reading all transactions
    for transaction in json_reader.get_json_transaction_list():
        transaction_time = transaction['time']
        # analyze only the confirmed transactions
        if json_reader.is_confirmed_transaction(transaction) and \
                not json_reader.is_coinbase_transaction(transaction):
            # getting the user_if from the input address
            from_user = address_user_dict[transaction['inputs'][0]['address']]

            # check if the user is already in the graph and add it if it is not
            if from_user not in graph.nodes:
                graph.add_node(from_user)

            for output in transaction['outputs']:
                to_user = address_user_dict[output['address']]

                # check that the transaction is not a change transaction
                if to_user != from_user:
                    # check if to_user is in the graph and add it if it is not
                    if to_user not in graph.nodes:
                        graph.add_node(to_user)

                    # check if there is an edge between this two users
                    if graph.has_edge(from_user, to_user):
                        # if there is an edge, increase the total transaction value and the transaction counter
                        edge_data = graph[from_user][to_user]
                        edge_data['value'] += output['value']
                        edge_data['count'] += 1
                    else:
                        # if the transaction is not in G then add it
                        temp_dictionary = {'value': output['value'], 'count': 1}
                        graph.add_edge(from_user, to_user, **temp_dictionary)

    # closing the json file
    json_reader.close_block_file()


def create_users_network_from_json_files(address_user_dict, files_folder_path):
    print('=' * 30)
    print('Analyzing files')
    # creating a BitcoinJsonReader
    json_reader = BitcoinJsonReader()
    # creating an empty directed graph
    graph = nx.DiGraph()
    current = 1
    total = len(os.listdir(files_folder_path))
    for filename in os.listdir(files_folder_path):
        print('Current: ', current, '/ Total: ', total)
        build_user_network_from_json_data(graph, json_reader, files_folder_path + '\\' + filename, address_user_dict)
        current += 1
    return graph


def read_addresses_relation_network_time_span_into_graph(graph: Graph, json_reader: BitcoinJsonReader, file_path: str):
    json_reader.read_block_file(file_path)

    for transaction in json_reader.get_json_transaction_list():
        if json_reader.is_confirmed_transaction(transaction) and not json_reader.is_coinbase_transaction(transaction):
            inputs = transaction['inputs']

            first_node = graph.run("match (n) where n.address = '%s' return n" % inputs[0]['address']).evaluate()
            if not first_node:
                graph.create(Node('Address', address=inputs[0]['address']))

            for i in range(len(inputs) - 1):
                node_address = inputs[i]['address']
                node_neighbor_address = inputs[i + 1]['address']
                node = graph.run("match (n) where n.address = '%s' return n" % node_address ).evaluate()
                node_neighbor = graph.run("match (n) where n.address = '%s' return n" % node_neighbor_address ).evaluate()

                if not node_neighbor:
                    node_neighbor = Node('Address', address=node_neighbor_address)
                    graph.create(node_neighbor)

                nodes_rel = graph.run("match (n)-[r]-(m) where n.address = '%s' and m.address = '%s' return r" % (node_address, node_neighbor_address)).evaluate()
                # print('Node rel ',  nodes_rel)
                if not nodes_rel:
                    # print('Enter to add noode_rel')
                    graph.create(Relationship(node, 'IS_RELATED', node_neighbor))

        for output in  transaction['outputs']:
            output_address = output['address']
            output_node = graph.run("match (n)-[r]-(m) where n.address = '%s' and m.address = '%s' return r").evaluate()

            if not output_node :
                graph.create(Node('Address', address=output_address))

        json_reader.close_block_file()


def create_address_relation_network(files_folder_path: str):
    json_reader = BitcoinJsonReader()
    graph = Graph(password="Lenin.41", port=11004)

    graph.delete_all()
    for file_name in os.listdir(files_folder_path):
        read_addresses_relation_network_time_span_into_graph(graph, json_reader, os.path.join(files_folder_path,
                                                                                                  file_name))
def build_user_network_from_json_data_in_database(graph, json_reader, file_path, address_user_dict):
    json_reader.read_block_file(file_path)

    for transaction in json_reader.get_json_transaction_list():
        if json_reader.is_confirmed_transaction(transaction) and  not json_reader.is_coinbase_transaction(transaction):
            from_user = address_user_dict[transaction['inputs'][0]['address']]
            from_user_node = graph.run("match (n) where n.address = '%s' return n" % from_user)

            if not from_user_node:
                from_user_node = Node('User', address=from_user)
                graph.create(from_user_node)

            for output in transaction['outputs']:
                to_user = address_user_dict[output['address']]
                to_user_node = graph.run("match (n) where n.address = '%s' return n" % to_user)

                if to_user != from_user:
                    if not to_user_node:
                        graph.create(to_user_node)

                nodes_rel = graph.run("match (n)-[r]-(m) where n.address = '%s' and m.address = '%s' return r" % (from_user, to_user)).evaluate()

                if nodes_rel:
                    nodes_rel['value'] += output['value']
                    nodes_rel['count'] += 1
                    graph.push(nodes_rel)
                else:
                    graph.create(Relationship(from_user_node, 'IS_RELATED', to_user_node, count=1, value=output['value']))

    json_reader.close_block_file()

def create_users_network_from_json_files_in_database(address_user_dict, files_folder_path):
    print('=' * 30)
    print('Analyzing files')
    # creating a BitcoinJsonReader
    json_reader = BitcoinJsonReader()
    # creating an empty directed graph
    graph = Graph(password="Lenin.41", port=11004)
    graph.delete_all()
    current = 1
    total = len(os.listdir(files_folder_path))
    for filename in os.listdir(files_folder_path):
        print('Current: ', current, '/ Total: ', total)
        build_user_network_from_json_data_in_database(graph, json_reader, files_folder_path + '\\' + filename, address_user_dict)
        current += 1
    return graph

if __name__ == '__main__':
    files_path = 'C:/Users/Administrator/Documents/School Work/Tesis/Implementation/NeoSparkFramework/Databases/Bitcoin/'
    # address_graph = create_addresses_relation_network_from_json_file(files_path)
    # connected_components = get_list_of_connected_components(address_graph)
    # dictionary = create_address_user_dictionary(connected_components)
    # save_address_user_dict_to_file(dictionary, 'dict.txt')
    dictionary = read_address_user_dict_from_file('dict.txt')
    # graph = Graph(password="Lenin.41", port=11004)
    # graph.delete_all()
    graph = create_users_network_from_json_files(dictionary, files_path)

    nx.write_graphml(graph, 'graph.graphml')




