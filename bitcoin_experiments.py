import time as tm
from sklearn import metrics
from NeoDatabase import NeoDatabase
import matplotlib.pyplot as plt

if __name__ == '__main__':
    database = NeoDatabase('C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0', 'Lenin.41')
    bitcoin_result = database.load_cache_dictionary('bitcoin_cada_result')

    ground_truth = [bitcoin_result['2165369'], bitcoin_result['2118193'], bitcoin_result['2120649'], bitcoin_result['2120411']]
    threshold = min(ground_truth)

    print('Threshold: ', threshold)

    count = 0

    for node_id in bitcoin_result:
        score = bitcoin_result[node_id]
        if score >= threshold:
            count += 1

    print('Anomlies detected: ', count)