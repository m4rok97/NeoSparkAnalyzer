import time as tm
from sklearn import metrics
from NeoDatabase import NeoDatabase
import matplotlib.pyplot as plt


def test_disney():
    database = NeoDatabase(
        'C:/Users/Administrator/.Neo4jDesktop/neo4jDatabases/database-460cb81a-07d5-4d10-b7f3-5ebba2c058df/installation-3.5.0',
        'Lenin.41')

    disney_result = database.load_cache_dictionary('testing_result')
    disney_pairs_ids_dictionary = database.load_cache_dictionary('disney_pair_ids_dictionary')

    mapped_result = {}

    for node_id in disney_result:
        mapped_result[disney_pairs_ids_dictionary[node_id]] = disney_result[node_id]

    expected_anomalies = ['B00005T5YC', 'B00006LPHB', 'B00004R99B', 'B00005T7HD', 'B00004T2SJ', 'B00004WL3E']

    expected_anomalies_count = len(expected_anomalies)
    expected_normal_count = len(mapped_result) - expected_anomalies_count

    threshold = 0.0
    tpr_list = []
    fpr_list = []
    while threshold <= 1:
        tp = 0
        fp = 0
        for node_id in mapped_result:
            score = mapped_result[node_id]
            if score > threshold:
                if node_id in expected_anomalies:
                    tp += 1
                else:
                    fp += 1

        print('Threshold: ', threshold)
        print('TP: ', tp)
        print('FP:', fp)

        tpr = tp / expected_anomalies_count
        fpr = fp / expected_normal_count
        tpr_list.append(tpr)
        fpr_list.append(fpr)

        threshold += 0.01

    auc = metrics.auc(fpr_list, tpr_list)
    auc = metrics.auc(fpr_list, tpr_list)

    plt.figure()
    lw = 2
    plt.plot(fpr_list, tpr_list, color='darkorange', lw=lw, label='ROC Curve (AUC = %0.2f)' % auc)
    plt.plot([0, 1], [0, 1], color='navy', linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.xlabel('True Positive Rate')
    plt.title('Receiver Operating Characteristics SubspaceGlance')
    plt.legend(loc='lower right')
    plt.show()

    auc = metrics.auc(fpr_list, tpr_list)

    print('AUC:', auc)
if __name__ == '__main__':
   test_disney()
