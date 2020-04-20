#region Static Methods

def communities_warpper(communities, method):
    for community in communities:
        yield (community, method)


def method_executor(community_and_method: tuple):
    community, method = community_and_method

    return eval('%s(community)' % method)


def get_average_difference(community: list, attribute: str):
    community_len = len(community)
    acc = 0
    for node_i in community:
        node_i_attribute_val = node_i[attribute]
        for node_j in community:
            node_j_attribute_val = node_j[attribute]
            acc += abs(node_i_attribute_val - node_j_attribute_val)
    return acc / community_len**2


def get_average_differences(community: list):
    differences = {}
    for attribute in filter(lambda x: x != 'nodeId', community[0]):
        differences[attribute] = get_average_difference(community, attribute)
    return differences

def get_inter_neighbors_difference(community: list):
    pass

def glance(community: list):
    average_differences = get_average_differences(community)
    community_len = len(community)
    ans = []

    for node_i in community:
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
                difference = abs(node_i_attribute - node_j_attribute)
                if difference > average_difference:
                    acc += 1
            attribute_score = acc / community_len
            attributes_scores.append(attribute_score)
        anomaly_score = max(attributes_scores)
        ans.append((node_i['nodeId'], anomaly_score))
    return ans

#endregion\