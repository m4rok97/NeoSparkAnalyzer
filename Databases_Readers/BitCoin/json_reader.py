import json

class BitcoinJsonReader(object):
    """ A class used for read the data from bitcoin in json format. """

    def __init__(self):
        """ Initialize a BitCoinJsonReader."""
        self._block = None
        self._json_block_file = None

    def get_json_block(self):
        """
        Return the current json block.
        :return: The current json block
        """
        return self._block

    def read_block_file(self, block_json_file_path):
        """
        Read the current json file from the given path and load the content as the current
        json file.
        :param block_json_file_path: The json blocks path.'
        :return:
        """
        self._json_block_file = open(block_json_file_path)
        self._block = json.load(self._json_block_file)

    def get_json_transaction_list(self):
        """
        Get the jsom transaction list from the current json file.
        :return: The jsom transaction list from the current.
        """
        return self._block['data']

    def is_confirmed_transaction(self, json_transaction):
        """
        Return True is the given json transaction is confirmed.
        :param json_transaction:  The desired json transaction to verify confirmation.
        :return: A boolean value that represent is the given json transaction is confirmed or not.
        """
        return json_transaction['confirmations'] > 0

    def is_coinbase_transaction(self, json_transaction):
        """
        Return True if the json transaction is the given json transaction is a coinbase transaction
        :param json_transaction: The desired json transaction to verify if is coinbase.
        :return: A boolean value that represent is the given json transaction is coinbase or not
        """
        return json_transaction['is_coinbase']

    def close_block_file(self):
        """
        Close the current json block file descriptor.
        """
        self._json_block_file.close()

if __name__ == '__main__':
    json_reader = BitcoinJsonReader()
    json_reader.read_block_file('C:/Users/Administrator/Documents/School Work/Tesis/Implementation/NeoSparkFramework/Databases/Bitcoin/00000000000000000eab32386b8854581ca95f672ec9ccd96d2201c493f2c644-1.json')
    print(json_reader.get_json_block())