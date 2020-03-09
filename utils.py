import shutil

def copy_file(source: str, destiny: str):
    """
    Copy a file from source to destiny
    :param source: The source address
    :param destiny: The destination address
    :return:
    """
    shutil.copy(source, destiny)

if __name__ == '__main__':
    copy_file('C:/Users/Administrator/Documents/Yo.docx', 'C:/Users/Administrator/Desktop')