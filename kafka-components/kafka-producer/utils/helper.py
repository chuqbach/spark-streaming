import configparser


def read_config(path):
    """
    Read configuration file and return object
    :return: config
    """
    config = configparser.ConfigParser()
    config.read(path)
    return config
