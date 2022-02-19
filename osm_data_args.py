import argparse

def get_args():
    """
    Returns a namedtuple with arguments extracted from the command line.
    :return: A namedtuple with arguments
    """
    parser = argparse.ArgumentParser(
        description='Welcome to the OSM Data Extractor')

    parser.add_argument('--countries', nargs="?", type=str, default=100, help='Two - Letter Country Codes')
    args = parser.parse_args()
    print(args)
    return args