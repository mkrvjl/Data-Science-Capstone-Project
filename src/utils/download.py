import os.path
import urllib


def get_missing_files(url: str, filepath: str):
    # create folder for filepath if it does not exist
    root_dir = os.path.dirname(filepath)

    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
        print(f"Directory ({root_dir}) created...")

    print(f"File '{filepath}' not found, downloading...")
    web_url = urllib.request.urlopen(url)
    data = web_url.read()

    with open(filepath, "wb") as binary_file:
        binary_file.write(data)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File {filepath} was not found!")

    with open(filepath, "r") as file:
        if file.readable():
            print(f"File '{filepath}' downloaded and ready...")
        else:
            raise IOError(f"Unable to read downloaded file '{filepath}'")

