from typing import Any, List
import os
import json
import glob


class FacebookSource:
    """
    This is an example of a lightweight data source - it simply reads from an on-disk Facebook data archive, and
    returns JSON-formatted data payloads.

    The client API encrypts and uploads data downloads to Azure Blob Storage. The BaseSource class contains a method
    to fetch and decrypt a particular user's data download, and each source subclass exposes several methods to interact
    with the data, once it's downloaded into temporary storage. When this process shuts down, the temporary storage is
    deleted, so no user data is persisted.
    """

    def __init__(self, root: str) -> None:
        self.root = root

    def glob(self, extension: str) -> List[str]:
        return glob.glob(self.root + '**/*' + extension, recursive=True)

    def read_json(self, path: str) -> Any:
        with open(path, 'r') as file:
            data = json.load(file)
            return data

    def read_node(self, path: str, node: str) -> Any:
        print(self.root + path)
        if os.path.exists(self.root + path):
            with open(self.root + path, 'r') as file:
                data = json.load(file)
                if node in data:
                    return data[node]

        raise LookupError(
            f"This path/node combination doesn't exist in the Facebook dataset (path={path}, node={node})."
        )
