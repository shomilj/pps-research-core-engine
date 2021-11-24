from typing import Any, List
import os
import json
import glob


class GoogleSource:
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

    def get_mailbox_paths(self) -> List[str]:
        return glob.glob(self.root + '**/*' + '.mbox', recursive=True)

    def get_calendar_paths(self) -> List[str]:
        return glob.glob(self.root + '**/*' + '.ics', recursive=True)

    def get_contact_paths(self) -> List[str]:
        return glob.glob(self.root + '**/*' + '.vcf', recursive=True)