def upload_to_blob_storage(company: str, input_path: str, storage_key: str, user_id: str):
    """
    Encrypts a data download archive from a specific company (e.g. Google, Apple, etc.) and uploads the data to
    Azure blob storage. Writes upload status (e.g. blob storage URL) to output_dir/company.json. The user owns the
    storage key.

    Args:
        root (str): [description]
        storage_key (str): [description]
    """
    pass


def download_from_blob_storage(company: str, output_path: str, storage_key: str, user_id: str):
    """
    Download and decrypt a data download from Azure blob storage and make it available for preprocessing. Data is
    downloaded into the VM's on-disk storage at output_path.

    Args:
        company (str): [description]
        path (str): [description]
        storage_key (str): [description]
        user_id (str): [description]
    """
    pass
