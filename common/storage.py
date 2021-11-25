import os
from typing import Optional
import uuid
import zipfile
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

from cryptography.fernet import Fernet

import hashlib

from zipfile import ZipFile
import os
from os.path import basename

INCLUDED_EXTENSIONS = ['.json', '.csv', '.ics', '.vcf', '.mbox']


class AzureStorage():

    def __init__(self, key_path: str, azure_connect_str: str, azure_container_name: str) -> None:
        self.azure_container_name = azure_container_name

        self.initialize_encryptor(key_path=key_path)
        self.initialize_storage(azure_connect_str=azure_connect_str)

        print('Azure storage initialized for container: ' +
              self.azure_container_name)

    def initialize_encryptor(self, key_path: str):
        with open(key_path, 'rb') as file:
            key = file.read()
            self.encryptor = Fernet(key)

    def initialize_storage(self, azure_connect_str: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            azure_connect_str)

    def get_uuid(self, company: str, user_id: str) -> str:
        m = hashlib.sha256()
        m.update(f'user_id/{user_id}'.encode())
        m.update(f'company/{company}'.encode())
        return m.hexdigest()[:16]

    def get_blob_client(self, remote_file_name: str) -> BlobServiceClient:
        return self.blob_service_client.get_blob_client(
            container=self.azure_container_name,
            blob=remote_file_name
        )

    def create_zip(self, input_path: str, output_path: str):
        n = 0
        zipf = zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(input_path):
            for file in files:
                if any([file.endswith(ext) for ext in INCLUDED_EXTENSIONS]):
                    n += 1
                    zipf.write(os.path.join(root, file),
                               os.path.relpath(os.path.join(root, file),
                                               os.path.join(input_path, '..')))
        print(f'Compressed {n} files into archive: ' + output_path)
        zipf.close()

    def upload_dataset(self, company: str, user_id: str, input_path: str):
        """
        Encrypts a data download archive from a specific company (e.g. Google, Apple, etc.) and uploads the data to
        Azure blob storage. Writes upload status (e.g. blob storage URL) to output_dir/company.json. The user owns
        the storage key.
        """
        remote_file_name = self.get_uuid(company=company, user_id=user_id)
        zip_path = 'zips/' + remote_file_name + '.zip'
        print('Compressing dataset...')
        self.create_zip(input_path, zip_path)
        blob_client = self.get_blob_client(remote_file_name=remote_file_name)
        with open(zip_path, 'rb') as data:
            print('Encrypting file contents...')
            encrypted_file_contents = self.encryptor.encrypt(data.read())
            print('Uploading to Azure...')
            blob_client.upload_blob(encrypted_file_contents)

    def download_dataset(self, company: str, user_id: str, download_file_path: str):
        """
        Download and decrypt a data download from Azure blob storage and make it available for preprocessing. Data is
        downloaded into the VM's on-disk storage at output_path.
        """
        remote_file_name = self.get_uuid(company=company, user_id=user_id)
        blob_client = self.get_blob_client(remote_file_name=remote_file_name)
        with open(download_file_path, "wb") as download_file:
            encrypted_file_contents = blob_client.download_blob().readall()
            download_file.write(
                self.encryptor.decrypt(encrypted_file_contents))

    @staticmethod
    def generate_key(key_path: str):
        key = Fernet.generate_key()
        with open(key_path, 'wb') as file:
            file.write(key)
