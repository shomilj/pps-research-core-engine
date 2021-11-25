from common.storage import AzureStorage
import os

from examples.search.main import SearchEngine

from dotenv import load_dotenv

from lens.sources.FacebookSource import FacebookSource
from lens.sources.GoogleSource import GoogleSource
load_dotenv()

# USER_ID = 'shomil@berkeley.edu'
# KEY_PATH = 'key.secret'

# AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
# AZURE_STORAGE_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_NAME')

# # If we don't already have a key, then generate one.
# if os.path.exists(KEY_PATH):
#     print('Generating new symmetric key...')
#     AzureStorage.generate_key(key_path='key.secret')
# else:
#     print('Using existing symmetric key...')

# # Initialize a storage manager
# storage = AzureStorage(
#     key_path=KEY_PATH,
#     azure_connect_str=AZURE_STORAGE_CONNECTION_STRING,
#     azure_container_name=AZURE_STORAGE_CONTAINER_NAME
# )

# # Upload each dataset to Azure
# for company in ['google', 'facebook']:
#     print(f'Uploading data for {company}...')
#     storage.upload_dataset(
#         company=company,
#         user_id='shomil@berkeley.edu',
#         input_path=f'test_data/{company}/'
#     )

# Initialize the Search application

TEMP_DIR = 'test_data/'

search = SearchEngine()
search.preprocess(
    facebook_source=FacebookSource(root=TEMP_DIR + 'facebook/'),
    google_source=GoogleSource(root=TEMP_DIR + 'google/')
)