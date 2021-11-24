from examples.search.main import SearchEngine
from cli.cli import upload_to_blob_storage, download_from_blob_storage
from lens.sources.FacebookSource import FacebookSource

import pickle
import os

from lens.sources.GoogleSource import GoogleSource

STORAGE_KEY = '5F254E10-677C-4D91-9CC8-00DC5AEF7F5F'
USER_ID = 'shomil@berkeley.edu'

# Demo of CLI package functionality

upload_to_blob_storage(
    company='facebook',
    input_path='test_data/facebook/',
    storage_key=STORAGE_KEY,
    user_id=USER_ID
)

TEMP_DIR = 'test_data/'

download_from_blob_storage(
    company='facebook',
    output_path=TEMP_DIR,
    storage_key=STORAGE_KEY,
    user_id=USER_ID
)

# Demo of simple Python serialization cache for in-memory search index

# cache = 'cache/search.pickle'

# try:
#     with open(cache, 'rb') as file:
#         search = pickle.load(file)

# except:
search = SearchEngine()
search.preprocess(
    facebook_source=FacebookSource(root=TEMP_DIR + 'facebook/'),
    google_source=GoogleSource(root=TEMP_DIR + 'google/')
)

search.help()

events = search.query({
    'person': 'John'
})

# for e in search.events:
#     if 'John' in str(e.to_json()):
#         print(e.to_json())
