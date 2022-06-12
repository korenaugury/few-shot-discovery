from google.cloud import storage
from torch.utils.data import DataLoader, Dataset

from configuration.config import Config


class DatasetCreator(Dataset):

    def __init__(self):
        storage_client = storage.Client()
        self._data_blobs = storage_client.list_blobs(Config.GCS_BUCKET, prefix=Config.RAW_DATA_GCS_PATH)
        bp = 0



Config(unique_sig=165503334)
dc = DatasetCreator()

