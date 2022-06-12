from feature_store.feature_store_client import FeatureStoreClient
from torch.utils.data import DataLoader, Dataset

from configuration.config import Config


class _BatchDataSet(Dataset):
    _fs_client = FeatureStoreClient()

    def __init__(self, conditions):
        features_df = self._fs_client.read_features_from_and_conditions(list_of_and_conditions=conditions)[
            Config.FEATURES_LIST].sort('recorded_at')
        features_df_per_machine = features_df.groupby('machine_id')
