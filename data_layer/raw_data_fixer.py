import datetime

import pandas as pd
from ...feature_store.feature_store_client import FeatureStoreClient
from ...feature_store.models.conditions import create_select_condition
from joblib import Parallel, delayed

from configuration.config import Config
from utils.date_functions import move_date


class RawDataFixer:

    def __init__(self):
        self._melted_trends_for_review = pd.read_csv(Config.MELTED_TRENDS_FOR_REVIEW_PATH)
        self._fs_client = FeatureStoreClient()

    def fix_data_to_cloud(self, conditions: list):
        features_df = self._fs_client.read_features_from_and_conditions(list_of_and_conditions=conditions)[Config.FEATURES_LIST]

        def parallel_for(condition):
            condition_df = features_df[(features_df['machine_id'] == condition['machine_id']) &
                                       (features_df['recorded_at'] >= condition['since']) &
                                       (features_df['recorded_at'] <= condition['until'])]
            condition_df.to_parquet(f"{Config.RAW_DATA_GCS_PATH}/{condition['machine_id']}_{condition['until']}.parquet")
            condition_df.to_csv(f"{Config.RAW_DATA_GCS_PATH}/{condition['machine_id']}_{condition['until']}.csv")

        Parallel(n_jobs=-1)(delayed(parallel_for)(condition) for condition in conditions)

    def fetch_data(self):
        conditions = self.create_conditions()

        features_df = self._fs_client.read_features_from_and_conditions(list_of_and_conditions=conditions)[Config.FEATURES_LIST]
        features_df.to_csv(Config.FOR_REVIEW_FEATURES_PATH)
        return features_df

    def create_conditions(self) -> list:
        condition_df = pd.DataFrame()
        condition_df['machine_id'] = self._melted_trends_for_review['Machine_id']
        condition_df['until'] = self._melted_trends_for_review['Until'].apply(lambda x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M'))
        condition_df['since'] = condition_df['until'] - pd.Timedelta(days=100)
        conditions_list = condition_df.to_dict('records')[:10]
        return [create_select_condition(**condition) for condition in conditions_list]


fixer = RawDataFixer()
conditions = fixer.create_conditions()
fixer.fix_data_to_cloud(conditions)
