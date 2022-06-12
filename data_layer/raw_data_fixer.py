import datetime

import pandas as pd
from feature_store.feature_store_client import FeatureStoreClient
from feature_store.models.conditions import create_select_condition
from google.cloud import storage
from joblib import Parallel, delayed

from configuration.config import Config


class RawDataFixer:

    _batch_size = 50

    def __init__(self):
        self._melted_trends_for_review = pd.read_csv(Config.MELTED_TRENDS_FOR_REVIEW_PATH)
        self._fs_client = FeatureStoreClient()
        storage_client = storage.Client()
        self._storage_bucket = storage_client.get_bucket(Config.GCS_BUCKET)

    def fix_data_to_cloud(self, conditions: list):
        for s_idx, e_idx in zip(
                range(0, len(conditions), self._batch_size),
                range(self._batch_size, len(conditions) + self._batch_size, self._batch_size)
        ):
            if s_idx == len(conditions):
                break
            if e_idx >= len(conditions) - 1:
                e_idx = len(conditions) - 1

            condition_batch = conditions[s_idx:e_idx]

            cond_expressions = [create_select_condition(**condition) for condition in condition_batch]
            features_df = self._fs_client.read_features_from_and_conditions(
                list_of_and_conditions=cond_expressions,
                batch_size=self._batch_size)[Config.FEATURES_LIST]

            def parallel_for(condition):
                condition_df = features_df[(features_df['machine_id'] == condition['machine_id']) &
                                           (features_df['recorded_at'] >= condition['since']) &
                                           (features_df['recorded_at'] <= condition['until'])].copy(deep=True)

                self._storage_bucket.blob(
                    f"{Config.RAW_DATA_GCS_PATH}/{Config.EXECUTION_UNIQUE_SIG}/{condition['machine_id']}_{condition['until']}.csv").upload_from_string(
                    condition_df.to_csv(), 'text/csv')
            Parallel(n_jobs=-1, backend='threading')(delayed(parallel_for)(condition) for condition in condition_batch)
            print(f"Calculated ~{100 * (e_idx / len(conditions)):.2f}%")

    def create_conditions(self) -> list:
        condition_df = pd.DataFrame()
        condition_df['machine_id'] = self._melted_trends_for_review['Machine_id']
        condition_df['until'] = self._melted_trends_for_review['Until'].apply(lambda x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M'))
        condition_df['since'] = condition_df['until'] - pd.Timedelta(days=100)
        return condition_df.to_dict('records')


# fixer = RawDataFixer()
# conditions = fixer.create_conditions()
# fixer.fix_data_to_cloud(conditions)
