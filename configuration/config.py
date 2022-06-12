from datetime import datetime
from pathlib import Path

PROJECT_PATH = Path(__file__).parent.parent
EXECUTION_UNIQUE_SIG = int(datetime.now().timestamp())


class Config:

    PROJECT_PATH = PROJECT_PATH
    MELTED_TRENDS_FOR_REVIEW_PATH = f'{PROJECT_PATH}/data_layer/data_files/melted_trends_for_review_2.csv'
    FOR_REVIEW_FEATURES_PATH = f'{PROJECT_PATH}/data_layer/data_files/for_review_features.csv'
    GCS_BUCKET = f'augury-datasets-research'
    RAW_DATA_GCS_PATH = f'semi-supervised-discovery/raw-data/{EXECUTION_UNIQUE_SIG}'

    FEATURES_LIST = [
        'machine_id',
        'component_id',
        'bearing',
        'plane',
        'recorded_at',

        'vibration_session_machine_on',

        'vibration_vel_rms',
        'vibration_acc_rms',
        'vibration_vel_order__0',
        'vibration_acc_p2p_percentiles',
        'vibration_env_rms',
        'impact_p2p_median',
    ]

    def __init__(self):
        pass
